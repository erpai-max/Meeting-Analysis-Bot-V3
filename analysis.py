# analysis.py
# Meeting Analysis Bot - Gemini (2.5 Model Version)
# Requirements: google-generativeai >= 0.8.0, google-api-python-client, mutagen (optional), tenacity

# --- Quiet gRPC/absl logs BEFORE importing Google/gRPC libraries ---
import os
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_CPP_VERBOSITY", "NONE")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3")

import io
import re
import json
import logging
import datetime as dt
from typing import Dict, Any, Tuple, List, Set, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Make sure google-generativeai is installed
try:
    import google.generativeai as genai
except ImportError:
    logging.critical("google-generativeai library not found. Please install it.")
    raise

# Make sure googleapiclient is installed
try:
    from googleapiclient.http import MediaIoBaseDownload
    from googleapiclient.errors import HttpError
except ImportError:
    logging.critical("google-api-python-client library not found. Please install it.")
    raise

# Make sure tenacity is installed
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    logging.critical("tenacity library not found. Please install it.")
    raise

# Import specific exceptions for robust retries
try:
    from google.api_core import exceptions as api_core_exceptions
    # Define QuotaExceeded before using it in the tuple
    class QuotaExceeded(Exception):
        """Raised when Gemini quota/rate-limit is hit."""
        pass
    
    RETRYABLE_EXCEPTIONS = (
        QuotaExceeded,
        api_core_exceptions.ResourceExhausted,  # Explicitly include for quota
        api_core_exceptions.InternalServerError,  # e.g., 500
        api_core_exceptions.ServiceUnavailable,  # e.g., 503
        api_core_exceptions.DeadlineExceeded,  # e.g., 504
    )
except ImportError:
    # Define QuotaExceeded even if api_core fails to import
    class QuotaExceeded(Exception):
        """Raised when Gemini quota/rate-limit is hit."""
        pass
    logging.warning("google.api_core.exceptions not found. Retrying only on QuotaExceeded.")
    RETRYABLE_EXCEPTIONS = (QuotaExceeded,)


# =========================
# Quota detection helper (strict)
# =========================
def _is_quota_error(e: Exception) -> bool:
    """
    Return True only for genuine quota/rate-limit cases (HTTP 429 / ResourceExhausted).
    We DO NOT treat long transcripts, token sizes, or generic errors as quota.
    """
    msg = str(e).lower()
    # Prefer typed exception if available
    try:
        import google.api_core.exceptions
        if isinstance(e, google.api_core.exceptions.ResourceExhausted):
            return True
    except (ImportError, AttributeError):
        pass
    except Exception as ie:
        logging.warning(f"Unexpected error checking for ResourceExhausted: {ie}")

    keywords = [
        "resourceexhausted", "resource exhausted",
        "quota exceeded", "quota", "too many requests",
        "rate limit", "rate-limit", "429"
    ]
    return any(k in msg for k in keywords)

# =========================
# Constants & Feature Maps
# =========================
ALLOWED_MIME_PREFIXES = ("audio/", "video/")
DEFAULT_MODEL_NAME = "gemini-2.5-flash"  # Use a known stable default

ERP_FEATURES = {
    "Tally import/export": ["tally", "tally import", "tally export"],
    "E-invoicing": ["e-invoice", "e invoicing", "einvoice"],
    "Bank reconciliation": ["bank reconciliation", "reco", "reconciliation"],
    "Vendor accounting": ["vendor accounting", "vendors ledger"],
    "Budgeting": ["budget", "budgeting"],
    "350+ bill combinations": ["bill combinations", "billing combinations"],
    "PO / WO approvals": ["purchase order", "po approval", "work order", "wo approval"],
    "Asset tagging via QR": ["asset tag", "qr asset", "asset qr"],
    "Inventory": ["inventory"],
    "Meter reading → auto invoices": ["meter reading", "auto invoice", "metering"],
    "Maker-checker billing": ["maker checker", "maker-checker"],
    "Reminders & Late fee calc": ["reminder", "late fee"],
    "UPI/cards gateway": ["upi", "payment gateway", "cards"],
    "Virtual accounts per unit": ["virtual account", "virtual accounts"],
    "Preventive maintenance": ["preventive maintenance", "pm schedule"],
    "Role-based access": ["role based", "role-based"],
    "Defaulter tracking": ["defaulter", "arrears tracking"],
    "GST/TDS reports": ["gst", "tds"],
    "Balance sheet & dashboards": ["balance sheet", "dashboard"],
}

ASP_FEATURES = {
    "Managed accounting (bills & receipts)": ["managed accounting", "computerized bills", "receipts"],
    "Bookkeeping (all incomes/expenses)": ["bookkeeping", "income expense"],
    "Bank reconciliation + suspense": ["suspense", "bank reconciliation", "reco"],
    "Financial reports (non-audited)": ["financial report", "non audited", "trial balance", "p&l", "profit and loss"],
    "Finalisation support & audit coordination": ["finalisation", "audit coordination", "auditor"],
    "Vendor & PO/WO management": ["vendor management", "po", "wo", "work order"],
    "Inventory & amenities booking": ["inventory", "amenities booking", "amenity booking"],
    "Dedicated remote accountant": ["remote accountant", "dedicated accountant"],
    "Annual data backup": ["annual data back", "backup", "data backup"],
}

# =========================
# Utility Helpers
# =========================
def _is_media_supported(mime_type: str) -> bool:
    return bool(mime_type) and any(mime_type.startswith(p) for p in ALLOWED_MIME_PREFIXES)

def _init_gemini():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY env var not set")
    try:
        genai.configure(api_key=api_key)
        logging.info("Gemini API configured successfully.")
    except Exception as e:
        logging.error(f"Failed to configure Gemini API: {e}")
        raise

def _drive_get_metadata(drive_service, file_id: str) -> Dict[str, Any]:
    """Fetch rich metadata: name, mimeType, createdTime, and duration for videos."""
    fields = (
        "id,name,mimeType,createdTime,"
        "videoMediaMetadata(durationMillis,height,width),"
        "size,parents"
    )
    try:
        return drive_service.files().get(fileId=file_id, fields=fields).execute()
    except HttpError as e:
        logging.error(f"HttpError getting metadata for {file_id}: {e.resp.status} {e.reason}")
        raise
    except Exception as e:
        logging.error(f"Error getting metadata for {file_id}: {e}")
        raise

def _download_drive_file(drive_service, file_id: str, out_path: str) -> Tuple[str, str]:
    """Download a Drive file to out_path. Returns (mime_type, out_path)."""
    try:
        meta = drive_service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
        mime_type = meta.get("mimeType", "")
        file_size = int(meta.get("size", 0))
        logging.info(f"Attempting download for {meta.get('name', file_id)} ({mime_type}, Size: {file_size} bytes)")

        if file_size == 0:
            logging.warning(f"File {file_id} has size 0 according to metadata. Skipping download.")
            # Create an empty file locally to avoid errors later if needed, or handle upstream
            with open(out_path, "wb") as fh_empty:
                pass # Creates an empty file
            return mime_type, out_path # Return mime_type even for empty file

        request = drive_service.files().get_media(fileId=file_id)
        with io.FileIO(out_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.debug(f"Download progress for {file_id}: {int(status.progress() * 100)}%") # Use debug level

        # Verify downloaded size matches metadata size
        downloaded_size = os.path.getsize(out_path)
        if downloaded_size != file_size:
            logging.warning(f"Downloaded file size ({downloaded_size}) differs from metadata size ({file_size}) for {file_id}")

        logging.info(f"File download complete: {out_path}")
        return mime_type, out_path
    except HttpError as e:
        logging.error(f"HttpError downloading {file_id}: {e.resp.status} {e.reason}")
        raise
    except Exception as e:
        logging.error(f"Error downloading {file_id}: {e}")
        raise

# ---------- Date extraction ----------
_DATE_PATTERNS = [
    (re.compile(r"\b(\d{1,2})[-/\.](\d{1,2})[-/\.](\d{4})\b"), "%d-%m-%Y"), # dd-mm-yyyy with 4-digit year
    (re.compile(r"\b(\d{1,2})[-/\.](\d{1,2})[-/\.](\d{2})\b"), "%d-%m-%y"), # dd-mm-yy with 2-digit year
    (re.compile(r"\b(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})\b"), "%Y-%m-%d"), # yyyy-mm-dd
    (re.compile(r"\b(20\d{2})(\d{2})(\d{2})\b"), "%Y%m%d"), # yyyymmdd (e.g., 20251023)
]

def _format_ddmmyy(d: dt.date) -> str:
    # Always output dd/mm/yy format
    return d.strftime("%d/%m/%y")

def _extract_date_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    base = os.path.splitext(name)[0]
    # More robust separator replacement
    base = re.sub(r'[_\-\|\s\.]+', ' ', base).strip()

    for rx, fmt in _DATE_PATTERNS:
        # Search anywhere in the base string
        m = rx.search(base)
        if m:
            g = m.groups()
            try:
                # Adjust parsing based on format string
                if fmt == "%d-%m-%Y" or fmt == "%d-%m-%y":
                    dd, mm, yyyy_or_yy = int(g[0]), int(g[1]), int(g[2])
                    # Handle 2-digit year (assume 20xx)
                    year = yyyy_or_yy if yyyy_or_yy > 1000 else 2000 + yyyy_or_yy
                    parsed = dt.date(year, mm, dd)
                elif fmt == "%Y-%m-%d":
                    yyyy, mm, dd = int(g[0]), int(g[1]), int(g[2])
                    parsed = dt.date(yyyy, mm, dd)
                elif fmt == "%Y%m%d":
                    yyyy, mm, dd = int(g[0]), int(g[1]), int(g[2])
                    parsed = dt.date(yyyy, mm, dd)
                else:
                    continue # Should not happen with current patterns
                
                return _format_ddmmyy(parsed)
            
            except ValueError:
                # Handles invalid date combinations (e.g., day 32)
                logging.debug(f"Invalid date extracted {g} with format {fmt}, continuing search.")
                continue
            except Exception as e:
                logging.warning(f"Error parsing date {g} with format {fmt}: {e}")
                continue # Continue searching with other patterns
                
    logging.debug(f"No date found in filename: {name}")
    return None

def _pick_date_for_output(file_name: str, created_time_iso: Optional[str]) -> str:
    """ 1) date in filename → 2) Drive createdTime → 3) "NA"; format dd/mm/yy """
    from_name = _extract_date_from_name(file_name)
    if from_name:
        logging.info(f"Using date from filename: {from_name}")
        return from_name

    if created_time_iso:
        try:
            # Handle ISO format like "2025-10-23T06:28:53.640Z"
            # Take only the date part
            date_part = created_time_iso.split('T')[0]
            d = dt.date.fromisoformat(date_part)
            formatted_date = _format_ddmmyy(d)
            logging.info(f"Using date from createdTime: {formatted_date}")
            return formatted_date
        except Exception as e:
            logging.warning(f"Could not parse createdTime '{created_time_iso}': {e}")
            pass

    logging.info("Could not determine date, using 'NA'")
    return "NA"

# ---------- Duration extraction ----------
def _millis_to_minutes(ms: int) -> str:
    # Use integer division for whole minutes, rounding might be better if needed
    minutes = ms // 60000 
    # Or round: minutes = int(round(ms / 60000.0))
    return f"{minutes}"

def _probe_duration_minutes(meta: Dict[str, Any], local_path: str, mime_type: str) -> str:
    """
    Prefers Drive videoMediaMetadata.durationMillis (videos).
    For audio, use mutagen (mp3/m4a/ogg/wav/etc.).
    Falls back to WAV wave reader if needed.
    Returns integer minutes as a string or "NA".
    """
    duration_str = "NA"  # Default

    # 1) Video metadata from Drive
    try:
        vmeta = meta.get("videoMediaMetadata") or {}
        dur_ms_str = vmeta.get("durationMillis") # API returns string sometimes
        if dur_ms_str:
            dur_ms = int(dur_ms_str)
            if dur_ms > 0:
                duration_str = _millis_to_minutes(dur_ms)
                logging.info(f"Duration from Drive metadata: {duration_str} min")
                return duration_str # Found duration, return early
    except Exception as e:
        logging.warning(f"Error reading video metadata duration: {e}")

    # If not found or not video, try audio libraries
    if duration_str == "NA" and mime_type and mime_type.startswith("audio/"):
        
        # 2) Audio duration via mutagen (preferred for various formats)
        try:
            # Ensure mutagen is installed (should be from requirements.txt)
            from mutagen import File as MutagenFile
            mf = MutagenFile(local_path)
            if mf is not None and getattr(mf, "info", None) and getattr(mf.info, "length", None):
                seconds = float(mf.info.length)
                if seconds > 0:
                    duration_str = f"{int(round(seconds / 60.0))}"
                    logging.info(f"Duration from mutagen: {duration_str} min")
                    return duration_str
        except ImportError:
            # Log this once if it happens, should not if reqs are installed
            logging.warning("mutagen library not found/installed, cannot get duration accurately for some audio types.")
        except Exception as e:
            logging.warning(f"Mutagen failed to read duration for {local_path}: {e}")

        # 3) WAV fallback without mutagen (if applicable)
        if duration_str == "NA" and local_path.lower().endswith(".wav"):
            try:
                import wave # Built-in module
                with wave.open(local_path, "rb") as w:
                    frames = w.getnframes()
                    rate = w.getframerate()
                    if rate > 0: # Avoid division by zero
                        seconds = frames / float(rate)
                        if seconds >= 0: # Allow 0 seconds
                            duration_str = f"{int(round(seconds / 60.0))}"
                            logging.info(f"Duration from wave module: {duration_str} min")
                            return duration_str
            except ImportError:
                # Should not happen for wave module
                logging.warning("wave module could not be imported for WAV fallback.")
            except Exception as e:
                logging.warning(f"Wave module failed for {local_path}: {e}")

    if duration_str == "NA":
        logging.warning(f"Could not determine duration for {local_path}")
    return duration_str

# ---------- Society name from file name ----------
def _society_from_filename(name: str) -> str:
    """Derive Society Name from file name (drop extension, tidy separators)."""
    if not name:
        return "Unknown Society"
    
    base = os.path.splitext(name)[0]
    
    # Replace common separators with spaces and collapse whitespace
    base = re.sub(r'[_\-\|\.]+', ' ', base).strip() # Use regex for robustness
    
    # Collapse multiple spaces into one
    base = re.sub(r'\s+', ' ', base).strip()
    
    return base or "Unknown Society" # Return default if name becomes empty

# ---------- ERP/ASP coverage & missed-opps ----------
def _normalize_text(s: str) -> str:
    import re as _re
    if not s:
        return ""
    # Remove punctuation, convert to lowercase, collapse whitespace
    text = _re.sub(r'[^\w\s]', '', s) # Keep only word chars and spaces
    text = text.lower()
    text = _re.sub(r"\s+", " ", text).strip()
    return text

def _match_coverage(transcript: str, feature_map: Dict[str, List[str]]) -> Tuple[Set[str], Set[str]]:
    # Normalize transcript once
    normalized_transcript = _normalize_text(transcript)
    
    if not normalized_transcript:
        # Handle empty transcript case
        return set(), set(feature_map.keys()) # All missed if transcript is empty

    covered, missed = set(), set()
    
    for feature, keys in feature_map.items():
        # Normalize keywords (assuming they are relatively clean already)
        normalized_keys = [_normalize_text(k) for k in keys if k]
        
        # Check if any normalized keyword is in the normalized transcript
        hit = any(key in normalized_transcript for key in normalized_keys)
        
        if hit:
            covered.add(feature)
        else:
            missed.add(feature)
            
    return covered, missed

def _build_feature_summary(covered_all: Set[str], total: int, label: str) -> str:
    pct = 0 if total == 0 else int(round(100 * len(covered_all) / total))
    covered_list = sorted(list(covered_all)) # Convert set to list for sorting
    
    head = f"{label} Coverage: {len(covered_all)}/{total} ({pct}%)."
    
    # Only add covered list if it's not empty
    if covered_list:
        head += " Covered: " + ", ".join(covered_list) + "."
        
    return head

def _feature_coverage_and_missed(transcript: str) -> Tuple[str, str]:
    if not transcript:
        # Handle empty transcript
        logging.warning("Cannot calculate feature coverage: transcript is empty.")
        # Return empty strings or appropriate placeholders
        return "NA - Empty Transcript", "NA - Empty Transcript"

    try:
        erp_cov, erp_missed = _match_coverage(transcript, ERP_FEATURES)
        asp_cov, asp_missed = _match_coverage(transcript, ASP_FEATURES)
    
        feature_coverage_summary = " ".join([
            _build_feature_summary(erp_cov, len(ERP_FEATURES), "ERP"),
            _build_feature_summary(asp_cov, len(ASP_FEATURES), "ASP")
        ]).strip() # Use strip to remove trailing space if one is empty
    
        priority = [
            # Top Priority (Core Value Props)
            "Tally import/export", "Bank reconciliation", "UPI/cards gateway",
            "Managed accounting (bills & receipts)", "Bank reconciliation + suspense",
            "Financial reports (non-audited)", "Dedicated remote accountant",
            # Secondary Priority (Key Differentiators/Common Needs)
            "Defaulter tracking", "PO / WO approvals", "Inventory", 
            "Reminders & Late fee calc", # Split from one item
            "GST/TDS reports", "Vendor accounting", "Role-based access",
            "Finalisation support & audit coordination", "Bookkeeping (all incomes/expenses)"
            # Add others if needed
        ]

        missed_all = erp_missed.union(asp_missed)
        
        if not missed_all:
            missed_text = "NA" # Indicate nothing missed clearly
        else:
            # Sort missed items: priority first, then alphabetically
            missed_list = sorted(list(missed_all)) # Convert set to list
            missed_sorted = sorted(
                missed_list,
                # Key: tuple (0 if priority else 1, index in priority list or large number, feature name)
                key=lambda x: (
                    0 if x in priority else 1, 
                    priority.index(x) if x in priority else float('inf'), 
                    x
                )
            )
            # Format as bullet points using newline character
            missed_text = "- " + "\n- ".join(missed_sorted)
        
        # Ensure we don't return empty strings if calculation succeeds but finds nothing
        return feature_coverage_summary or "NA", missed_text or "NA"

    except Exception as e:
        logging.error(f"Error calculating feature coverage: {e}", exc_info=True)
        return "Error calculating coverage", "Error calculating missed opportunities"


# ---------- Gemini config & calls ----------

# Gets the primary model from config, falling back to DEFAULT_MODEL_NAME if needed
def _get_model(config: Dict[str, Any]) -> str:
    model_name = config.get("google_llm", {}).get("model", DEFAULT_MODEL_NAME)
    logging.debug(f"Using primary model: {model_name}")
    return model_name

# Gets the analysis model, falling back to the primary model
def _get_analysis_model(config: Dict[str, Any]) -> str:
    analysis_model = config.get("google_llm", {}).get("analysis_model")
    primary_model = _get_model(config) # Get primary model using the function above
    model_name = analysis_model or primary_model # Fallback to primary if analysis_model not set
    logging.debug(f"Using analysis model: {model_name}")
    return model_name

def _load_master_prompt(config: Dict[str, Any]) -> str:
    # Use absolute path if needed, or ensure script runs from correct directory
    prompt_filename = "prompt.txt"
    try:
        # Check in current working directory first
        if os.path.exists(prompt_filename):
            prompt_path = prompt_filename
        else:
            # Check in the script's directory as a fallback
            script_dir = os.path.dirname(os.path.abspath(__file__))
            prompt_path = os.path.join(script_dir, prompt_filename)

        if os.path.exists(prompt_path):
            with open(prompt_path, "r", encoding="utf-8") as f:
                prompt_content = f.read().strip()
            if prompt_content:
                logging.info(f"Loaded master prompt from {prompt_path}")
                return prompt_content
            else:
                logging.warning(f"Prompt file '{prompt_path}' is empty.")
        else:
            logging.warning(f"Prompt file '{prompt_filename}' not found in CWD or script directory.")
            
    except Exception as e:
        logging.error(f"Error loading prompt file '{prompt_filename}': {e}")

    # Fallback prompt if file loading fails or file is empty
    logging.warning("Using fallback schema prompt from config or default.")
    fallback_prompt = config.get("google_llm", {}).get("schema_prompt", """
    Act as an expert business analyst for society-management ERP & ASP meetings.
    Analyze the transcript provided.
    Strictly return ONLY a single valid JSON object adhering to the specified schema.
    Do not include any introductory text, closing text, markdown formatting like ```json, or ```.
    If a specific piece of information cannot be found in the transcript, use "NA" as the value for that key.
    For score fields ("Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling", "Closing Effectiveness", "Negotiation Strength"), output numeric strings between "2" (poor/absent) and "10" (excellent).
    Calculate "Total Score" (sum of 5 scores) and "% Score" (Total/50 as percentage string with one decimal).
    Fill all other keys based on the transcript content or "NA" if absent.
    """).strip()
    
    if not fallback_prompt:
        logging.error("CRITICAL: Fallback schema prompt is also empty!")
        return "" # Return empty or raise error
        
    return fallback_prompt

def _is_one_shot(config: Dict[str, Any]) -> bool:
    # ENV override first (case-insensitive check)
    env = os.getenv("GEMINI_ONE_SHOT")
    if env is not None:
        one_shot_env = env.strip().lower() in ("1", "true", "yes", "on")
        logging.info(f"GEMINI_ONE_SHOT environment variable set to '{env}'. One-shot mode: {one_shot_env}")
        return one_shot_env
        
    # Then check config, default to True if key missing
    one_shot_config = bool(config.get("google_llm", {}).get("one_shot", True))
    logging.info(f"Config 'one_shot' setting: {one_shot_config}. One-shot mode: {one_shot_config}")
    return one_shot_config

# Define reasonable safety settings - adjust level as needed
SAFETY_SETTINGS = {
    'HARM_CATEGORY_HARASSMENT': 'block_none',
    'HARM_CATEGORY_HATE_SPEECH': 'block_none',
    'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'block_none',
    'HARM_CATEGORY_DANGEROUS_CONTENT': 'block_none',
}


# =========================================================================
# GENERATION / TRANSCRIPTION FUNCTIONS (FIXED upload calls)
# =========================================================================

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20),
       # Updated retry policy to include transient API errors
       retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS))
def _gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    """Transcribes audio/video using Gemini with retries for quota and transient errors."""
    
    uploaded_file_name = None # For cleanup
    
    try:
        model = genai.GenerativeModel(model_name)
        
        logging.info(f"Uploading file for transcription: {file_path} ({mime_type})")

        # Handle empty file before upload
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logging.warning(f"Transcription input file is empty or missing: {file_path}. Returning empty transcript.")
            return ""

        # ✅ Use genai.upload_file
        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        uploaded_file_name = getattr(uploaded, "name", None)
        
        logging.info(f"File uploaded successfully for transcription: {uploaded_file_name}")

        prompt = "Transcribe the audio verbatim with punctuation. Do not summarize. Output plain text only."
        
        # Construct contents list correctly
        contents = [{"role": "user", "parts": [prompt, uploaded]}]

        resp = model.generate_content(contents=contents, safety_settings=SAFETY_SETTINGS)

        # Check for blocks or empty candidates
        if getattr(resp, "prompt_feedback", None) and resp.prompt_feedback.block_reason:
            logging.error(f"Transcription prompt blocked: {resp.prompt_feedback.block_reason} - Ratings: {resp.prompt_feedback.safety_ratings}")
            raise RuntimeError(f"Transcription prompt blocked: {resp.prompt_feedback.block_reason}")

        if not getattr(resp, "candidates", None):
            logging.error("No candidates returned during transcription. Check safety settings or prompt.")
            finish_reason_detail = getattr(resp, 'finish_reason', 'N/A')
            safety_ratings_detail = getattr(resp, 'safety_ratings', 'N/A')
            logging.error(f"Overall finish reason: {finish_reason_detail}, Safety Ratings: {safety_ratings_detail}")
            raise RuntimeError("No response candidates received during transcription.")

        # Check finish reason of the first candidate
        first_candidate = resp.candidates[0]
        finish_reason = getattr(first_candidate, "finish_reason", None)
        safety_ratings = getattr(first_candidate, "safety_ratings", None)

        if finish_reason not in ('STOP', 'MAX_TOKENS'): # Allow MAX_TOKENS, but log warning
            logging.warning(f"Transcription generation finished unexpectedly with reason: {finish_reason}")
            if finish_reason == 'SAFETY':
                logging.error(f"Transcription blocked by safety settings: {safety_ratings}")
                raise RuntimeError(f"Transcription blocked by safety settings.")
            else:
                raise RuntimeError(f"Transcription failed with finish reason: {finish_reason}")
        elif finish_reason == 'MAX_TOKENS':
            logging.warning("Transcription may be incomplete due to MAX_TOKENS limit.")

        text = ""
        # Safely access text from parts
        if getattr(first_candidate, "content", None) and getattr(first_candidate.content, "parts", None):
            try:
                text = first_candidate.content.parts[0].text.strip()
            except Exception:
                text = ""
        
        if not text and finish_reason == 'STOP':
            # Log if empty but stopped normally (e.g., silent audio)
            logging.warning("Received empty transcript from Gemini (stopped normally).")

        return text

    except Exception as e:
        if _is_quota_error(e):
            logging.error(f"Quota exceeded during TRANSCRIBE call: {e}")
            raise QuotaExceeded(str(e)) # Re-raise specific exception
            
        # Log other errors clearly
        logging.error(f"Error during transcription call: {e}", exc_info=True)
        # Wrap other exceptions in RuntimeError for consistency
        raise RuntimeError(f"Transcription failed: {e}") from e

    finally:
        # Cleanup uploaded file on Gemini side
        if uploaded_file_name:
            try:
                logging.info(f"Attempting to delete uploaded file (transcribe): {uploaded_file_name}")
                # ✅ Add hasattr check for future-proofing
                if hasattr(genai, "delete_file"):
                    genai.delete_file(uploaded_file_name)
                    logging.info(f"Successfully deleted uploaded file: {uploaded_file_name}")
                else:
                    logging.warning("genai.delete_file function not found, cannot delete uploaded file.")
            except Exception as del_e:
                # Log deletion error but don't fail the main process
                logging.warning(f"Could not delete uploaded file {uploaded_file_name} after transcription: {del_e}")


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20),
       # Updated retry policy
       retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS))
def _gemini_analyze(transcript: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    """Analyzes transcript using Gemini, expecting JSON output."""
    
    try:
        model = genai.GenerativeModel(model_name)
        
        logging.info(f"Sending analysis request with transcript length: {len(transcript)} chars")

        # Construct contents list correctly for analysis
        contents = [{"role": "user", "parts": [master_prompt, "\n\n---\nMEETING TRANSCRIPT:\n", transcript]}]

        resp = model.generate_content(
            contents=contents,
            generation_config={
                "temperature": 0.2, # Low temperature for consistent JSON structure
                "response_mime_type": "application/json",
            },
            safety_settings=SAFETY_SETTINGS
        )

        # Check for blocks or empty candidates
        if getattr(resp, "prompt_feedback", None) and resp.prompt_feedback.block_reason:
            logging.error(f"Analysis prompt blocked: {resp.prompt_feedback.block_reason} - Ratings: {resp.prompt_feedback.safety_ratings}")
            raise RuntimeError(f"Analysis prompt blocked: {resp.prompt_feedback.block_reason}")

        if not getattr(resp, "candidates", None):
            logging.error("No candidates returned during analysis.")
            finish_reason_detail = getattr(resp, 'finish_reason', 'N/A')
            safety_ratings_detail = getattr(resp, 'safety_ratings', 'N/A')
            logging.error(f"Overall finish reason: {finish_reason_detail}, Safety Ratings: {safety_ratings_detail}")
            raise RuntimeError("No response candidates received during analysis.")

        first_candidate = resp.candidates[0]
        finish_reason = getattr(first_candidate, "finish_reason", None)
        safety_ratings = getattr(first_candidate, "safety_ratings", None)

        if finish_reason != 'STOP':
            logging.warning(f"Analysis generation finished unexpectedly with reason: {finish_reason}")
            if finish_reason == 'SAFETY':
                logging.error(f"Analysis blocked by safety settings: {safety_ratings}")
                raise RuntimeError(f"Analysis blocked by safety settings.")
            elif finish_reason == 'MAX_TOKENS':
                logging.error("Analysis response might be truncated due to MAX_TOKENS limit.")
            else:
                raise RuntimeError(f"Analysis failed with finish reason: {finish_reason}")

        # Robust JSON parsing from parts
        raw = ""
        if getattr(first_candidate, "content", None) and getattr(first_candidate.content, "parts", None):
            try:
                raw = first_candidate.content.parts[0].text.strip()
            except Exception:
                raw = ""
        
        if raw.startswith("```json"):
            raw = raw[7:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

        if not raw:
            logging.error("Received empty response during analysis.")
            raise RuntimeError("Empty analysis response from Gemini.")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as je:
            logging.error(f"Failed to parse JSON from analysis model. Raw response sample: '{raw[:500]}...'")
            raise RuntimeError(f"Failed to parse JSON from analysis model: {je}") from je
            
        if not isinstance(data, dict):
            logging.error(f"Analysis output is not a JSON object. Type: {type(data)}. Raw sample: '{raw[:500]}...'")
            raise RuntimeError("Analysis output is not a JSON object.")

        logging.info("Successfully received and parsed JSON analysis.")
        return data

    except Exception as e:
        if _is_quota_error(e):
            logging.error(f"Quota exceeded during ANALYZE call: {e}")
            raise QuotaExceeded(str(e))
            
        logging.error(f"Error during analysis call: {e}", exc_info=True)
        raise RuntimeError(f"Analysis failed: {e}") from e


# =========================================================================
# == CORRECTED _gemini_one_shot FUNCTION (Fixes NameError) ==
# =========================================================================

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20),
       # Updated retry policy
       retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS))
def _gemini_one_shot(file_path: str, mime_type: str, master_prompt: str, model_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """One-shot analysis using Gemini with file upload (fixed to genai.upload_file) and structured prompt."""
    
    uploaded_file_name = None # For potential cleanup if needed and possible
    
    try:
        model = genai.GenerativeModel(model_name)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logging.warning(f"One-shot input file is empty or missing: {file_path}. Returning error dict.")
            # Return a dict that can be handled by _write_success, indicating error
            return {"error": "Input file was empty", "Risks / Unresolved Issues": "Input file was empty", "Society Name": f"EmptyFile_{os.path.basename(file_path)}"}

        logging.info(f"Uploading file via genai.upload_file for one-shot: {file_path}")
        
        # ✅ Use genai.upload_file (handles the v1beta/ragStoreName issue)
        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        uploaded_file_name = getattr(uploaded, "name", None)
        
        logging.info(f"File uploaded successfully for one-shot: {uploaded_file_name}")

        logging.info("Sending one-shot generate_content request with new content structure...")
        
        # ✅✅✅ THE FIX IS APPLIED HERE ✅✅✅
        # The variable 'uploaded' is now correctly used instead of 'uploaded_file'
        response = model.generate_content(
            contents=[
                {"role": "user", "parts": [{"text": master_prompt}]},
                {"role": "user", "parts": [uploaded]} # <-- Correct variable 'uploaded'
            ],
            generation_config={
                "temperature": 0.2,
                "response_mime_type": "application/json"
            },
            safety_settings=SAFETY_SETTINGS
        )

        # Process response
        if getattr(response, "prompt_feedback", None) and response.prompt_feedback.block_reason:
            logging.error(f"One-shot prompt blocked: {response.prompt_feedback.block_reason} - Ratings: {response.prompt_feedback.safety_ratings}")
            raise RuntimeError(f"Prompt blocked due to {response.prompt_feedback.block_reason}")

        if not getattr(response, "candidates", None):
            logging.error("No candidates returned from one-shot call.")
            finish_reason_detail = getattr(response, 'finish_reason', 'N/A')
            safety_ratings_detail = getattr(response, 'safety_ratings', 'N/A')
            logging.error(f"Overall finish reason: {finish_reason_detail}, Safety Ratings: {safety_ratings_detail}")
            raise RuntimeError("No response candidates received from Gemini.")

        first_candidate = response.candidates[0]
        finish_reason = getattr(first_candidate, "finish_reason", None)
        safety_ratings = getattr(first_candidate, "safety_ratings", None)

        if finish_reason != 'STOP':
            logging.warning(f"One-shot generation finished unexpectedly with reason: {finish_reason}")
            if finish_reason == 'SAFETY':
                logging.error(f"One-shot blocked by safety settings: {safety_ratings}")
                raise RuntimeError(f"One-shot blocked by safety settings.")
            elif finish_reason == 'MAX_TOKENS':
                logging.error("One-shot response might be truncated due to MAX_TOKENS limit.")
            else:
                raise RuntimeError(f"One-shot failed with finish reason: {finish_reason}")

        # Robust JSON parsing from parts
        raw = ""
        if getattr(first_candidate, "content", None) and getattr(first_candidate.content, "parts", None):
            try:
                raw = first_candidate.content.parts[0].text.strip()
            except Exception:
                raw = ""
        
        if raw.startswith("```json"):
            raw = raw[7:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        
        if not raw:
            logging.error("Received empty response during one-shot analysis.")
            raise RuntimeError("Empty one-shot response from Gemini.")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as je:
            logging.error(f"Failed to parse JSON from one-shot model. Raw sample: '{raw[:500]}...'")
            raise RuntimeError(f"Failed to parse JSON from one-shot model: {je}") from je
            
        if not isinstance(data, dict):
            logging.error(f"One-shot output is not a JSON object. Type: {type(data)}. Raw sample: '{raw[:500]}...'")
            raise RuntimeError("One-shot output is not a JSON object.")

        logging.info("Successfully received and parsed JSON from new one-shot method.")

        # --- Add full transcript if missing (using separate call) ---
        if "transcript_full" in master_prompt and ("transcript_full" not in data or not data["transcript_full"]):
            logging.warning("Transcript missing/empty in one-shot JSON, attempting separate transcription...")
            try:
                transcription_model = _get_model(config)
                transcript_text = _gemini_transcribe(file_path, mime_type, transcription_model)
                data["transcript_full"] = transcript_text or "Transcription failed or empty"
                if transcript_text:
                    logging.info("Successfully added transcript via separate call.")
                else:
                    logging.warning("Separate transcription resulted in empty text.")
            except QuotaExceeded as qe_transcribe:
                logging.error(f"Quota exceeded during separate transcription attempt: {qe_transcribe}")
                data["transcript_full"] = "Transcription failed due to quota"
            except Exception as te:
                logging.error(f"Failed to get transcript separately after one-shot: {te}")
                data["transcript_full"] = f"Transcription failed: {str(te)[:100]}"
                
        return data

    except Exception as e:
        # Catch and log the specific error
        logging.error(f"Error encountered during NEW one-shot processing: {e}", exc_info=True)
        if _is_quota_error(e):
            raise QuotaExceeded(f"Quota exceeded during NEW ONE-SHOT call: {e}") from e
        raise RuntimeError(f"One-shot processing failed: {e}") from e

    finally:
        # Cleanup for uploaded file if possible
        if uploaded_file_name:
            try:
                # ✅ Add hasattr check for future-proofing
                if hasattr(genai, 'delete_file'):
                    logging.info(f"Attempting delete on uploaded file (new one-shot): {uploaded_file_name}")
                    genai.delete_file(uploaded_file_name)
                    logging.info(f"Deleted uploaded file (new one-shot): {uploaded_file_name}")
                else:
                    logging.warning("genai.delete_file not found, cannot explicitly delete file from new one-shot.")
            except Exception as del_e:
                logging.warning(f"Could not delete uploaded file {uploaded_file_name} (new one-shot): {del_e}")


# ---------- Manager info enrichment ----------
def _augment_with_manager_info(analysis_obj: Dict[str, Any], member_name: str, config: Dict[str, Any]) -> None:
    """
    Fill Owner/Email/Manager/Team/Manager Email using config.manager_map / manager_emails.
    Overrides existing values in analysis_obj with info from config based on member_name.
    """
    if not member_name:
        logging.warning("Cannot augment manager info: member_name is empty.")
        analysis_obj["Owner (Who handled the meeting)"] = "Unknown"
        # Ensure other keys exist with default values if owner is unknown
        analysis_obj.setdefault("Email Id", "")
        analysis_obj.setdefault("Manager", "")
        analysis_obj.setdefault("Team", "")
        analysis_obj.setdefault("Manager Email", "")
        return

    owner_name = member_name.strip()
    analysis_obj["Owner (Who handled the meeting)"] = owner_name # Set owner first

    try:
        # Assume keys in manager_map match member_name case/spacing or normalize here
        manager_map = config.get("manager_map", {})
        
        # Case-insensitive lookup (optional, if map keys might not match exactly)
        member_info = None
        for key, info in manager_map.items():
            if key.strip().lower() == owner_name.lower():
                member_info = info
                break

        if member_info and isinstance(member_info, dict):
            logging.info(f"Found manager map entry for '{owner_name}'")
            # Get values, falling back to existing value in analysis_obj, then to empty string
            analysis_obj["Email Id"] = member_info.get("Email", analysis_obj.get("Email Id", ""))
            analysis_obj["Manager"] = member_info.get("Manager", analysis_obj.get("Manager", ""))
            analysis_obj["Team"] = member_info.get("Team", analysis_obj.get("Team", ""))

            manager_name = member_info.get("Manager")
            if manager_name:
                manager_emails = config.get("manager_emails", {})
                manager_name_stripped = manager_name.strip()
                
                mgr_email = None
                for key, email in manager_emails.items():
                     if key.strip().lower() == manager_name_stripped.lower():
                        mgr_email = email
                        break
                        
                analysis_obj["Manager Email"] = mgr_email if mgr_email is not None else analysis_obj.get("Manager Email", "")
            else:
                analysis_obj.setdefault("Manager Email", "")
        else:
            logging.warning(f"No manager map entry found or entry invalid for owner: '{owner_name}'")
            analysis_obj.setdefault("Email Id", "")
            analysis_obj.setdefault("Manager", "")
            analysis_obj.setdefault("Team", "")
            analysis_obj.setdefault("Manager Email", "")
            
    except Exception as e:
        logging.error(f"Error during manager info augmentation for '{owner_name}': {e}", exc_info=True)
        # Ensure owner is still set even if augmentation fails
        analysis_obj["Owner (Who handled the meeting)"] = owner_name


# ---------- Sheets I/O ----------
def _write_success(gsheets_sheet, file_id: str, file_name: str, date_out: str, duration_min: str, feature_coverage: str, missed_opps: str, analysis_obj: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    """Writes the processed analysis object to Google Sheets and updates the ledger."""
    
    try:
        import sheets
    except ImportError:
        logging.critical("Failed to import 'sheets' module. Cannot write results to Google Sheets.")
        raise RuntimeError("Sheets module not found") # Raise error as writing is critical

    # --- Pre-write Validation and Cleaning ---
    if not isinstance(analysis_obj, dict):
        logging.error(f"Analysis result for {file_name} is not a dictionary (Type: {type(analysis_obj)}). Cannot write to sheet.")
        try:
            sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Invalid analysis result type: {type(analysis_obj)}", config, file_name)
        except Exception as le:
            logging.error(f"Also failed to update ledger for invalid type error on {file_name}: {le}")
        raise ValueError("Analysis result is not a dictionary.") # Raise error to indicate failure

    # Replace None values with "NA" for consistency, unless it's a score field
    score_keys = {
        "Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling",
        "Closing Effectiveness", "Negotiation Strength", "Total Score", "% Score"
    }
    
    cleaned_analysis = {}
    
    # Use default headers from sheets module if available to ensure all expected columns are present
    expected_headers = getattr(sheets, "DEFAULT_HEADERS", list(analysis_obj.keys()))
    
    for key in expected_headers:
        # Iterate through expected headers
        value = analysis_obj.get(key) # Get value from analysis object
        
        if value is None and key not in score_keys:
            cleaned_analysis[key] = "NA"
        elif value is None and key in score_keys:
            logging.warning(f"Score field '{key}' is None for {file_name}. Defaulting to '2'.")
            cleaned_analysis[key] = "2" # Default missing score
        else:
            cleaned_analysis[key] = str(value) if value is not None else "NA"

    # Add unexpected keys
    for key, value in analysis_obj.items():
        if key not in cleaned_analysis:
            logging.warning(f"Unexpected key '{key}' found in analysis_obj for {file_name}. Including it.")
            cleaned_analysis[key] = str(value) if value is not None else "NA"

    # --- Populate Standard Fields ---
    cleaned_analysis["Date"] = date_out if date_out != "NA" else "NA"
    cleaned_analysis["Meeting duration (min)"] = duration_min if duration_min != "NA" else "NA"
    cleaned_analysis["Society Name"] = _society_from_filename(file_name)
    
    cleaned_analysis["Feature Checklist Coverage"] = feature_coverage if feature_coverage and "Error" not in feature_coverage else cleaned_analysis.get("Feature Checklist Coverage", "NA")
    cleaned_analysis["Missed Opportunities"] = missed_opps if missed_opps and "Error" not in missed_opps else cleaned_analysis.get("Missed Opportunities", "NA")

    # --- Augment with Manager Info ---
    try:
        _augment_with_manager_info(cleaned_analysis, member_name, config)
    except Exception as e:
        logging.error(f"Failed during manager augmentation for {file_name}: {e}. Proceeding without full augmentation.")
        cleaned_analysis["Owner (Who handled the meeting)"] = member_name or "Unknown"

    # --- Calculate Scores if Missing or Invalid (Defensive) ---
    try:
        scores_valid = True
        individual_scores = []
        score_keys_individual = ["Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling", "Closing Effectiveness", "Negotiation Strength"]
    
        for key in score_keys_individual:
            score_str = cleaned_analysis.setdefault(key, "2")
            try:
                score_val = int(score_str)
                if not (2 <= score_val <= 10):
                    logging.warning(f"Score '{score_val}' for {key} out of range (2-10) in {file_name}. Clamping to 2.")
                    score_val = 2
                    cleaned_analysis[key] = "2"
                individual_scores.append(score_val)
            except (ValueError, TypeError):
                logging.warning(f"Invalid score value '{score_str}' for {key} in {file_name}. Defaulting score part to 2.")
                individual_scores.append(2)
                cleaned_analysis[key] = "2"
                scores_valid = False

        expected_total = sum(individual_scores)
        current_total_str = cleaned_analysis.get("Total Score")
        
        recalculate = not scores_valid
        
        if not recalculate and current_total_str and current_total_str != "NA":
            try:
                current_total = int(current_total_str)
                if current_total != expected_total:
                    logging.warning(f"Total Score mismatch for {file_name}. Model: {current_total}, Calculated: {expected_total}. Recalculating.")
                    recalculate = True
            except (ValueError, TypeError):
                logging.warning(f"Invalid Total Score '{current_total_str}' from model for {file_name}. Recalculating.")
                recalculate = True
        else:
            recalculate = True

        if recalculate:
            cleaned_analysis["Total Score"] = str(expected_total)
            percent_score = (expected_total / 50.0) * 100.0
            cleaned_analysis["% Score"] = f"{percent_score:.1f}%"
            logging.info(f"Recalculated scores for {file_name}: Total={expected_total}, Percent={cleaned_analysis['% Score']}")
        else:
            # Ensure existing % score is formatted correctly
            current_percent_str = cleaned_analysis.get("% Score", "")
            if not (current_percent_str.endswith('%') and ('.' in current_percent_str or current_percent_str[:-1].isdigit())):
                logging.warning(f"Formatting existing % Score '{current_percent_str}' for {file_name}")
                try:
                    percent_val = float(re.sub(r'[^\d.]', '', current_percent_str))
                    cleaned_analysis["% Score"] = f"{percent_val:.1f}%"
                except Exception:
                    logging.error(f"Could not reformat existing % Score for {file_name}. Setting to NA.")
                    cleaned_analysis["% Score"] = "NA"

    except Exception as score_calc_e:
        logging.error(f"Error during score validation/recalculation for {file_name}: {score_calc_e}")
        cleaned_analysis.setdefault("Total Score", "NA")
        cleaned_analysis.setdefault("% Score", "NA")


    # --- Write to Sheets ---
    status_note = f"Processed via Gemini model; duration={duration_min}m"
    try:
        sheets.update_ledger(gsheets_sheet, file_id, "Processed", status_note, config, file_name)
    except Exception as e:
        logging.error(f"Failed to update ledger to 'Processed' for {file_name}: {e}")

    try:
        write_func = getattr(sheets, "write_analysis_result", getattr(sheets, "append_result", None))
        if write_func:
            write_func(gsheets_sheet, cleaned_analysis, config)
            logging.info(f"Successfully wrote analysis results to sheet for {file_name}")
        else:
            logging.critical("CRITICAL: Function 'write_analysis_result' or 'append_result' not found in sheets module. Cannot write results.")
            raise AttributeError("Required sheet writing function not found.")
            
    except Exception as e:
        logging.error(f"CRITICAL: Failed to write analysis results to sheet for {file_name}: {e}", exc_info=True)
        try:
            error_msg = f"Failed write results: {str(e)[:200]}"
            sheets.update_ledger(gsheets_sheet, file_id, "Error", error_msg, config, file_name)
            logging.info(f"Updated ledger back to 'Error' for {file_name} due to sheet write failure.")
        except Exception as le:
            logging.critical(f"CRITICAL: Also failed to update ledger back to Error status for {file_name}: {le}")
        
        raise RuntimeError(f"Failed to write results to sheet for {file_name}") from e


# =========================
# Entry point
# =========================
def process_single_file(drive_service, gsheets_sheet, file_meta: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    """
    Orchestrates: metadata -> download -> (one-shot OR transcribe+analyze) -> enrich -> write.
    Handles exceptions, logs status, and cleans up temporary files.
    """
    local_path = None # Ensure defined for finally block
    file_id = file_meta["id"]
    file_name = file_meta.get("name", f"Unknown_{file_id}")
    analysis_obj = {} # Initialize analysis object

    try:
        _init_gemini() # Configure Gemini API

        # --- Metadata Fetch ---
        try:
            logging.info(f"Fetching metadata for file ID: {file_id}")
            meta = _drive_get_metadata(drive_service, file_id)
            file_name = meta.get("name", file_name) # Update name
            mime_type = meta.get("mimeType", file_meta.get("mimeType", ""))
            created_iso = meta.get("createdTime")

            if not file_name or file_name.startswith("Unknown_"):
                 logging.warning(f"Filename missing or generic in metadata for {file_id}. Using fallback.")
                 file_name = f"File_{file_id}"
                 
            logging.info(f"[Gemini] Processing: '{file_name}' ({mime_type}) | ID: {file_id}")
            
        except Exception as e:
            error_detail = f"Metadata fetch failed: {str(e)[:200]}"
            logging.error(error_detail, exc_info=True)
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", error_detail, config, file_name)
            except Exception as le:
                logging.error(f"Failed to import sheets or update ledger for metadata error on {file_id}: {le}")
            raise RuntimeError(f"Metadata fetch failed for {file_id}") from e

        # --- Basic Info Extraction ---
        date_out = _pick_date_for_output(file_name, created_iso)

        # --- Download ---
        tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        safe_suffix = re.sub(r'[^\w\.\-]', '_', file_name)
        safe_suffix = re.sub(r'_+', '_', safe_suffix).strip('_')
        local_path = os.path.join(tmp_dir, f"{file_id}_{safe_suffix[:150]}")

        try:
            logging.info(f"Starting download for '{file_name}'...")
            _, downloaded_mime_type = _download_drive_file(drive_service, file_id, local_path)
            mime_type = downloaded_mime_type or mime_type # Prefer downloaded mime_type if available
            logging.info(f"File downloaded to: {local_path}")
        except Exception as e:
            error_detail = f"Download failed: {str(e)[:200]}"
            logging.error(error_detail + f" for {file_name} (ID: {file_id})", exc_info=True)
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", error_detail, config, file_name)
            except Exception as le:
                logging.error(f"Failed to import sheets or update ledger for download error on {file_id}: {le}")
            raise RuntimeError(f"Download failed for {file_id}") from e

        # --- Duration ---
        duration_min = _probe_duration_minutes(meta, local_path, mime_type)

        # --- Prompt ---
        master_prompt = _load_master_prompt(config)
        if not master_prompt:
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", "Master prompt could not be loaded", config, file_name)
            except Exception as le:
                logging.error(f"Failed to import sheets or update ledger for prompt error on {file_id}: {le}")
            raise ValueError("Master prompt is empty, cannot proceed.")

        # --- Gemini Processing (One-shot or Two-call) ---
        logging.info(f"Using one-shot mode: {_is_one_shot(config)}")
        transcript = "" # Initialize transcript

        if _is_one_shot(config):
            model_to_use = _get_analysis_model(config)
            logging.info(f"Calling Gemini one-shot with model: {model_to_use}")
            analysis_obj = _gemini_one_shot(local_path, mime_type, master_prompt, model_to_use, config)
            transcript = analysis_obj.get("transcript_full", "")
            if not transcript or "Transcription failed" in transcript:
                 logging.warning(f"Transcript may be missing or failed in one-shot result for '{file_name}'")
        else:
            transcribe_model = _get_model(config)
            logging.info(f"Calling Gemini transcribe with model: {transcribe_model}")
            transcript = _gemini_transcribe(local_path, mime_type, transcribe_model)
            logging.info(f"Transcription complete for '{file_name}'. Length: {len(transcript)} chars")

            if not transcript:
                logging.error(f"Transcription resulted in empty text for '{file_name}'. Analysis cannot proceed.")
                try:
                    import sheets
                    sheets.update_ledger(gsheets_sheet, file_id, "Error", "Empty transcript", config, file_name)
                except Exception as le:
                    logging.error(f"Failed to update ledger for empty transcript on {file_id}: {le}")
                raise ValueError("Empty transcript received, analysis skipped.")

            analyze_model = _get_analysis_model(config)
            logging.info(f"Calling Gemini analyze with model: {analyze_model}")
            analysis_obj = _gemini_analyze(transcript, master_prompt, analyze_model)
            analysis_obj["transcript_full"] = transcript # Ensure transcript is included
            logging.info(f"Analysis complete for '{file_name}'")

        # --- Feature Coverage & Missed Opps ---
        feature_coverage, missed_opps = ("", "")
        try:
            # Use transcript from 'transcript' var (two-shot) or 'transcript_full' (one-shot)
            calc_transcript = transcript or analysis_obj.get("transcript_full", "")
            if calc_transcript and "Transcription failed" not in calc_transcript:
                feature_coverage, missed_opps = _feature_coverage_and_missed(calc_transcript)
                logging.info("Calculated feature coverage and missed opportunities.")
            else:
                logging.warning("Transcript unavailable or failed, cannot calculate coverage. Using defaults from analysis.")
                feature_coverage = analysis_obj.get("Feature Checklist Coverage", "NA")
                missed_opps = analysis_obj.get("Missed Opportunities", "NA")
        except Exception as e:
            logging.error(f"Error calculating feature coverage for '{file_name}': {e}")
            feature_coverage = "Error in calculation"
            missed_opps = "Error in calculation"

        # --- Write Results ---
        _write_success(
            gsheets_sheet=gsheets_sheet,
            file_id=file_id,
            file_name=file_name,
            date_out=date_out,
            duration_min=duration_min,
            feature_coverage=feature_coverage,
            missed_opps=missed_opps,
            analysis_obj=analysis_obj,
            member_name=member_name,
            config=config,
        )
        
        logging.info(f"SUCCESS: Fully processed and results written for '{file_name}'")

    except QuotaExceeded as qe:
        logging.error(f"Quota Exceeded during processing of '{file_name}': {qe}. Propagating error.")
        raise # Re-raise to be caught by main loop for backoff
    except (RuntimeError, ValueError, FileNotFoundError, AttributeError, Exception) as e:
        logging.error(f"CRITICAL failure during processing of '{file_name}': {e}. File should be quarantined by main loop.", exc_info=True)
        raise # Re-raise to be caught by main loop and marked as Error
    
    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logging.info(f"Cleaned up temporary file: {local_path}")
            except Exception as e:
                logging.warning(f"Could not remove temporary file '{local_path}': {e}")
