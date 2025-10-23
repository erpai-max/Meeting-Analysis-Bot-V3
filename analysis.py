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

# =========================
# Exceptions
# =========================
class QuotaExceeded(Exception):
    """Raised when Gemini quota/rate-limit is hit."""
    pass

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
        # Check if google.api_core.exceptions exists before importing
        import google.api_core.exceptions
        if isinstance(e, google.api_core.exceptions.ResourceExhausted):
            return True
    except (ImportError, AttributeError):
        pass # Ignore if the specific exception type isn't available
    except Exception as ie:
         # Log unexpected error during check
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
# This DEFAULT_MODEL_NAME is usually a fallback, the config.yaml setting takes precedence.
DEFAULT_MODEL_NAME = "gemini-1.5-flash" # Use a known stable default

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
    (re.compile(r"\b(20\d{2})(\d{2})(\d{2})\b"), "%Y%m%d"),                 # yyyymmdd (e.g., 20251023)
]

def _format_ddmmyy(d: dt.date) -> str:
    # Always output dd/mm/yy format
    return d.strftime("%d/%m/%y")

def _extract_date_from_name(name: str) -> Optional[str]:
    if not name: return None
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

                # Check if the parsed date is realistic (optional, e.g., not too far in past/future)
                # if dt.date(2020, 1, 1) <= parsed <= dt.date.today() + dt.timedelta(days=30):
                return _format_ddmmyy(parsed)
                # else:
                #     logging.debug(f"Extracted date {parsed} out of range, continuing search.")
                #     continue
            except ValueError: # Handles invalid date combinations (e.g., day 32)
                 logging.debug(f"Invalid date extracted {g} with format {fmt}, continuing search.")
                 continue
            except Exception as e:
                 logging.warning(f"Error parsing date {g} with format {fmt}: {e}")
                 continue # Continue searching with other patterns
    logging.debug(f"No date found in filename: {name}")
    return None


def _pick_date_for_output(file_name: str, created_time_iso: Optional[str]) -> str:
    """
    1) date in filename → 2) Drive createdTime → 3) "NA"; format dd/mm/yy
    """
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
    For audio, use mutagen (mp3/m4a/ogg/wav/etc.). Falls back to WAV wave reader if needed.
    Returns integer minutes as a string or "NA".
    """
    duration_str = "NA" # Default

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
            from mutagen import File as MutagenFile
            # Ensure mutagen is installed
            mf = MutagenFile(local_path)
            if mf is not None and getattr(mf, "info", None) and getattr(mf.info, "length", None):
                seconds = float(mf.info.length)
                if seconds > 0:
                     duration_str = f"{int(round(seconds / 60.0))}"
                     logging.info(f"Duration from mutagen: {duration_str} min")
                     return duration_str
        except ImportError:
             logging.warning("mutagen library not installed, cannot get duration for some audio types.")
        except Exception as e:
            logging.warning(f"Mutagen failed to read duration for {local_path}: {e}")

        # 3) WAV fallback without mutagen (if applicable)
        if duration_str == "NA" and local_path.lower().endswith(".wav"):
            try:
                import wave
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
                 logging.warning("wave module not available for WAV fallback.")
            except Exception as e:
                logging.warning(f"Wave module failed for {local_path}: {e}")

    if duration_str == "NA":
         logging.warning(f"Could not determine duration for {local_path}")

    return duration_str


# ---------- Society name from file name ----------
def _society_from_filename(name: str) -> str:
    """Derive Society Name from file name (drop extension, tidy separators)."""
    if not name: return "Unknown Society"
    base = os.path.splitext(name)[0]
    # Replace common separators with spaces and collapse whitespace
    base = re.sub(r'[_\-\|\.]+', ' ', base).strip() # Use regex for robustness
    # Collapse multiple spaces into one
    base = re.sub(r'\s+', ' ', base).strip()
    return base or "Unknown Society" # Return default if name becomes empty

# ---------- ERP/ASP coverage & missed-opps ----------
def _normalize_text(s: str) -> str:
    import re as _re
    if not s: return ""
    # Remove punctuation, convert to lowercase, collapse whitespace
    text = _re.sub(r'[^\w\s]', '', s) # Keep only word chars and spaces
    text = text.lower()
    text = _re.sub(r"\s+", " ", text).strip()
    return text

def _match_coverage(transcript: str, feature_map: Dict[str, List[str]]) -> Tuple[Set[str], Set[str]]:
    # Normalize transcript once
    normalized_transcript = _normalize_text(transcript)
    if not normalized_transcript: # Handle empty transcript case
         return set(), set(feature_map.keys()) # All missed if transcript is empty

    covered, missed = set(), set()
    for feature, keys in feature_map.items():
        # Normalize keywords (assuming they are relatively clean already)
        normalized_keys = [_normalize_text(k) for k in keys if k]
        # Check if any normalized keyword is in the normalized transcript
        # Use word boundaries for more precise matching if needed: fr'\b{key}\b'
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
    if not transcript: # Handle empty transcript
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
            "Automated reminders", "Late fee calc", "GST/TDS reports",
            "Vendor accounting", "Role-based access",
             "Finalisation support & audit coordination", "Bookkeeping (all incomes/expenses)"
            # Add others if needed
        ]

        missed_all = erp_missed.union(asp_missed)
        if not missed_all:
             missed_text = "" # No missed opportunities
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
             missed_text = "- " + "\n- ".join(missed_sorted) # Format as bullet points

        return feature_coverage_summary or "NA", missed_text or "NA" # Return NA if empty
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
For score fields ("Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling", "Closing Effectiveness", "Negotiation Strength"), output numeric strings between "2" (poor/absent) and "10" (excellent). Calculate "Total Score" (sum of 5 scores) and "% Score" (Total/50 as percentage string with one decimal).
Fill all other keys based on the transcript content or "NA" if absent.
""").strip()
    # Ensure fallback prompt is not empty
    if not fallback_prompt:
         logging.error("CRITICAL: Fallback schema prompt is also empty!")
         # You might want to raise an error here if a prompt is absolutely required
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
# BLOCK_NONE might be too permissive, consider BLOCK_ONLY_HIGH or BLOCK_MEDIUM_AND_ABOVE
# Check Google AI documentation for category names and levels
SAFETY_SETTINGS = {
    'HARM_CATEGORY_HARASSMENT': 'block_none',
    'HARM_CATEGORY_HATE_SPEECH': 'block_none',
    'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'block_none',
    'HARM_CATEGORY_DANGEROUS_CONTENT': 'block_none',
}

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       # Only retry on QuotaExceeded or specific transient RuntimeErrors
       retry=retry_if_exception_type((QuotaExceeded,))) # Add specific RuntimeErrors if identified
def _gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    """Transcribes audio/video using Gemini with retries for quota."""
    try:
        model = genai.GenerativeModel(model_name)
        logging.info(f"Uploading file for transcription: {file_path} ({mime_type})")

        # Handle empty file before upload
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logging.warning(f"Transcription input file is empty or missing: {file_path}. Returning empty transcript.")
            return ""

        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        logging.info(f"File uploaded successfully for transcription: {uploaded.name}")

        prompt = "Transcribe the audio verbatim with punctuation. Do not summarize. Output plain text only."
        resp = model.generate_content([uploaded, {"text": prompt}], safety_settings=SAFETY_SETTINGS)

        # Check for blocks or empty candidates
        if resp.prompt_feedback.block_reason:
            logging.error(f"Transcription prompt blocked: {resp.prompt_feedback.block_reason}")
            raise RuntimeError(f"Transcription prompt blocked: {resp.prompt_feedback.block_reason}")
        if not resp.candidates:
            logging.error("No candidates returned during transcription. Check safety settings or prompt.")
            raise RuntimeError("No response candidates received during transcription.")
        # Check finish reason
        finish_reason = resp.candidates[0].finish_reason
        if finish_reason not in ('STOP', 'MAX_TOKENS'): # Allow MAX_TOKENS, but log warning
             logging.warning(f"Transcription generation finished with reason: {finish_reason}")
             if finish_reason == 'SAFETY':
                  logging.error(f"Transcription blocked by safety settings: {resp.candidates[0].safety_ratings}")
                  raise RuntimeError(f"Transcription blocked by safety settings.")
             # Consider raising error for other reasons like RECITATION

        text = (resp.text or "").strip()
        if not text and finish_reason == 'STOP':
            # Log if empty but stopped normally (e.g., silent audio)
            logging.warning("Received empty transcript from Gemini (stopped normally).")

        # Cleanup uploaded file on Gemini side (optional, helps manage storage)
        try:
             genai.delete_file(uploaded.name)
             logging.info(f"Deleted uploaded file: {uploaded.name}")
        except Exception as del_e:
             logging.warning(f"Could not delete uploaded file {uploaded.name}: {del_e}")

        return text

    except Exception as e:
        if _is_quota_error(e):
            logging.error(f"Quota exceeded during TRANSCRIBE call: {e}")
            raise QuotaExceeded(str(e)) # Re-raise specific exception
        # Log other errors clearly
        logging.error(f"Error during transcription call: {e}", exc_info=True)
        # Wrap other exceptions in RuntimeError for consistency if desired
        raise RuntimeError(f"Transcription failed: {e}") from e

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded,)))
def _gemini_analyze(transcript: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    """Analyzes transcript using Gemini, expecting JSON output."""
    try:
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(
            [
                {"text": master_prompt},
                {"text": "\n\n---\nMEETING TRANSCRIPT:\n"},
                {"text": transcript},
            ],
            generation_config={
                "temperature": 0.2, # Low temperature for consistent JSON structure
                "response_mime_type": "application/json",
            },
             safety_settings=SAFETY_SETTINGS
        )

        # Check for blocks or empty candidates
        if resp.prompt_feedback.block_reason:
            logging.error(f"Analysis prompt blocked: {resp.prompt_feedback.block_reason}")
            raise RuntimeError(f"Analysis prompt blocked: {resp.prompt_feedback.block_reason}")
        if not resp.candidates:
            logging.error("No candidates returned during analysis.")
            raise RuntimeError("No response candidates received during analysis.")
        finish_reason = resp.candidates[0].finish_reason
        if finish_reason != 'STOP':
             logging.warning(f"Analysis generation finished with reason: {finish_reason}")
             if finish_reason == 'SAFETY':
                  logging.error(f"Analysis blocked by safety settings: {resp.candidates[0].safety_ratings}")
                  raise RuntimeError(f"Analysis blocked by safety settings.")
             # Potentially raise error for other reasons

        # Robust JSON parsing
        raw = (resp.text or "").strip()
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

        return data

    except Exception as e:
        if _is_quota_error(e):
            logging.error(f"Quota exceeded during ANALYZE call: {e}")
            raise QuotaExceeded(str(e))
        logging.error(f"Error during analysis call: {e}", exc_info=True)
        raise RuntimeError(f"Analysis failed: {e}") from e

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded,)))
def _gemini_one_shot(file_path: str, mime_type: str, master_prompt: str, model_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Single-call path: upload audio + ask for final JSON directly.
    Includes logic to add transcript separately if missing.
    """
    uploaded_file_name = None # Keep track of uploaded file name for deletion
    try:
        model = genai.GenerativeModel(model_name)

        logging.info(f"Uploading file for one-shot: {file_path} ({mime_type})")
        if not os.path.exists(file_path):
             raise FileNotFoundError(f"File not found for one-shot upload: {file_path}")
        file_size = os.path.getsize(file_path)
        if file_size == 0:
             logging.warning(f"File is empty, skipping one-shot processing: {file_path}")
             # Return a default JSON indicating the error, matching expected structure if possible
             return {"error": "Input file was empty", "Risks / Unresolved Issues": "Input file was empty"}

        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        uploaded_file_name = uploaded.name # Store for cleanup
        logging.info(f"File uploaded successfully for one-shot: {uploaded_file_name}")

        resp = model.generate_content(
            [uploaded, {"text": master_prompt}],
            generation_config={
                "temperature": 0.2,
                "response_mime_type": "application/json",
            },
             safety_settings=SAFETY_SETTINGS
        )

        # Detailed feedback check
        if resp.prompt_feedback.block_reason:
             logging.error(f"One-shot prompt blocked: {resp.prompt_feedback.block_reason}")
             logging.error(f"Safety ratings: {resp.prompt_feedback.safety_ratings}")
             raise RuntimeError(f"Prompt blocked due to {resp.prompt_feedback.block_reason}")
        if not resp.candidates:
             logging.error("No candidates returned from one-shot call.")
             raise RuntimeError("No response candidates received from Gemini.")
        finish_reason = resp.candidates[0].finish_reason
        if finish_reason != 'STOP':
             logging.warning(f"One-shot generation finished with reason: {finish_reason}")
             if finish_reason == 'SAFETY':
                  logging.error(f"One-shot blocked by safety settings: {resp.candidates[0].safety_ratings}")
                  raise RuntimeError(f"One-shot blocked by safety settings.")
             # Consider raising error for MAX_TOKENS if full JSON is critical

        # Robust JSON parsing
        raw = (resp.text or "").strip()
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

        # --- Add full transcript if missing ---
        # Only do this if the key is expected and missing/empty
        if "transcript_full" in master_prompt and ("transcript_full" not in data or not data["transcript_full"]):
            logging.warning("Transcript missing from one-shot JSON, attempting separate transcription...")
            try:
                # Use the primary model for transcription
                transcription_model = _get_model(config)
                # Need to re-upload the file for transcription call
                transcript_text = _gemini_transcribe(file_path, mime_type, transcription_model)
                data["transcript_full"] = transcript_text or "Transcription failed or empty"
                logging.info("Successfully added transcript via separate call.")
            except QuotaExceeded as qe_transcribe:
                 logging.error(f"Quota exceeded during separate transcription attempt: {qe_transcribe}")
                 data["transcript_full"] = "Transcription failed due to quota"
                 # Do not re-raise quota here, let the main process decide based on primary call failure
            except Exception as te:
                logging.error(f"Failed to get transcript separately after one-shot: {te}")
                data["transcript_full"] = f"Transcription failed: {te}" # Indicate failure

        return data

    except Exception as e:
        if _is_quota_error(e):
            logging.error(f"Quota exceeded during ONE-SHOT call: {e}")
            raise QuotaExceeded(str(e))
        logging.error(f"Error during one-shot processing call: {e}", exc_info=True)
        raise RuntimeError(f"One-shot processing failed: {e}") from e
    finally:
         # Cleanup uploaded file from one-shot call
         if uploaded_file_name:
              try:
                   genai.delete_file(uploaded_file_name)
                   logging.info(f"Deleted uploaded file (one-shot): {uploaded_file_name}")
              except Exception as del_e:
                   logging.warning(f"Could not delete uploaded file {uploaded_file_name} (one-shot): {del_e}")


# ---------- Manager info enrichment ----------
def _augment_with_manager_info(analysis_obj: Dict[str, Any], member_name: str, config: Dict[str, Any]) -> None:
    """
    Fill Owner/Email/Manager/Team/Manager Email using config.manager_map / manager_emails.
    Overrides existing values in analysis_obj with info from config based on member_name.
    """
    if not member_name:
         logging.warning("Cannot augment manager info: member_name is empty.")
         analysis_obj["Owner (Who handled the meeting)"] = "Unknown"
         return

    analysis_obj["Owner (Who handled the meeting)"] = member_name.strip() # Set owner first
    try:
        # Assume keys in manager_map match member_name case/spacing or normalize here
        manager_map = config.get("manager_map", {})
        member_info = manager_map.get(member_name.strip()) # Use strip()

        if member_info and isinstance(member_info, dict):
            logging.info(f"Found manager map entry for '{member_name}'")
            analysis_obj["Email Id"] = member_info.get("Email", analysis_obj.get("Email Id", ""))
            analysis_obj["Manager"] = member_info.get("Manager", analysis_obj.get("Manager", ""))
            analysis_obj["Team"] = member_info.get("Team", analysis_obj.get("Team", ""))

            manager_name = member_info.get("Manager")
            if manager_name:
                manager_emails = config.get("manager_emails", {})
                # Assume keys in manager_emails match manager_name case/spacing
                mgr_email = manager_emails.get(manager_name.strip()) # Use strip()
                analysis_obj["Manager Email"] = mgr_email if mgr_email is not None else analysis_obj.get("Manager Email", "")
            else:
                 # If no manager in map, keep existing or set default
                 analysis_obj.setdefault("Manager Email", "")
        else:
            logging.warning(f"No manager map entry found or entry invalid for owner: '{member_name}'")
            # Ensure keys exist even if no mapping found
            analysis_obj.setdefault("Email Id", "")
            analysis_obj.setdefault("Manager", "")
            analysis_obj.setdefault("Team", "")
            analysis_obj.setdefault("Manager Email", "")

    except Exception as e:
        logging.error(f"Error during manager info augmentation for '{member_name}': {e}", exc_info=True)
        # Ensure owner is still set
        analysis_obj["Owner (Who handled the meeting)"] = member_name.strip()


# ---------- Sheets I/O ----------
def _write_success(gsheets_sheet, file_id: str, file_name: str, date_out: str, duration_min: str,
                   feature_coverage: str, missed_opps: str, analysis_obj: Dict[str, Any],
                   member_name: str, config: Dict[str, Any]):
    """Writes the processed analysis object to Google Sheets and updates the ledger."""
    # Import sheets locally to avoid potential circular dependencies if sheets uses analysis
    try:
        import sheets
    except ImportError:
        logging.critical("Failed to import 'sheets' module. Cannot write results to Google Sheets.")
        raise RuntimeError("Sheets module not found") # Raise error as writing is critical

    # --- Pre-write Validation and Cleaning ---
    if not isinstance(analysis_obj, dict):
        logging.error(f"Analysis result for {file_name} is not a dictionary (Type: {type(analysis_obj)}). Cannot write to sheet.")
        # Update ledger with specific error and stop writing
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
    for key, value in analysis_obj.items():
         if value is None and key not in score_keys:
              cleaned_analysis[key] = "NA"
         # Convert score values to string if they aren't already (defensive coding)
         elif key in score_keys and value is not None:
             cleaned_analysis[key] = str(value)
         elif value is None and key in score_keys:
              # Handle missing scores - Default to "2" as per rule?
              logging.warning(f"Score field '{key}' is None for {file_name}. Defaulting to '2'.")
              cleaned_analysis[key] = "2" # Default missing score
         else:
             # Ensure all values are strings for sheet writing? Check sheets module requirements
             cleaned_analysis[key] = str(value) if not isinstance(value, str) else value


    # --- Populate Standard Fields ---
    # Override Date, Duration, Society Name regardless of model output
    cleaned_analysis["Date"] = date_out if date_out != "NA" else "NA" # Ensure NA if extraction failed
    cleaned_analysis["Meeting duration (min)"] = duration_min if duration_min != "NA" else "NA"
    cleaned_analysis["Society Name"] = _society_from_filename(file_name)

    # Add calculated coverage/missed opportunities, ensuring keys exist
    cleaned_analysis["Feature Checklist Coverage"] = feature_coverage if feature_coverage not in ["NA", "Error calculating coverage"] else cleaned_analysis.get("Feature Checklist Coverage", "NA")
    cleaned_analysis["Missed Opportunities"] = missed_opps if missed_opps not in ["NA", "Error calculating missed opportunities"] else cleaned_analysis.get("Missed Opportunities", "NA")

    # --- Augment with Manager Info ---
    try:
        _augment_with_manager_info(cleaned_analysis, member_name, config)
    except Exception as e:
         logging.error(f"Failed during manager augmentation for {file_name}: {e}. Proceeding without full augmentation.")
         cleaned_analysis["Owner (Who handled the meeting)"] = member_name or "Unknown" # Ensure owner is set

    # --- Calculate Scores if Missing (Defensive) ---
    # Ensure score keys exist and attempt calculation if Total/% Score are missing/invalid
    scores_valid = True
    individual_scores = []
    for key in ["Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling", "Closing Effectiveness", "Negotiation Strength"]:
         cleaned_analysis.setdefault(key, "2") # Default to '2' if key is missing
         try:
             individual_scores.append(int(cleaned_analysis[key]))
         except (ValueError, TypeError):
             logging.warning(f"Invalid score value '{cleaned_analysis[key]}' for {key} in {file_name}. Defaulting score part to 2.")
             individual_scores.append(2) # Use default if conversion fails
             scores_valid = False

    try:
        current_total = int(cleaned_analysis.get("Total Score", "0"))
        expected_total = sum(individual_scores)
        if current_total != expected_total or not scores_valid:
             logging.warning(f"Recalculating Total Score for {file_name}. Was: {current_total}, Expected: {expected_total}")
             cleaned_analysis["Total Score"] = str(expected_total)
             # Recalculate % Score based on new total
             percent_score = (expected_total / 50.0) * 100
             cleaned_analysis["% Score"] = f"{percent_score:.1f}%"
    except (ValueError, TypeError):
        logging.error(f"Could not validate/recalculate scores for {file_name}. Using potentially incorrect values from model.")
        cleaned_analysis.setdefault("Total Score", "NA")
        cleaned_analysis.setdefault("% Score", "NA")


    # --- Write to Sheets ---
    status_note = f"Processed via Gemini model; duration={duration_min}m"
    try:
        # Update ledger FIRST to indicate processing attempt is complete
        sheets.update_ledger(gsheets_sheet, file_id, "Processed", status_note, config, file_name)
    except Exception as e:
         # Log ledger failure but continue trying to write results
         logging.error(f"Failed to update ledger to 'Processed' for {file_name}: {e}")

    # Write the actual analysis row
    try:
        # Use the specific function name from sheets module
        if hasattr(sheets, "write_analysis_result"):
            sheets.write_analysis_result(gsheets_sheet, cleaned_analysis, config)
        elif hasattr(sheets, "append_result"): # Support alternative name
             sheets.append_result(gsheets_sheet, cleaned_analysis, config)
        else:
            raise AttributeError("Function 'write_analysis_result' or 'append_result' not found in sheets module.")
        logging.info(f"Successfully wrote analysis results to sheet for {file_name}")
    except Exception as e:
         logging.error(f"CRITICAL: Failed to write analysis results to sheet for {file_name}: {e}", exc_info=True)
         # If writing results fails AFTER ledger was set to Processed, update ledger back to Error
         try:
              error_msg = f"Failed to write results to sheet: {str(e)[:200]}" # Limit error msg length
              sheets.update_ledger(gsheets_sheet, file_id, "Error", error_msg, config, file_name)
              logging.info(f"Updated ledger back to 'Error' for {file_name} due to sheet write failure.")
         except Exception as le:
              logging.error(f"CRITICAL: Also failed to update ledger back to Error status for {file_name}: {le}")
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
    # Get initial name, might be updated after metadata fetch
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
            logging.info(f"[Gemini] Processing: '{file_name}' ({mime_type}) | ID: {file_id}")
        except Exception as e:
            logging.error(f"Metadata fetch failed for {file_id}: {e}")
            # Try importing sheets here for ledger update on failure
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Metadata fetch failed: {str(e)[:200]}", config, file_name)
            except Exception as le:
                logging.error(f"Failed to import sheets or update ledger for metadata error on {file_id}: {le}")
            raise # Re-raise to signal failure for this file

        # --- Basic Info Extraction ---
        date_out = _pick_date_for_output(file_name, created_iso)

        # --- Download ---
        tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        # Sanitize filename robustly
        safe_suffix = re.sub(r'[^\w\.\-]', '_', file_name) # Allow word chars, period, hyphen
        safe_suffix = re.sub(r'_+', '_', safe_suffix) # Collapse multiple underscores
        local_path = os.path.join(tmp_dir, f"{file_id}_{safe_suffix[:150]}") # Limit length

        try:
            logging.info(f"Starting download for {file_name}...")
            _, downloaded_mime_type = _download_drive_file(drive_service, file_id, local_path)
            mime_type = downloaded_mime_type or mime_type # Prefer downloaded type
            logging.info(f"File downloaded to: {local_path}")
        except Exception as e:
            logging.error(f"Download failed for {file_name} (ID: {file_id}): {e}")
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Download failed: {str(e)[:200]}", config, file_name)
            except Exception as le:
                 logging.error(f"Failed to import sheets or update ledger for download error on {file_id}: {le}")
            raise # Re-raise download failure

        # --- Duration ---
        duration_min = _probe_duration_minutes(meta, local_path, mime_type)

        # --- Prompt ---
        master_prompt = _load_master_prompt(config)
        if not master_prompt:
            # Error already logged in _load_master_prompt
            try:
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", "Master prompt could not be loaded", config, file_name)
            except Exception as le:
                 logging.error(f"Failed to import sheets or update ledger for prompt error on {file_id}: {le}")
            raise ValueError("Master prompt is empty, cannot proceed.") # Raise critical error

        # --- Gemini Processing (One-shot or Two-call) ---
        logging.info(f"Using one-shot mode: {_is_one_shot(config)}")
        transcript = ""

        if _is_one_shot(config):
            model_to_use = _get_analysis_model(config)
            logging.info(f"Calling Gemini one-shot with model: {model_to_use}")
            # Pass config to one-shot for potential separate transcription call
            analysis_obj = _gemini_one_shot(local_path, mime_type, master_prompt, model_to_use, config)
            transcript = analysis_obj.get("transcript_full", "") # Get transcript if available
            if not transcript or "Transcription failed" in transcript:
                 logging.warning(f"Transcript may be missing or failed in one-shot result for {file_name}")
        else: # Two-call approach
            # Transcribe
            transcribe_model = _get_model(config)
            logging.info(f"Calling Gemini transcribe with model: {transcribe_model}")
            transcript = _gemini_transcribe(local_path, mime_type, transcribe_model)
            logging.info(f"Transcription complete. Length: {len(transcript)} chars")

            if not transcript:
                logging.warning(f"Transcription resulted in empty text for {file_name}. Cannot perform analysis.")
                # Update ledger and stop for this file
                try:
                     import sheets
                     sheets.update_ledger(gsheets_sheet, file_id, "Error", "Empty transcript", config, file_name)
                except Exception as le:
                     logging.error(f"Failed to update ledger for empty transcript on {file_id}: {le}")
                # Use raise instead of return to be caught by main.py's exception handler
                raise ValueError("Empty transcript received, analysis skipped.")

            # Analyze
            analyze_model = _get_analysis_model(config)
            logging.info(f"Calling Gemini analyze with model: {analyze_model}")
            analysis_obj = _gemini_analyze(transcript, master_prompt, analyze_model)
            # Ensure transcript is in the final object
            analysis_obj["transcript_full"] = transcript
            logging.info(f"Analysis complete for {file_name}")

        # --- Feature Coverage & Missed Opps ---
        # Calculate AFTER getting analysis_obj and transcript
        feature_coverage, missed_opps = ("", "")
        try:
            # Prioritize calculated transcript if available
            calc_transcript = transcript or analysis_obj.get("transcript_full", "")
            if calc_transcript and "Transcription failed" not in calc_transcript:
                 feature_coverage, missed_opps = _feature_coverage_and_missed(calc_transcript)
                 logging.info("Calculated feature coverage and missed opportunities.")
            else:
                 logging.warning("Transcript unavailable or failed, cannot calculate coverage. Using defaults.")
                 feature_coverage = analysis_obj.get("Feature Checklist Coverage", "NA")
                 missed_opps = analysis_obj.get("Missed Opportunities", "NA")
        except Exception as e:
            logging.error(f"Error calculating feature coverage for {file_name}: {e}")
            feature_coverage = "Error in calculation"
            missed_opps = "Error in calculation"


        # --- Write Results ---
        # This function now handles validation, augmentation, score calculation, and sheet writing
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

    # --- Exception Handling for process_single_file ---
    except QuotaExceeded as qe:
         # Log specifically and re-raise for main loop to catch and stop run
         logging.error(f"Quota Exceeded during processing of '{file_name}': {qe}. Propagating error.")
         # DO NOT update ledger here, main.py will handle quarantine
         raise # Re-raise the QuotaExceeded exception
    except (RuntimeError, ValueError, FileNotFoundError, Exception) as e:
         # Catch specific errors raised within this function or underlying calls
         # These errors should have already updated the ledger to 'Error' where they occurred
         # Log the final failure point here and let main.py handle quarantine
         logging.error(f"Processing failed for '{file_name}' due to: {e}. File should be quarantined by main loop.", exc_info=True)
         # **IMPORTANT**: Re-raise the exception so main.py's except block catches it
         raise # Ensure the exception propagates to main.py for quarantine logic

    finally:
        # --- Cleanup: Remove downloaded file ---
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logging.info(f"Cleaned up temporary file: {local_path}")
            except Exception as e:
                # Log cleanup failure but don't prevent further processing
                logging.warning(f"Could not remove temporary file '{local_path}': {e}")
