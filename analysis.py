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

import google.generativeai as genai
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
        from google.api_core.exceptions import ResourceExhausted
        if isinstance(e, ResourceExhausted):
            return True
    except Exception:
        pass
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
DEFAULT_MODEL_NAME = "gemini-1.5-flash-preview-05-20"

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
    genai.configure(api_key=api_key)

def _drive_get_metadata(drive_service, file_id: str) -> Dict[str, Any]:
    """Fetch rich metadata: name, mimeType, createdTime, and duration for videos."""
    fields = (
        "id,name,mimeType,createdTime,"
        "videoMediaMetadata(durationMillis,height,width),"
        "size,parents"
    )
    return drive_service.files().get(fileId=file_id, fields=fields).execute()

def _download_drive_file(drive_service, file_id: str, out_path: str) -> Tuple[str, str]:
    """Download a Drive file to out_path. Returns (mime_type, out_path)."""
    meta = drive_service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
    mime_type = meta.get("mimeType", "")
    request = drive_service.files().get_media(fileId=file_id)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return mime_type, out_path

# ---------- Date extraction ----------
_DATE_PATTERNS = [
    (re.compile(r"\b(\d{1,2})[-/\.](\d{1,2})[-/\.](\d{4})\b"), "%d-%m-%Y"), # dd-mm-yyyy
    (re.compile(r"\b(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})\b"), "%Y-%m-%d"), # yyyy-mm-dd
    (re.compile(r"\b(20\d{2})(\d{2})(\d{2})\b"), "%Y%m%d"),                 # yyyymmdd
]

def _format_ddmmyy(d: dt.date) -> str:
    return d.strftime("%d/%m/%y")

def _extract_date_from_name(name: str) -> Optional[str]:
    base = os.path.splitext(name)[0]
    base = base.replace("|", " ").replace("_", " ").replace(".", " ")
    for rx, fmt in _DATE_PATTERNS:
        m = rx.search(base)
        if m:
            g = m.groups()
            try:
                if fmt == "%d-%m-%Y":
                    dd, mm, yyyy = int(g[0]), int(g[1]), int(g[2])
                    parsed = dt.date(yyyy, mm, dd)
                elif fmt == "%Y-%m-%d":
                    yyyy, mm, dd = int(g[0]), int(g[1]), int(g[2])
                    parsed = dt.date(yyyy, mm, dd)
                elif fmt == "%Y%m%d":
                    yyyy, mm, dd = int(g[0]), int(g[1]), int(g[2])
                    parsed = dt.date(yyyy, mm, dd)
                else:
                    continue
                return _format_ddmmyy(parsed)
            except Exception:
                continue
    return None

def _pick_date_for_output(file_name: str, created_time_iso: Optional[str]) -> str:
    """
    1) date in filename → 2) Drive createdTime → 3) "NA"; format dd/mm/yy
    """
    from_name = _extract_date_from_name(file_name)
    if from_name:
        return from_name
    if created_time_iso:
        try:
            d = dt.datetime.fromisoformat(created_time_iso.replace("Z", "+00:00")).date()
            return _format_ddmmyy(d)
        except Exception:
            pass
    return "NA"

# ---------- Duration extraction ----------
def _millis_to_minutes(ms: int) -> str:
    return f"{int(round(ms / 60000.0))}"

def _probe_duration_minutes(meta: Dict[str, Any], local_path: str, mime_type: str) -> str:
    """
    Prefers Drive videoMediaMetadata.durationMillis (videos).
    For audio, use mutagen (mp3/m4a/ogg/wav/etc.). Falls back to WAV wave reader if needed.
    Returns integer minutes as a string.
    """
    # 1) Video metadata from Drive
    vmeta = meta.get("videoMediaMetadata") or {}
    dur_ms = vmeta.get("durationMillis")
    if isinstance(dur_ms, (int, float)) and dur_ms > 0:
        return _millis_to_minutes(int(dur_ms))

    # 2) Audio duration via mutagen
    try:
        from mutagen import File as MutagenFile
        mf = MutagenFile(local_path)
        if mf is not None and getattr(mf, "info", None) and getattr(mf.info, "length", None):
            seconds = float(mf.info.length)
            return f"{int(round(seconds / 60.0))}"
    except Exception:
        pass

    # 3) WAV fallback without mutagen (if applicable)
    if local_path.lower().endswith(".wav"):
        try:
            import wave
            with wave.open(local_path, "rb") as w:
                frames = w.getnframes()
                rate = w.getframerate()
                seconds = frames / float(rate)
                return f"{int(round(seconds / 60.0))}"
        except Exception:
            pass

    return "NA"

# ---------- Society name from file name ----------
def _society_from_filename(name: str) -> str:
    """Derive Society Name from file name (drop extension, tidy separators)."""
    base = os.path.splitext(name or "")[0]
    # Replace common separators with spaces and collapse whitespace
    base = base.replace("_", " ").replace("-", " ").replace(".", " ").strip()
    base = " ".join(base.split())
    return base or "Unknown"

# ---------- ERP/ASP coverage & missed-opps ----------
def _normalize_text(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", s or "").strip().lower()

def _match_coverage(transcript: str, feature_map: Dict[str, List[str]]) -> Tuple[Set[str], Set[str]]:
    text = _normalize_text(transcript)
    covered, missed = set(), set()
    for feature, keys in feature_map.items():
        hit = any(k in text for k in keys)
        if hit:
            covered.add(feature)
        else:
            missed.add(feature)
    return covered, missed

def _build_feature_summary(covered_all: Set[str], total: int, label: str) -> str:
    pct = 0 if total == 0 else int(round(100 * len(covered_all) / total))
    covered_list = sorted(covered_all)
    head = f"{label} Coverage: {len(covered_all)}/{total} ({pct}%)."
    if covered_list:
        head += " Covered: " + ", ".join(covered_list) + "."
    return head

def _feature_coverage_and_missed(transcript: str) -> Tuple[str, str]:
    erp_cov, erp_missed = _match_coverage(transcript, ERP_FEATURES)
    asp_cov, asp_missed = _match_coverage(transcript, ASP_FEATURES)
    feature_coverage_summary = " ".join([
        _build_feature_summary(erp_cov, len(ERP_FEATURES), "ERP"),
        _build_feature_summary(asp_cov, len(ASP_FEATURES), "ASP")
    ])
    priority = [
        "Tally import/export", "Bank reconciliation", "UPI/cards gateway",
        "Defaulter tracking", "PO / WO approvals", "Inventory",
        "Managed accounting (bills & receipts)", "Bank reconciliation + suspense",
        "Financial reports (non-audited)", "Dedicated remote accountant"
    ]
    missed_all = list(sorted(erp_missed.union(asp_missed)))
    missed_sorted = sorted(
        missed_all,
        key=lambda x: (0 if x in priority else 1, priority.index(x) if x in priority else 999, x)
    )
    missed_text = ", ".join(missed_sorted) if missed_sorted else ""
    return feature_coverage_summary, missed_text

# ---------- Gemini config & calls ----------
# Gets the primary model from config, falling back to DEFAULT_MODEL_NAME if needed
def _get_model(config: Dict[str, Any]) -> str:
    return config.get("google_llm", {}).get("model", DEFAULT_MODEL_NAME) # Changed fallback

# Gets the analysis model, falling back to the primary model
def _get_analysis_model(config: Dict[str, Any]) -> str:
    return config.get("google_llm", {}).get("analysis_model", _get_model(config))

def _load_master_prompt(config: Dict[str, Any]) -> str:
    prompt_path = os.path.join(os.getcwd(), "prompt.txt")
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    # Fallback prompt if prompt.txt is missing
    return config.get("google_llm", {}).get("schema_prompt", """
Act as an expert business analyst for society-management ERP & ASP meetings.
Strictly return a single JSON object (no prose outside JSON).
If a field is unknown, set it to "" (empty string).
""").strip()

def _is_one_shot(config: Dict[str, Any]) -> bool:
    # ENV override first; then default to True unless explicitly disabled
    env = os.getenv("GEMINI_ONE_SHOT")
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "on")
    return bool(config.get("google_llm", {}).get("one_shot", True))

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded, RuntimeError)))
def _gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    try:
        model = genai.GenerativeModel(model_name)
        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        prompt = "Transcribe the audio verbatim with punctuation. Do not summarize. Output plain text only."
        # Use safety_settings to potentially allow more content types if needed
        resp = model.generate_content([uploaded, {"text": prompt}], safety_settings={'HARASSMENT': 'block_none'})
        if getattr(resp, "prompt_feedback", None) and getattr(resp.prompt_feedback, "block_reason", None):
             # Log the specific block reason
             logging.error(f"Transcription prompt blocked: {resp.prompt_feedback.block_reason}")
             # Depending on the reason, you might want to retry differently or just raise
             raise RuntimeError(f"Prompt blocked: {resp.prompt_feedback.block_reason}")
        text = (resp.text or "").strip()
        if not text:
             # It might be empty due to safety settings even if not explicitly blocked
             logging.warning("Received empty transcript from Gemini. Check safety settings or content.")
             # Decide if this should be a hard error or return empty string
             # For now, let's treat it as potentially valid (e.g., silent audio)
             # raise RuntimeError("Empty transcript from Gemini.")
        return text
    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during TRANSCRIBE call.")
            raise QuotaExceeded(str(e))
        # Log other specific API errors if helpful
        logging.error(f"Error during transcription: {e}", exc_info=True)
        raise

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded, RuntimeError)))
def _gemini_analyze(transcript: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    try:
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(
            [
                {"text": master_prompt},
                {"text": "\n\n---\nMEETING TRANSCRIPT:\n"},
                {"text": transcript},
            ],
            generation_config={
                "temperature": 0.2,
                "response_mime_type": "application/json",
            },
             # Consider adding safety settings here too if content might be sensitive
             safety_settings={'HARASSMENT': 'block_none'}
        )
        if getattr(resp, "prompt_feedback", None) and getattr(resp.prompt_feedback, "block_reason", None):
             logging.error(f"Analysis prompt blocked: {resp.prompt_feedback.block_reason}")
             raise RuntimeError(f"Prompt blocked: {resp.prompt_feedback.block_reason}")

        # Improved JSON parsing robustness
        raw = (resp.text or "").strip()
        # Handle potential markdown code fences
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
            logging.error(f"Failed to parse JSON from model. Raw response: '{raw[:500]}...'")
            raise RuntimeError(f"Failed to parse JSON from model: {je}") from je

        if not isinstance(data, dict):
             logging.error(f"Analysis output is not a JSON object. Type: {type(data)}. Raw: '{raw[:500]}...'")
             raise RuntimeError("Analysis output is not a JSON object.")
        return data

    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during ANALYZE call.")
            raise QuotaExceeded(str(e))
        # Log other API errors
        logging.error(f"Error during analysis: {e}", exc_info=True)
        raise

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded, RuntimeError)))
def _gemini_one_shot(file_path: str, mime_type: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    """
    Single-call path: upload audio + ask for final JSON directly.
    If your prompt includes `transcript_full`, we'll use it for deterministic coverage.
    """
    try:
        model = genai.GenerativeModel(model_name)

        logging.info(f"Uploading file for one-shot: {file_path} ({mime_type})")
        # Check file existence and size before upload
        if not os.path.exists(file_path):
             raise FileNotFoundError(f"File not found for upload: {file_path}")
        file_size = os.path.getsize(file_path)
        if file_size == 0:
             logging.warning(f"File is empty, skipping upload: {file_path}")
             # Decide how to handle empty files - raise error or return default JSON?
             # Returning a default might be safer to avoid breaking the whole run.
             return {"error": "Input file was empty"} # Example default/error structure

        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        logging.info(f"File uploaded successfully: {uploaded.name}")

        resp = model.generate_content(
            [uploaded, {"text": master_prompt}],
            generation_config={
                "temperature": 0.2,
                "response_mime_type": "application/json",
            },
             safety_settings={'HARASSMENT': 'block_none'} # Add safety settings
        )

        # More detailed feedback check
        if resp.prompt_feedback.block_reason:
             logging.error(f"One-shot prompt blocked: {resp.prompt_feedback.block_reason}")
             logging.error(f"Safety ratings: {resp.prompt_feedback.safety_ratings}")
             raise RuntimeError(f"Prompt blocked due to {resp.prompt_feedback.block_reason}")
        if not resp.candidates:
             logging.error("No candidates returned from one-shot call. Check safety settings or prompt.")
             logging.error(f"Finish reason: {resp.candidates[0].finish_reason if resp.candidates else 'N/A'}")
             logging.error(f"Safety ratings: {resp.candidates[0].safety_ratings if resp.candidates else 'N/A'}")
             raise RuntimeError("No response candidates received from Gemini.")
        if resp.candidates[0].finish_reason != 'STOP':
             logging.warning(f"One-shot generation finished with reason: {resp.candidates[0].finish_reason}")
             # Potentially raise error depending on finish reason (e.g., MAX_TOKENS)

        # Robust JSON parsing
        raw = resp.text.strip()
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
            logging.error(f"Failed to parse JSON from one-shot model. Raw: '{raw[:500]}...'")
            raise RuntimeError(f"Failed to parse JSON from one-shot model: {je}") from je

        if not isinstance(data, dict):
             logging.error(f"One-shot output is not a JSON object. Type: {type(data)}. Raw: '{raw[:500]}...'")
             raise RuntimeError("One-shot output is not a JSON object.")

        # ---- Attempt to add full transcript if missing and one-shot was used ----
        # This requires a separate transcription call ONLY if 'transcript_full' is missing
        # AND you are in one-shot mode. This adds cost but ensures coverage calculation.
        # Consider if this is worth the extra API call.
        if "transcript_full" not in data or not data["transcript_full"]:
            logging.warning("Transcript missing from one-shot JSON, attempting separate transcription...")
            try:
                # Use the primary model for transcription if different
                transcription_model = _get_model(config)
                # Need to re-upload or use existing upload if handle is kept
                # Simpler: re-upload for now
                transcript_text = _gemini_transcribe(file_path, mime_type, transcription_model)
                data["transcript_full"] = transcript_text
                logging.info("Successfully added transcript via separate call.")
            except Exception as te:
                logging.error(f"Failed to get transcript separately after one-shot: {te}")
                data["transcript_full"] = "Transcription failed" # Indicate failure

        return data

    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during ONE-SHOT call.")
            raise QuotaExceeded(str(e))
        # Log other API errors
        logging.error(f"Error during one-shot processing: {e}", exc_info=True)
        raise

# ---------- Manager info enrichment ----------
def _augment_with_manager_info(analysis_obj: Dict[str, Any], member_name: str, config: Dict[str, Any]) -> None:
    """
    Fill Owner/Email/Manager/Team/Manager Email using config.manager_map / manager_emails.
    We OVERRIDE to ensure correctness based on folder ownership.
    """
    analysis_obj["Owner (Who handled the meeting)"] = member_name or "Unknown" # Ensure owner always set
    try:
        # Normalize member_name for lookup if necessary (e.g., case-insensitive)
        normalized_member_name = member_name.strip() # Add .lower() if map keys are lowercase
        m = (config.get("manager_map") or {}).get(normalized_member_name, {})
        if m: # Only update if a mapping was found
             analysis_obj["Email Id"] = m.get("Email", analysis_obj.get("Email Id", "")) # Keep existing if map is empty
             analysis_obj["Manager"] = m.get("Manager", analysis_obj.get("Manager", ""))
             analysis_obj["Team"] = m.get("Team", analysis_obj.get("Team", ""))
             manager_name = m.get("Manager")
             if manager_name:
                 # Normalize manager name for email lookup too
                 mgr_email = (config.get("manager_emails") or {}).get(manager_name.strip(), "") # Add .lower() if keys are lowercase
                 analysis_obj["Manager Email"] = mgr_email or analysis_obj.get("Manager Email", "")
             else:
                  analysis_obj["Manager Email"] = analysis_obj.get("Manager Email", "") # Keep existing if no manager in map
        else:
             logging.warning(f"No manager map entry found for owner: '{member_name}'")
             # Ensure defaults if no map entry
             analysis_obj.setdefault("Email Id", "")
             analysis_obj.setdefault("Manager", "")
             analysis_obj.setdefault("Team", "")
             analysis_obj.setdefault("Manager Email", "")

    except Exception as e:
        logging.error(f"Error during manager info augmentation for '{member_name}': {e}", exc_info=True)
        # Ensure owner is still set even if mapping lookup fails
        analysis_obj["Owner (Who handled the meeting)"] = member_name or "Unknown"


# ---------- Sheets I/O ----------
def _write_success(gsheets_sheet, file_id: str, file_name: str, date_out: str, duration_min: str,
                   feature_coverage: str, missed_opps: str, analysis_obj: Dict[str, Any],
                   member_name: str, config: Dict[str, Any]):
    # Import sheets locally to avoid circular dependency if sheets uses analysis
    try:
        import sheets
    except ImportError:
        logging.error("Failed to import 'sheets' module. Cannot write results.")
        return # Cannot proceed without sheets module

    # --- Pre-write Validation and Cleaning ---
    # Ensure analysis_obj is a dict
    if not isinstance(analysis_obj, dict):
        logging.error(f"Analysis result is not a dictionary for file {file_name}. Type: {type(analysis_obj)}. Skipping write.")
        # Update ledger with error?
        sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Invalid analysis result type: {type(analysis_obj)}", config, file_name)
        return

    # Handle potential None values returned from model, replace with "NA" or "" based on prompt rules
    for key, value in analysis_obj.items():
        if value is None:
            # Check prompt rules if None should be "NA" or ""
            # Assuming prompt rule implies "NA" for missing facts
            # If a field *exists* but is None, maybe "" is better? Adjust as needed.
            analysis_obj[key] = "NA" # Or "" depending on desired output

    # --- Populate Standard Fields ---
    analysis_obj["Date"] = date_out if date_out != "NA" else analysis_obj.get("Date", "NA") # Use extracted if valid
    analysis_obj["Meeting duration (min)"] = duration_min if duration_min != "NA" else analysis_obj.get("Meeting duration (min)", "NA")
    analysis_obj["Society Name"] = _society_from_filename(file_name) # Always derive from filename

    # Add coverage/missed only if calculated and not empty
    if feature_coverage:
        analysis_obj["Feature Checklist Coverage"] = feature_coverage
    else:
        # Ensure key exists even if empty, using model's value or default ""
        analysis_obj.setdefault("Feature Checklist Coverage", "")

    if missed_opps:
        analysis_obj["Missed Opportunities"] = missed_opps
    else:
        # Ensure key exists
        analysis_obj.setdefault("Missed Opportunities", "")


    # Ensure enrichment is applied (idempotent override)
    try:
        _augment_with_manager_info(analysis_obj, member_name, config)
    except Exception as e:
         logging.error(f"Failed during manager augmentation for {file_name}: {e}")
         # Continue without augmentation if it fails, owner name should still be set


    # --- Write to Sheets ---
    status_note = f"Processed via Gemini; duration={duration_min}m"
    try:
        sheets.update_ledger(gsheets_sheet, file_id, "Processed", status_note, config, file_name)
    except Exception as e:
         logging.error(f"Failed to update ledger for {file_name} (Processed): {e}")
         # Continue to write results even if ledger fails

    # Write the actual row to "Analysis Results"
    try:
        if hasattr(sheets, "write_analysis_result"):
            sheets.write_analysis_result(gsheets_sheet, analysis_obj, config)
            logging.info(f"Successfully wrote analysis results to sheet for {file_name}")
        elif hasattr(sheets, "append_result"): # Check for alternative name
             sheets.append_result(gsheets_sheet, analysis_obj, config)
             logging.info(f"Successfully wrote analysis results (via append_result) to sheet for {file_name}")
        else:
            logging.warning("sheets.write_analysis_result (or append_result) not found; results row not appended.")
    except Exception as e:
         logging.error(f"Failed to write analysis results to sheet for {file_name}: {e}")
         # Consider updating ledger back to Error status here?
         try:
              sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Failed to write results to sheet: {e}", config, file_name)
         except Exception as le:
              logging.error(f"Also failed to update ledger to Error status for {file_name}: {le}")


# =========================
# Entry point
# =========================
def process_single_file(drive_service, gsheets_sheet, file_meta: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    """
    Orchestrates: metadata -> download -> (one-shot OR transcribe+analyze) -> enrich -> write.
    Exceptions are allowed to bubble so main.py can quarantine & stop on quota.
    """
    local_path = None # Define local_path here to ensure it's available in finally block
    file_id = file_meta["id"]
    # Get initial name from metadata if possible, fallback later
    file_name = file_meta.get("name", f"Unknown_{file_id}")

    try:
        _init_gemini() # Ensure Gemini is configured

        # --- Metadata Fetch ---
        try:
             meta = _drive_get_metadata(drive_service, file_id)
             # Update file_name with potentially more accurate version from metadata
             file_name = meta.get("name", file_name)
             mime_type = meta.get("mimeType", file_meta.get("mimeType", ""))
             created_iso = meta.get("createdTime")
             logging.info(f"[Gemini] Processing: {file_name} ({mime_type}) | ID: {file_id}")
        except Exception as e:
             logging.error(f"Failed to get metadata for file ID {file_id}: {e}")
             # Update ledger with metadata error and skip processing this file
             import sheets # Import locally for error handling
             sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Metadata fetch failed: {e}", config, file_name)
             return # Stop processing this file

        # --- Basic Info Extraction ---
        date_out = _pick_date_for_output(file_name, created_iso)

        # --- Download ---
        tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        # Sanitize filename for local path
        safe_suffix = re.sub(r'[^\w\-_\.]', '_', file_name)
        local_path = os.path.join(tmp_dir, f"{file_id}_{safe_suffix}")

        try:
            _, downloaded_mime_type = _download_drive_file(drive_service, file_id, local_path)
            # Use downloaded mime_type if more accurate or available
            mime_type = downloaded_mime_type or mime_type
            logging.info(f"File downloaded to: {local_path}")
        except Exception as e:
            logging.error(f"Failed to download file {file_name} (ID: {file_id}): {e}")
            import sheets
            sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Download failed: {e}", config, file_name)
            return # Stop processing this file

        # --- Duration ---
        duration_min = _probe_duration_minutes(meta, local_path, mime_type)

        # --- Prompt ---
        master_prompt = _load_master_prompt(config)
        if not master_prompt:
             logging.error("Master prompt is empty. Cannot proceed with analysis.")
             import sheets
             sheets.update_ledger(gsheets_sheet, file_id, "Error", "Master prompt is empty", config, file_name)
             return

        # --- Gemini Processing (One-shot or Two-call) ---
        logging.info(f"Using one-shot mode: {_is_one_shot(config)}")
        transcript = ""
        analysis_obj = {}

        if _is_one_shot(config):
            try:
                model_to_use = _get_analysis_model(config) # One-shot uses analysis model
                logging.info(f"Calling Gemini one-shot with model: {model_to_use}")
                analysis_obj = _gemini_one_shot(local_path, mime_type, master_prompt, model_to_use)
                transcript = analysis_obj.get("transcript_full", "") # Attempt to get transcript from JSON
                if not transcript:
                     logging.warning(f"Transcript not found in one-shot result for {file_name}")
            except QuotaExceeded as qe:
                 logging.error(f"Quota exceeded during one-shot for {file_name}: {qe}")
                 raise # Re-raise QuotaExceeded to be caught by main loop
            except Exception as e:
                 logging.error(f"Error during Gemini one-shot call for {file_name}: {e}", exc_info=True)
                 import sheets
                 sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Gemini one-shot failed: {e}", config, file_name)
                 return # Stop processing this file
        else: # Two-call approach
            try:
                model_to_use = _get_model(config) # Transcription uses primary model
                logging.info(f"Calling Gemini transcribe with model: {model_to_use}")
                transcript = _gemini_transcribe(local_path, mime_type, model_to_use)
                logging.info(f"Transcription complete for {file_name}. Length: {len(transcript)} chars")
            except QuotaExceeded as qe:
                logging.error(f"Quota exceeded during transcription for {file_name}: {qe}")
                raise # Re-raise QuotaExceeded
            except Exception as e:
                logging.error(f"Error during Gemini transcription for {file_name}: {e}", exc_info=True)
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Transcription failed: {e}", config, file_name)
                return

            if not transcript:
                 logging.warning(f"Transcription resulted in empty text for {file_name}. Skipping analysis.")
                 import sheets
                 sheets.update_ledger(gsheets_sheet, file_id, "Error", "Empty transcript", config, file_name)
                 return

            try:
                model_to_use = _get_analysis_model(config) # Analysis uses analysis model
                logging.info(f"Calling Gemini analyze with model: {model_to_use}")
                analysis_obj = _gemini_analyze(transcript, master_prompt, model_to_use)
                # Add transcript to the final JSON object if using two-call
                analysis_obj["transcript_full"] = transcript
                logging.info(f"Analysis complete for {file_name}")
            except QuotaExceeded as qe:
                logging.error(f"Quota exceeded during analysis for {file_name}: {qe}")
                raise # Re-raise QuotaExceeded
            except Exception as e:
                logging.error(f"Error during Gemini analysis for {file_name}: {e}", exc_info=True)
                import sheets
                sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Analysis failed: {e}", config, file_name)
                return

        # --- Feature Coverage & Missed Opps ---
        feature_coverage, missed_opps = ("", "")
        try:
            # Use transcript for calculation if available and non-empty
            if transcript:
                feature_coverage, missed_opps = _feature_coverage_and_missed(transcript)
            else:
                # Fallback to values from JSON if transcript is missing (e.g., failed transcription)
                feature_coverage = analysis_obj.get("Feature Checklist Coverage", "")
                missed_opps = analysis_obj.get("Missed Opportunities", "")
        except Exception as e:
            logging.warning(f"Failed to calculate feature coverage for {file_name}: {e}")
            # Use defaults from analysis_obj or empty strings
            feature_coverage = analysis_obj.get("Feature Checklist Coverage", "")
            missed_opps = analysis_obj.get("Missed Opportunities", "")


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

        logging.info(f"SUCCESS: Fully processed {file_name} (date={date_out}, duration={duration_min}m)")

    except QuotaExceeded:
         # Log and re-raise specifically for main loop to catch
         logging.error(f"Quota exceeded while processing {file_name}. Propagating error.")
         raise
    except Exception as e:
         # Catch any other unexpected errors during the process
         logging.error(f"Unexpected critical error processing {file_name}: {e}", exc_info=True)
         try:
             # Attempt to update ledger to Error status for unexpected failures
             import sheets
             sheets.update_ledger(gsheets_sheet, file_id, "Error", f"Critical unexpected error: {e}", config, file_name)
         except Exception as le:
             logging.error(f"Also failed to update ledger for critical error on {file_name}: {le}")
         # Depending on policy, you might want to raise e here to stop the whole run
         # or just return to allow processing of other files. Let's return for now.
         return
    finally:
        # --- Cleanup: Remove downloaded file ---
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logging.info(f"Cleaned up temporary file: {local_path}")
            except Exception as e:
                logging.warning(f"Could not remove temporary file {local_path}: {e}")
