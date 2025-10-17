import os
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
    msg = str(e).lower()
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
DEFAULT_MODEL_NAME = "gemini-2.5-flash-preview-05-20" 

ERP_FEATURES = {
    # Add ERP feature mappings here...
}

ASP_FEATURES = {
    # Add ASP feature mappings here...
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

def _download_drive_file(drive_service, file_id: str, out_path: str) -> Tuple[str, str]:
    meta = drive_service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
    mime_type = meta.get("mimeType", "")
    request = drive_service.files().get_media(fileId=file_id)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return mime_type, out_path

def _get_model(config: Dict[str, Any]) -> str:
    return config.get("google_llm", {}).get("model", "gemini-1.5-flash")

def _load_master_prompt(config: Dict[str, Any]) -> str:
    prompt_path = os.path.join(os.getcwd(), "prompt.txt")
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return config.get("google_llm", {}).get("schema_prompt", "Default schema prompt").strip()

# =========================
# Gemini config & calls
# =========================
@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded, RuntimeError)))
def _gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    try:
        model = genai.GenerativeModel(model_name)
        uploaded = genai.upload_file(path=file_path, mime_type=mime_type)
        prompt = "Transcribe the audio verbatim with punctuation. Do not summarize. Output plain text only."
        resp = model.generate_content([uploaded, {"text": prompt}])
        if getattr(resp, "prompt_feedback", None) and getattr(resp.prompt_feedback, "block_reason", None):
            raise RuntimeError(f"Prompt blocked: {resp.prompt_feedback.block_reason}")
        text = (resp.text or "").strip()
        if not text:
            raise RuntimeError("Empty transcript from Gemini.")
        return text
    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during TRANSCRIBE call.")
            raise QuotaExceeded(str(e))
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
        )
        if getattr(resp, "prompt_feedback", None) and getattr(resp.prompt_feedback, "block_reason", None):
            raise RuntimeError(f"Prompt blocked: {resp.prompt_feedback.block_reason}")
        raw = (resp.text or "").strip().strip("` ").removeprefix("json").lstrip(":").strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("Analysis output is not a JSON object.")
        return data
    except json.JSONDecodeError as je:
        raise RuntimeError(f"Failed to parse JSON from model: {je}") from je
    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during ANALYZE call.")
            raise QuotaExceeded(str(e))
        raise

# =========================
# Entry point
# =========================
def process_single_file(drive_service, gsheets_sheet, file_meta: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    _init_gemini()

    file_id = file_meta["id"]
    meta = _download_drive_file(drive_service, file_id, "/tmp")
    file_name = meta.get("name", "Unknown Filename")
    mime_type = meta.get("mimeType", "")
    created_iso = meta.get("createdTime")

    logging.info(f"[Gemini-only] Processing: {file_name} ({mime_type})")

    # Date (dd/mm/yy)
    date_out = _pick_date_for_output(file_name, created_iso)

    # Download
    tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, f"{file_id}_{file_name}".replace("/", "_"))
    mime_type, _ = _download_drive_file(drive_service, file_id, local_path)

    # Duration (minutes)
    duration_min = _probe_duration_minutes(meta, local_path, mime_type)

    # Prompt
    master_prompt = _load_master_prompt(config)

    # One-shot vs Two-call
    transcript = ""
    if _is_one_shot(config):
        try:
            analysis_obj = _gemini_one_shot(local_path, mime_type, master_prompt, _get_analysis_model(config))
            transcript = analysis_obj.get("transcript_full", "")
            if not transcript:
                logging.error(f"Transcript missing in one-shot response for {file_name}.")
        except Exception as e:
            logging.error(f"Error in one-shot processing for {file_name}: {e}")
            return
    else:
        try:
            transcript = _gemini_transcribe(local_path, mime_type, _get_model(config))
            logging.info(f"Transcript length: {len(transcript)} chars")
        except QuotaExceeded:
            raise
        except Exception as e:
            logging.error(f"Error in transcription for {file_name}: {e}")
            return

        analysis_obj = _gemini_analyze(transcript, master_prompt, _get_analysis_model(config))

    # Ensure that 'transcript' field exists
    if not transcript:
        logging.error(f"Empty transcript for {file_name}.")
        return

    # Further processing...
    feature_coverage, missed_opps = ("", "")
    try:
        if transcript:
            feature_coverage, missed_opps = _feature_coverage_and_missed(transcript)
    except Exception:
        pass

    analysis_obj.setdefault("Date", date_out)
    analysis_obj.setdefault("Meeting duration (min)", duration_min)
    analysis_obj.setdefault("Feature Checklist Coverage", feature_coverage or "")
    analysis_obj.setdefault("Missed Opportunities", missed_opps or "")

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

    logging.info(f"SUCCESS: Processed {file_name} (date={date_out}, duration={duration_min}m)")

