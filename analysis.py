# analysis.py
# Meeting Analysis Bot – FREE Gemini API (Dec 2025)
# Model: gemini-2.5-flash (active & supported)

import os
import io
import re
import json
import logging
import datetime as dt
from typing import Dict, Any, Tuple, List, Set

from tenacity import retry, stop_after_attempt, wait_exponential
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import google.generativeai as genai

# ------------------------------------------------------------------
# ENV & LOGGING
# ------------------------------------------------------------------
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

DEFAULT_MODEL = "gemini-2.5-flash"
ALLOWED_MIME_PREFIXES = ("audio/", "video/")

# ------------------------------------------------------------------
# FEATURE MAPS (Sales Insight Logic)
# ------------------------------------------------------------------
ERP_FEATURES = {
    "Tally import/export": ["tally"],
    "Bank reconciliation": ["reconciliation", "reco"],
    "UPI/cards gateway": ["upi", "payment"],
    "GST/TDS reports": ["gst", "tds"],
    "Defaulter tracking": ["defaulter"],
}

ASP_FEATURES = {
    "Managed accounting": ["managed accounting"],
    "Bookkeeping": ["bookkeeping"],
    "Audit coordination": ["audit"],
    "Dedicated accountant": ["accountant"],
}

# ------------------------------------------------------------------
# UTILITY HELPERS
# ------------------------------------------------------------------
def _is_media_supported(mime_type: str) -> bool:
    return bool(mime_type) and mime_type.startswith(ALLOWED_MIME_PREFIXES)

def _download_drive_file(drive_service, file_id: str, out_path: str) -> str:
    meta = drive_service.files().get(
        fileId=file_id,
        fields="name,mimeType"
    ).execute()

    mime_type = meta.get("mimeType", "")
    request = drive_service.files().get_media(fileId=file_id)

    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return mime_type

def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", text.lower())).strip()

def _feature_coverage(transcript: str) -> Tuple[str, str]:
    norm = _normalize(transcript)
    covered, missed = set(), set()

    for feature, keys in {**ERP_FEATURES, **ASP_FEATURES}.items():
        if any(_normalize(k) in norm for k in keys):
            covered.add(feature)
        else:
            missed.add(feature)

    coverage = f"{len(covered)}/{len(covered) + len(missed)} covered: {', '.join(sorted(covered))}"
    missed_text = "- " + "\n- ".join(sorted(missed)) if missed else "NA"
    return coverage, missed_text

# ------------------------------------------------------------------
# GEMINI TRANSCRIPTION (FREE – NO UPLOAD API)
# ------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
def gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    model = genai.GenerativeModel(model_name)

    with open(file_path, "rb") as f:
        media_bytes = f.read()

    response = model.generate_content([
        "Transcribe this meeting audio verbatim with punctuation. Output plain text only.",
        {
            "mime_type": mime_type,
            "data": media_bytes
        }
    ])

    return response.text or ""

# ------------------------------------------------------------------
# GEMINI ANALYSIS (FREE – JSON OUTPUT)
# ------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
def gemini_analyze(transcript: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    model = genai.GenerativeModel(model_name)

    full_prompt = f"""
{master_prompt}

---
MEETING TRANSCRIPT:
{transcript}
"""

    response = model.generate_content(
        full_prompt,
        generation_config={"temperature": 0.2}
    )

    raw = (response.text or "").strip()

    if raw.startswith("```"):
        raw = raw.replace("```json", "").replace("```", "").strip()

    return json.loads(raw)

# ------------------------------------------------------------------
# MAIN PROCESSOR
# ------------------------------------------------------------------
def process_single_file(drive_service, gsheets_sheet, file_meta, member_name, config):
    file_id = file_meta["id"]
    file_name = file_meta.get("name", file_id)

    tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    safe_name = re.sub(r"[^\w\.-]", "_", file_name)
    local_path = os.path.join(tmp_dir, f"{file_id}_{safe_name}")

    try:
        logging.info(f"Processing file: {file_name}")

        mime_type = _download_drive_file(drive_service, file_id, local_path)
        if not _is_media_supported(mime_type):
            raise ValueError("Unsupported media type")

        transcript = gemini_transcribe(
            local_path,
            mime_type,
            config.get("google_llm", {}).get("model", DEFAULT_MODEL)
        )

        if not transcript.strip():
            raise ValueError("Empty transcript")

        with open("prompt.txt", encoding="utf-8") as f:
            master_prompt = f.read().strip()

        analysis = gemini_analyze(
            transcript,
            master_prompt,
            config.get("google_llm", {}).get("analysis_model", DEFAULT_MODEL)
        )

        coverage, missed = _feature_coverage(transcript)

        analysis["Feature Checklist Coverage"] = coverage
        analysis["Missed Opportunities"] = missed
        analysis["transcript_full"] = transcript
        analysis["Owner (Who handled the meeting)"] = member_name
        analysis["Society Name"] = os.path.splitext(file_name)[0]

        import sheets
        sheets.write_analysis_result(gsheets_sheet, analysis, config)
        sheets.update_ledger(gsheets_sheet, file_id, "Processed", "Success", config, file_name)

        logging.info(f"SUCCESS: {file_name}")

    except Exception as e:
        logging.error(f"FAILED: {file_name} → {e}", exc_info=True)
        import sheets
        sheets.update_ledger(gsheets_sheet, file_id, "Error", str(e)[:200], config, file_name)

    finally:
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass
