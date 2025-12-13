# ===================================================================
# analysis.py — Meeting Analysis Bot
# FREE Gemini API | Full ERP / ASP Intelligence | Dec 2025
# ===================================================================

import os
import io
import re
import json
import logging
from typing import Dict, Any, Tuple, Set

from tenacity import retry, stop_after_attempt, wait_exponential
from googleapiclient.http import MediaIoBaseDownload
import google.generativeai as genai

# -------------------------------------------------------------------
# ENV & LOGGING
# -------------------------------------------------------------------
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

DEFAULT_MODEL = "gemini-2.5-flash"
ALLOWED_MIME_PREFIXES = ("audio/", "video/")

MAX_FILE_MB = 150
MAX_TRANSCRIPT_CHARS = 120_000

# -------------------------------------------------------------------
# FULL ERP FEATURE MAP
# -------------------------------------------------------------------
ERP_FEATURES = {
    "Tally import/export": ["tally", "tally import", "tally export"],
    "E-invoicing": ["e invoice", "einvoice", "e-invoicing"],
    "Bank reconciliation": ["bank reconciliation", "reco", "reconciliation"],
    "Vendor accounting": ["vendor accounting", "vendor ledger"],
    "Budgeting": ["budget", "budgeting"],
    "PO / WO approvals": ["purchase order", "po approval", "work order", "wo approval"],
    "Inventory management": ["inventory", "stock"],
    "Asset tagging via QR": ["asset tagging", "qr code", "asset qr"],
    "Meter reading → auto billing": ["meter reading", "auto billing", "auto invoice"],
    "Maker-checker billing": ["maker checker", "maker-checker"],
    "Late fee & reminders": ["late fee", "penalty", "reminder"],
    "UPI / Cards payment gateway": ["upi", "payment gateway", "cards"],
    "Virtual accounts per unit": ["virtual account", "unit account"],
    "Preventive maintenance": ["preventive maintenance", "pm schedule"],
    "Role-based access": ["role based access", "rbac"],
    "Defaulter tracking": ["defaulter", "arrears"],
    "GST reports": ["gst"],
    "TDS reports": ["tds"],
    "Balance sheet & dashboards": ["balance sheet", "dashboard"],
    "Audit-ready books": ["audit ready", "auditor"],
    "Automated bill generation": ["bill generation", "auto bill"],
    "Collection tracking": ["collection tracking", "payment tracking"],
}

# -------------------------------------------------------------------
# FULL ASP FEATURE MAP
# -------------------------------------------------------------------
ASP_FEATURES = {
    "Managed accounting": ["managed accounting", "outsourced accounting"],
    "Bookkeeping": ["bookkeeping", "day book"],
    "Bank reconciliation + suspense": ["bank reconciliation", "suspense"],
    "Income & expense tracking": ["income", "expense"],
    "Financial reports (P&L, TB)": ["profit and loss", "trial balance", "p&l"],
    "Audit coordination": ["audit coordination", "auditor"],
    "Vendor & PO management": ["vendor management", "purchase order"],
    "Inventory & amenities booking": ["amenities booking", "inventory"],
    "Dedicated accountant": ["dedicated accountant", "remote accountant"],
    "Annual data backup": ["data backup", "backup"],
    "Compliance management": ["compliance", "roc", "statutory"],
    "Finalisation support": ["finalisation", "year end closing"],
    "Society advisory": ["advisory", "consulting"],
}

# -------------------------------------------------------------------
# UTILS
# -------------------------------------------------------------------
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
    text = re.sub(r"[^\w\s]", "", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _feature_coverage(transcript: str) -> Tuple[str, str]:
    norm = _normalize(transcript)
    covered, missed = set(), set()

    combined = {**ERP_FEATURES, **ASP_FEATURES}
    for feature, keywords in combined.items():
        if any(_normalize(k) in norm for k in keywords):
            covered.add(feature)
        else:
            missed.add(feature)

    coverage = f"{len(covered)}/{len(combined)} covered: {', '.join(sorted(covered))}"
    missed_text = "- " + "\n- ".join(sorted(missed)) if missed else "NA"
    return coverage, missed_text


# -------------------------------------------------------------------
# GEMINI — TRANSCRIPTION
# -------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
def gemini_transcribe(file_path: str, mime_type: str, model_name: str) -> str:
    model = genai.GenerativeModel(model_name)

    with open(file_path, "rb") as f:
        media_bytes = f.read()

    response = model.generate_content([
        "Transcribe the meeting verbatim with punctuation. Output plain text only.",
        {
            "mime_type": mime_type,
            "data": media_bytes
        }
    ])

    return response.text or ""


# -------------------------------------------------------------------
# GEMINI — ANALYSIS
# -------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
def gemini_analyze(transcript: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    model = genai.GenerativeModel(model_name)

    prompt = f"""
{master_prompt}

---
MEETING TRANSCRIPT:
{transcript}
"""

    response = model.generate_content(
        prompt,
        generation_config={"temperature": 0.2}
    )

    raw = (response.text or "").strip()
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("Gemini returned invalid JSON")


# -------------------------------------------------------------------
# MAIN ENTRY — SINGLE FILE
# -------------------------------------------------------------------
def process_single_file(drive_service, gsheets_sheet, file_meta, member_name, config):
    file_id = file_meta["id"]
    file_name = file_meta.get("name", file_id)

    file_size = int(file_meta.get("size", 0))
    if file_size > MAX_FILE_MB * 1024 * 1024:
        raise ValueError("File too large for FREE Gemini tier")

    tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    safe_name = re.sub(r"[^\w\.-]", "_", file_name)
    local_path = os.path.join(tmp_dir, f"{file_id}_{safe_name}")

    try:
        logging.info(f"Processing: {file_name}")

        mime_type = _download_drive_file(drive_service, file_id, local_path)
        if not _is_media_supported(mime_type):
            raise ValueError("Unsupported media type")

        model_name = config.get("google_llm", {}).get("model", DEFAULT_MODEL)

        transcript = gemini_transcribe(local_path, mime_type, model_name)
        if not transcript.strip():
            raise ValueError("Empty transcript")

        if len(transcript) > MAX_TRANSCRIPT_CHARS:
            transcript = transcript[:MAX_TRANSCRIPT_CHARS]

        with open("prompt.txt", encoding="utf-8") as f:
            master_prompt = f.read().strip()

        analysis = gemini_analyze(transcript, master_prompt, model_name)

        coverage, missed = _feature_coverage(transcript)

        analysis.update({
            "Feature Checklist Coverage": coverage,
            "Missed Opportunities": missed,
            "transcript_full": transcript,
            "Owner (Who handled the meeting)": member_name,
            "Society Name": os.path.splitext(file_name)[0],
        })

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
