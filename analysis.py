import os
import io
import re
import json
import math
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
# Constants & Feature Maps
# =========================
ALLOWED_MIME_PREFIXES = ("audio/", "video/")

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
    mins = int(round(ms / 60000.0))
    return f"{mins}"

def _probe_duration_minutes(meta: Dict[str, Any], local_path: str, mime_type: str) -> str:
    vmeta = meta.get("videoMediaMetadata") or {}
    dur_ms = vmeta.get("durationMillis")
    if isinstance(dur_ms, (int, float)) and dur_ms > 0:
        return _millis_to_minutes(int(dur_ms))
    if mime_type.startswith("audio/"):
        try:
            if local_path.lower().endswith(".wav"):
                import wave
                with wave.open(local_path, "rb") as w:
                    frames = w.getnframes()
                    rate = w.getframerate()
                    seconds = frames / float(rate)
                    return f"{int(round(seconds / 60.0))}"
        except Exception:
            pass
    return "NA"

# ---------- ERP/ASP coverage & missed-opps ----------
def _normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()

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
    missed_sorted = sorted(missed_all, key=lambda x: (0 if x in priority else 1, priority.index(x) if x in priority else 999, x))
    missed_text = ", ".join(missed_sorted) if missed_sorted else ""
    return feature_coverage_summary, missed_text

# ---------- Gemini calls ----------
def _get_model(config: Dict[str, Any]) -> str:
    return config.get("google_llm", {}).get("model", "gemini-1.5-flash")

def _get_analysis_model(config: Dict[str, Any]) -> str:
    return config.get("google_llm", {}).get("analysis_model", _get_model(config))

def _load_master_prompt(config: Dict[str, Any]) -> str:
    prompt_path = os.path.join(os.getcwd(), "prompt.txt")
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return config.get("google_llm", {}).get("schema_prompt", """
Act as an expert business analyst for society-management ERP & ASP meetings.
Strictly return a single JSON object (no prose outside JSON).
If a field is unknown, set it to "" (empty string).
""").strip()

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    retry=retry_if_exception_type((QuotaExceeded, RuntimeError))
)
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
        msg = str(e).lower()
        if any(k in msg for k in ["quota", "rate limit", "resourceexhausted"]):
            raise QuotaExceeded(msg)
        raise

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    retry=retry_if_exception_type((QuotaExceeded, RuntimeError))
)
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
        msg = str(e).lower()
        if any(k in msg for k in ["quota", "rate limit", "resourceexhausted"]):
            raise QuotaExceeded(msg)
        raise

# ---------- Sheets I/O ----------
def _write_success(gsheets_sheet, file_id: str, file_name: str, date_out: str, duration_min: str,
                   feature_coverage: str, missed_opps: str, analysis_obj: Dict[str, Any], config: Dict[str, Any]):
    import sheets  # local module

    analysis_obj["Date"] = date_out
    analysis_obj["Meeting duration (min)"] = duration_min
    analysis_obj["Feature Checklist Coverage"] = feature_coverage
    analysis_obj["Missed Opportunities"] = missed_opps

    status_note = f"Processed via Gemini; duration={duration_min}m"
    sheets.update_ledger(gsheets_sheet, file_id, "Processed", status_note, config, file_name)

    if hasattr(sheets, "append_result"):
        sheets.append_result(gsheets_sheet, analysis_obj, config)
    elif hasattr(sheets, "append_json"):
        sheets.append_json(gsheets_sheet, analysis_obj, config)
    elif hasattr(sheets, "append_raw"):
        sheets.append_raw(gsheets_sheet, json.dumps(analysis_obj, ensure_ascii=False), config)

# =========================
# Entry point
# =========================
def process_single_file(drive_service, gsheets_sheet, file_meta: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    """
    Orchestrates: metadata -> download -> transcribe -> analyze -> enrich -> write sheets.
    Exceptions are allowed to bubble so main.py can quarantine & stop on quota.
    """
    _init_gemini()

    file_id = file_meta["id"]
    meta = _drive_get_metadata(drive_service, file_id)
    file_name = meta.get("name", file_meta.get("name", "Unknown Filename"))
    mime_type = meta.get("mimeType", file_meta.get("mimeType", ""))
    created_iso = meta.get("createdTime")

    logging.info(f"[Gemini-only] Processing: {file_name} ({mime_type})")

    # Date (dd/mm/yy)
    date_out = _pick_date_for_output(file_name, created_iso)

    # Download
    tmp_dir = config.get("runtime", {}).get("tmp_dir", "/tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, f"{file_id}_{file_name}".replace("/", "_"))
    _, _ = _download_drive_file(drive_service, file_id, local_path)

    # Duration (minutes)
    duration_min = _probe_duration_minutes(meta, local_path, mime_type)

    # Transcribe
    transcript = _gemini_transcribe(local_path, mime_type, _get_model(config))
    logging.info(f"Transcript length: {len(transcript)} chars")

    # Deterministic coverage/missed-opps
    feature_coverage, missed_opps = _feature_coverage_and_missed(transcript)

    # Analyze with your master prompt → JSON
    master_prompt = _load_master_prompt(config)
    analysis_obj = _gemini_analyze(transcript, master_prompt, _get_analysis_model(config))

    # Ensure fields exist
    analysis_obj.setdefault("Date", "")
    analysis_obj.setdefault("Meeting duration (min)", "")
    analysis_obj.setdefault("Feature Checklist Coverage", "")
    analysis_obj.setdefault("Missed Opportunities", "")

    # Write
    _write_success(
        gsheets_sheet=gsheets_sheet,
        file_id=file_id,
        file_name=file_name,
        date_out=date_out,
        duration_min=duration_min,
        feature_coverage=feature_coverage,
        missed_opps=missed_opps,
        analysis_obj=analysis_obj,
        config=config,
    )

    logging.info(f"SUCCESS: Processed {file_name} (date={date_out}, duration={duration_min}m)")
