import os
import re
import json
import math
import logging
from typing import Dict, Any, Tuple
from faster_whisper import WhisperModel
import google.generativeai as genai

import sheets  # for DEFAULT_HEADERS reading via Sheet (we only need header names from the sheet itself)

# -----------------------
# Helpers: JSON extraction & coercion
# -----------------------

EXACT_HEADERS = [
    "Date","POC Name","Society Name","Visit Type","Meeting Type","Amount Value","Months","Deal Status",
    "Vendor Leads","Society Leads","Opening Pitch Score","Product Pitch Score","Cross-Sell / Opportunity Handling",
    "Closing Effectiveness","Negotiation Strength","Rebuttal Handling","Overall Sentiment","Total Score","% Score",
    "Risks / Unresolved Issues","Improvements Needed","Owner (Who handled the meeting)","Email Id","Kibana ID",
    "Manager","Product Pitch","Team","Media Link","Doc Link","Suggestions & Missed Topics","Pre-meeting brief",
    "Meeting duration (min)","Rapport Building","Improvement Areas","Product Knowledge Displayed",
    "Call Effectiveness and Control","Next Step Clarity and Commitment","Missed Opportunities","Key Discussion Points",
    "Key Questions","Competition Discussion","Action items","Positive Factors","Negative Factors","Customer Needs",
    "Overall Client Sentiment","Feature Checklist Coverage","Manager Email","File Name","File ID"
]

# Accept common variants from the LLM and map to exact sheet keys
ALIAS_MAP = {
    "owner": "Owner (Who handled the meeting)",
    "owner_name": "Owner (Who handled the meeting)",
    "meeting_type": "Meeting Type",
    "visit_type": "Visit Type",
    "poc": "POC Name",
    "poc_name": "POC Name",
    "society": "Society Name",
    "society_name": "Society Name",
    "amount": "Amount Value",
    "amount_value": "Amount Value",
    "months_count": "Months",
    "deal_status": "Deal Status",
    "vendor_leads": "Vendor Leads",
    "society_leads": "Society Leads",
    "opening_pitch_score": "Opening Pitch Score",
    "product_pitch_score": "Product Pitch Score",
    "cross_sell": "Cross-Sell / Opportunity Handling",
    "cross_sell_opportunity_handling": "Cross-Sell / Opportunity Handling",
    "closing_effectiveness": "Closing Effectiveness",
    "negotiation_strength": "Negotiation Strength",
    "rebuttal_handling": "Rebuttal Handling",
    "overall_sentiment": "Overall Sentiment",
    "total_score": "Total Score",
    "percent_score": "% Score",
    "%score": "% Score",
    "risks": "Risks / Unresolved Issues",
    "risks_unresolved_issues": "Risks / Unresolved Issues",
    "improvements_needed": "Improvements Needed",
    "email": "Email Id",
    "email_id": "Email Id",
    "kibana": "Kibana ID",
    "kibana_id": "Kibana ID",
    "manager": "Manager",
    "product_pitch": "Product Pitch",
    "team": "Team",
    "media_link": "Media Link",
    "doc_link": "Doc Link",
    "suggestions_missed_topics": "Suggestions & Missed Topics",
    "premeting_brief": "Pre-meeting brief",
    "pre_meeting_brief": "Pre-meeting brief",
    "meeting_duration_min": "Meeting duration (min)",
    "meeting_duration": "Meeting duration (min)",
    "rapport_building": "Rapport Building",
    "improvement_areas": "Improvement Areas",
    "product_knowledge_displayed": "Product Knowledge Displayed",
    "call_effectiveness_control": "Call Effectiveness and Control",
    "next_step_clarity_commitment": "Next Step Clarity and Commitment",
    "missed_opportunities": "Missed Opportunities",
    "key_discussion_points": "Key Discussion Points",
    "key_questions": "Key Questions",
    "competition_discussion": "Competition Discussion",
    "action_items": "Action items",
    "positive_factors": "Positive Factors",
    "negative_factors": "Negative Factors",
    "customer_needs": "Customer Needs",
    "overall_client_sentiment": "Overall Client Sentiment",
    "feature_checklist_coverage": "Feature Checklist Coverage",
    "manager_email": "Manager Email",
    "file_name": "File Name",
    "file_id": "File ID",
    "date": "Date"
}

SCORE_KEYS = [
    "Opening Pitch Score",
    "Product Pitch Score",
    "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness",
    "Negotiation Strength",
]

def _safe_json_from_text(text: str) -> Dict[str, Any]:
    """
    Extract the first {...} block and parse as JSON.
    Cleans code fences and trailing commas.
    """
    if not text:
        return {}

    # Strip code fences if any
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:].strip()

    # Find first JSON object by brace counting
    start = t.find("{")
    if start == -1:
        return {}

    depth = 0
    end = -1
    for i, ch in enumerate(t[start:], start=start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end == -1:
        return {}

    candidate = t[start:end+1]

    # Remove trailing commas before closing braces/brackets
    candidate = re.sub(r",\s*([\]}])", r"\1", candidate)

    try:
        return json.loads(candidate)
    except Exception:
        # last attempt: replace single quotes with double quotes if it looks like JSON-ish
        try:
            candidate2 = candidate.replace("'", '"')
            return json.loads(candidate2)
        except Exception:
            logging.error("Could not parse LLM JSON.")
            return {}

def _coerce_to_exact_headers(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Map alias keys to the exact sheet headers and ensure all headers exist.
    """
    mapped: Dict[str, str] = {}
    # Map aliases (case-insensitive)
    for k, v in (data or {}).items():
        if k in EXACT_HEADERS:
            mapped[k] = str(v if v not in (None, "") else "N/A")
            continue
        low = k.strip().lower()
        if low in ALIAS_MAP:
            mapped[ALIAS_MAP[low]] = str(v if v not in (None, "") else "N/A")
        else:
            # try remove spaces/underscores for fuzzy match
            norm = low.replace(" ", "").replace("_", "")
            hit = None
            for ek in EXACT_HEADERS:
                if norm == ek.lower().replace(" ", "").replace("_", ""):
                    hit = ek
                    break
            if hit:
                mapped[hit] = str(v if v not in (None, "") else "N/A")

    # Fill missing with N/A
    for h in EXACT_HEADERS:
        if h not in mapped:
            mapped[h] = "N/A"
    return mapped

def _to_int_or_none(s: str) -> int | None:
    try:
        n = int(str(s).strip())
        return n
    except Exception:
        # sometimes "7/10" or "7.0"
        try:
            f = float(str(s).strip().replace("%",""))
            return int(round(f))
        except Exception:
            return None

def _compute_totals(mapped: Dict[str, str]) -> None:
    # compute Total Score and % Score if possible
    ints = []
    for k in SCORE_KEYS:
        val = _to_int_or_none(mapped.get(k, ""))
        if val is not None:
            ints.append(max(0, min(10, val)))
    if len(ints) == 5:
        total = sum(ints)
        mapped["Total Score"] = str(total)
        mapped["% Score"] = f"{(total/50.0)*100:.1f}%"
    else:
        # if model provided already, keep it; otherwise N/A
        if not mapped.get("Total Score") or mapped["Total Score"] == "N/A":
            mapped["Total Score"] = "N/A"
        if not mapped.get("% Score") or mapped["% Score"] == "N/A":
            mapped["% Score"] = "N/A"

DATE_PAT = re.compile(
    r"(?P<d>\b\d{1,2})[-_/\.](?P<m>\d{1,2})[-_/\.](?P<y>\d{2,4})\b"
)

def _date_from_filename(file_name: str) -> str:
    """
    Extract date like 31-08-25 or 2025_09_04 from file name and return ISO YYYY-MM-DD.
    """
    if not file_name:
        return "N/A"
    m = DATE_PAT.search(file_name)
    if not m:
        return "N/A"
    d = int(m.group("d"))
    mo = int(m.group("m"))
    y = m.group("y")
    if len(y) == 2:
        y = "20" + y
    try:
        import datetime
        dt = datetime.date(int(y), mo, d)
        return dt.isoformat()
    except Exception:
        return "N/A"

# -----------------------
# Transcription
# -----------------------

def transcribe_audio(file_path: str, config: dict) -> Tuple[str, float]:
    """
    Transcribes audio using Faster-Whisper.
    Returns (transcript_text, duration_minutes).
    """
    model_size = config["analysis"].get("whisper_model", "small")
    device = "cpu"
    logging.info(f"Loading faster-whisper: {model_size} on {device}")

    try:
        model = WhisperModel(model_size, device=device, compute_type="auto")
        # Small beam + translate→English for Hindi/Gujarati/etc.
        segments, info = model.transcribe(
            file_path,
            task="translate",         # translate non-English → English
            vad_filter=True,          # trim long silences
            beam_size=3
        )
        logging.info("Processing audio with Faster-Whisper (task=translate, beam_size=3)")
        logging.info(f"Processing audio with duration {info.duration:.3f}s")

        words = []
        last_end = 0.0
        for seg in segments:
            words.append(seg.text.strip())
            last_end = max(last_end, seg.end)

        transcript = " ".join(w for w in words if w)
        duration_min = (last_end or info.duration) / 60.0
        return transcript.strip(), float(f"{duration_min:.2f}")
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return "", 0.0

# -----------------------
# Prompt Loader
# -----------------------

def _load_prompt(owner_name: str, transcript: str) -> str:
    """
    Loads prompt.txt if present; otherwise uses a strong inline prompt.
    Injects {owner_name} and {transcript}.
    """
    prompt_path = "prompt.txt"
    base = None
    if os.path.exists(prompt_path):
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                base = f.read()
        except Exception:
            base = None

    if not base:
        # Minimal but strict fallback prompt
        base = (
            "You are a sales meeting analyst. Output a single VALID JSON object using exactly these keys "
            f"in this order:\n{json.dumps(EXACT_HEADERS)}\n"
            "All values must be strings. Use 'N/A' when not present. Use short bullet lines for lists."
            "\n\nTRANSCRIPT:\n{transcript}"
        )

    return base.format(owner_name=owner_name, transcript=transcript)

# -----------------------
# AI Analysis
# -----------------------

def _call_gemini(prompt: str, config: dict) -> str:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    model_name = config["analysis"].get("gemini_model", "gemini-1.5-flash")
    model = genai.GenerativeModel(model_name)
    resp = model.generate_content(prompt)
    return (resp.text or "").strip()

def _call_openrouter(prompt: str, config: dict) -> str:
    import openai
    openai.api_key = os.environ.get("OPENROUTER_API_KEY")
    model_name = config["analysis"].get("openrouter_model_name", "openrouter/auto")
    # OpenRouter now uses the Chat Completions v1 compatible API
    response = openai.ChatCompletion.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return response["choices"][0]["message"]["content"].strip()

def analyze_transcript(transcript: str, owner_name: str, file_name: str, config: dict) -> Dict[str, str]:
    """
    Calls LLM(s) to analyze transcript and return a normalized dict for the sheet.
    Robust against formatting variations. Fills derived fields when missing.
    """
    if not transcript.strip():
        return {}

    prompt = _load_prompt(owner_name, transcript)

    text = ""
    # Try Gemini then OpenRouter
    try:
        text = _call_gemini(prompt, config)
        if not text:
            raise RuntimeError("Empty Gemini response")
        logging.info("SUCCESS: LLM response received (Gemini).")
    except Exception as e:
        logging.error(f"Gemini failed: {e}")
        try:
            text = _call_openrouter(prompt, config)
            logging.info("SUCCESS: LLM response received (OpenRouter).")
        except Exception as e2:
            logging.error(f"OpenRouter failed: {e2}")
            return {}

    data = _safe_json_from_text(text)
    mapped = _coerce_to_exact_headers(data)

    # Fill derived fields (if still N/A)
    if mapped.get("Date", "N/A") == "N/A":
        mapped["Date"] = _date_from_filename(file_name)

    _compute_totals(mapped)

    return mapped

# -----------------------
# Main File Processor
# -----------------------

def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    """
    download → transcribe → analyze → save to Sheets.
    """
    from gdrive import download_file, move_file

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # 1) Transcribe
        transcript, duration_min = transcribe_audio(local_path, config)

        # 2) Analyze
        analysis_data = analyze_transcript(transcript, member_name, file_name, config)
        if not analysis_data:
            raise ValueError("Empty analysis result")

        # 3) Inject metadata & manager mapping
        #    - Manager/Team/Manager Email from config.manager_map
        mm = config.get("manager_map", {}).get(member_name, {})
        manager = mm.get("Manager", "N/A")
        team = mm.get("Team", "N/A")
        mgr_email = config.get("manager_emails", {}).get(manager, "N/A")

        # Duration if missing
        if analysis_data.get("Meeting duration (min)", "N/A") == "N/A" and duration_min:
            analysis_data["Meeting duration (min)"] = str(int(round(duration_min)))

        analysis_data["Owner (Who handled the meeting)"] = member_name or "N/A"
        analysis_data["Manager"] = analysis_data.get("Manager", "N/A") if analysis_data.get("Manager", "N/A") != "N/A" else manager
        analysis_data["Team"] = analysis_data.get("Team", "N/A") if analysis_data.get("Team", "N/A") != "N/A" else team
        analysis_data["Manager Email"] = analysis_data.get("Manager Email", "N/A") if analysis_data.get("Manager Email", "N/A") != "N/A" else mgr_email
        analysis_data["Media Link"] = analysis_data.get("Media Link", "N/A") if analysis_data.get("Media Link", "N/A") != "N/A" else file_name
        analysis_data["File Name"] = file_name
        analysis_data["File ID"] = file_id

        # 4) Write to Google Sheets
        sheets.write_analysis_result(gsheets_client, analysis_data, config)

        # 5) Ledger
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        # Quarantine
        try:
            from gdrive import quarantine_file
            quarantine_file(drive_service, file_id, file_meta.get("parents", [""])[0], str(e), config)
        except Exception as qe:
            logging.warning(f"Could not quarantine file {file_id}: {qe}")
        # Ledger
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        raise
