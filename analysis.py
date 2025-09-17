import os
import json
import logging
import re
import shlex
import subprocess
from typing import Dict, Any

import sheets
from faster_whisper import WhisperModel

try:
    import google.generativeai as genai
except Exception:
    genai = None

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Default prompt fallback ----------
DEFAULT_PROMPT = """
### ROLE
You are a senior Sales Conversation Analyst for society-management software (ERP + ASP).

### TASK
1) Read the transcript and fill **all** fields in the schema below.
2) If any info is missing, return "N/A".
3) Output **only** valid JSON (no prose/markdown).
4) Keep numeric scores as plain numbers inside strings (e.g., "7", "83.3%").

### SCHEMA (exact keys; 47 fields)
{
"Date": "", "POC Name": "", "Society Name": "", "Visit Type": "", "Meeting Type": "",
"Amount Value": "", "Months": "", "Deal Status": "", "Vendor Leads": "", "Society Leads": "",
"Opening Pitch Score": "", "Product Pitch Score": "", "Cross-Sell / Opportunity Handling": "",
"Closing Effectiveness": "", "Negotiation Strength": "", "Rebuttal Handling": "",
"Overall Sentiment": "", "Total Score": "", "% Score": "", "Risks / Unresolved Issues": "",
"Improvements Needed": "", "Owner (Who handled the meeting)": "", "Email Id": "", "Kibana ID": "",
"Manager": "", "Product Pitch": "", "Team": "", "Media Link": "", "Doc Link": "",
"Suggestions & Missed Topics": "", "Pre-meeting brief": "", "Meeting duration (min)": "",
"Rapport Building": "", "Improvement Areas": "", "Product Knowledge Displayed": "",
"Call Effectiveness and Control": "", "Next Step Clarity and Commitment": "",
"Missed Opportunities": "", "Key Discussion Points": "", "Key Questions": "",
"Competition Discussion": "", "Action items": "", "Positive Factors": "", "Negative Factors": "",
"Customer Needs": "", "Overall Client Sentiment": "", "Feature Checklist Coverage": "", "Manager Email": ""
}

### CONTEXT
- ERP: ₹12 + 18% GST / flat / month. Key: Tally import/export, in-house gateway, 350+ bill combos, maker-checker, reminders, e-invoicing, bank rec, inventory, QR assets, PPM, dashboards.
- ASP: ₹22.5 + 18% GST / flat / month. Key: managed accounting (billing/receipt), bookkeeping, bank rec + suspense follow-up, non-audited reports, finalisation support, audit coordination, vendor/PO/inventory, amenities booking, dedicated remote accountant.

### TRANSCRIPT
{transcript}
"""

class _SafeDict(dict):
    def __missing__(self, key):
        return ""

def _load_prompt(config: Dict[str, Any]) -> str:
    path = (
        (config.get("analysis", {}) or {}).get("prompt_path")
        or os.environ.get("PROMPT_PATH")
        or "prompt.txt"
    )
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                prompt = f.read()
                logging.info(f"Loaded prompt template from {path}")
                return prompt
        logging.warning(f"Prompt file not found at '{path}'. Using built-in default prompt.")
        return DEFAULT_PROMPT
    except Exception as e:
        logging.warning(f"Failed to read prompt at '{path}': {e}. Using built-in default prompt.")
        return DEFAULT_PROMPT

def _normalize_audio(in_path: str) -> str:
    """Convert to 16k mono WAV with ffmpeg for better ASR stability."""
    try:
        base = os.path.basename(in_path)
        safe = re.sub(r"[^\w\-.]+", "_", base)
        out = os.path.join("/tmp", f"{safe}_16k.wav")
        if os.path.exists(out):
            return out
        cmd = f'ffmpeg -y -i {shlex.quote(in_path)} -ac 1 -ar 16000 -vn {shlex.quote(out)}'
        subprocess.run(shlex.split(cmd), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info(f"Audio normalized with ffmpeg → {out}")
        return out
    except Exception as e:
        logging.warning(f"ffmpeg normalization failed: {e}. Using original audio.")
        return in_path

def transcribe_audio(file_path: str, config: dict) -> str:
    """Transcribe with faster-whisper. Keep args minimal for compatibility."""
    model_name = config.get("analysis", {}).get("whisper_model", "small")
    device = config.get("analysis", {}).get("whisper_device", "cpu")
    audio_path = _normalize_audio(file_path)

    try:
        logging.info(f"Loading faster-whisper: {model_name} on {device}")
        model = WhisperModel(model_name, device=device)
        segments, _info = model.transcribe(audio_path, beam_size=5)
        text = " ".join([s.text.strip() for s in segments if s.text.strip()])
        logging.info(f"SUCCESS: Transcribed {len(text.split())} words.")
        return text
    except TypeError:
        # Fallback for versions not supporting beam_size
        try:
            segments, _info = model.transcribe(audio_path)
            text = " ".join([s.text.strip() for s in segments if s.text.strip()])
            logging.info(f"SUCCESS (fallback): Transcribed {len(text.split())} words.")
            return text
        except Exception as e2:
            logging.error(f"Transcription fallback failed: {e2}")
            return ""
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return ""

def _rule_based_extract(transcript: str, owner: str) -> Dict[str, str]:
    out = {h: "N/A" for h in sheets.DEFAULT_HEADERS}
    out["Owner (Who handled the meeting)"] = owner or "N/A"
    out["Media Link"] = "N/A"
    out["Doc Link"] = "N/A"

    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", transcript)
    if emails:
        out["Email Id"] = emails[0]

    amt = re.search(r"(?:₹|INR|Rs\.?|Rs )\s*([0-9\.,]+)", transcript, flags=re.IGNORECASE)
    if amt:
        raw = amt.group(1).replace(",", "")
        try:
            out["Amount Value"] = str(int(float(raw)))
        except Exception:
            out["Amount Value"] = amt.group(1)

    dur = re.search(r"(\d{1,3})\s*(?:minutes|min|mins|m)\b", transcript, flags=re.IGNORECASE)
    if dur:
        out["Meeting duration (min)"] = dur.group(1)

    low = transcript.lower()
    if any(w in low for w in ["happy", "interested", "positive", "good", "great", "excellent"]):
        out["Overall Sentiment"] = "Positive"
    elif any(w in low for w in ["angry", "upset", "negative", "concern", "issue"]):
        out["Overall Sentiment"] = "Negative"
    else:
        out["Overall Sentiment"] = "Neutral"

    if "erp" in low and "asp" in low:
        out["Meeting Type"] = "ERP & ASP"
        out["Product Pitch"] = "ERP & ASP"
    elif "erp" in low:
        out["Meeting Type"] = "ERP Pitch"
        out["Product Pitch"] = "ERP"
    elif "asp" in low or "accounting service" in low:
        out["Meeting Type"] = "ASP Pitch"
        out["Product Pitch"] = "ASP"
    else:
        out["Meeting Type"] = "General Inquiry"

    pct = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%", transcript)
    if pct:
        out["% Score"] = f"{pct.group(1)}%"

    sentences = re.split(r'(?<=[.!?])\s+', transcript.strip())
    if sentences:
        out["Key Discussion Points"] = " ".join(sentences[:3])[:1500]

    return out

def _llm_extract(transcript: str, owner_name: str, config: Dict[str, Any]) -> Dict[str, str]:
    template = _load_prompt(config)
    prompt = template.format_map(_SafeDict(transcript=transcript, owner_name=owner_name))

    # Gemini first
    if genai and os.environ.get("GEMINI_API_KEY"):
        try:
            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            model_name = config.get("analysis", {}).get("gemini_model", "gemini-1.5-flash")
            response = genai.GenerativeModel(model_name).generate_content(prompt)
            raw = (response.text or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.lower().startswith("json"):
                    raw = raw[4:].strip()
            parsed = json.loads(raw)
            logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
            return {k: (str(parsed.get(k)) if parsed.get(k) is not None else "N/A") for k in sheets.DEFAULT_HEADERS}
        except Exception as e:
            logging.error(f"Gemini failed: {e}")

    # OpenRouter/OpenAI fallback
    openrouter_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if openrouter_key:
        try:
            import openai
            openai.api_key = openrouter_key
            model_name = config.get("analysis", {}).get("openrouter_model_name", "gpt-4o-mini")
            resp = openai.ChatCompletion.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp["choices"][0]["message"]["content"].strip()
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.lower().startswith("json"):
                    raw = raw[4:].strip()
            parsed = json.loads(raw)
            logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter/OpenAI).")
            return {k: (str(parsed.get(k)) if parsed.get(k) is not None else "N/A") for k in sheets.DEFAULT_HEADERS}
        except Exception as e:
            logging.error(f"OpenRouter/OpenAI failed: {e}")

    return {}

def analyze_transcript(transcript: str, config: Dict[str, Any], owner_name: str) -> Dict[str, str]:
    if not transcript or not transcript.strip():
        return {}
    data = _llm_extract(transcript, owner_name, config)
    if not data:
        data = _rule_based_extract(transcript, owner_name)
    clean = {k: (str(data.get(k)) if data.get(k) is not None else "N/A") for k in sheets.DEFAULT_HEADERS}
    clean["Owner (Who handled the meeting)"] = owner_name or clean.get("Owner (Who handled the meeting)", "N/A")
    return clean

def process_single_file(drive_service, gsheets_sheet, file_meta, member_name: str, config: dict):
    from gdrive import download_file

    file_id = file_meta.get("id")
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        transcript = transcribe_audio(local_path, config)
        if not transcript:
            raise ValueError("Empty transcript")

        analysis_row = analyze_transcript(transcript, config, member_name)
        if not analysis_row:
            raise ValueError("Empty analysis result")

        # Enrich with mapping (Manager/Team/Email)
        mm = config.get("manager_map", {}).get(member_name, {})
        if mm:
            analysis_row["Manager"] = analysis_row.get("Manager") or mm.get("Manager") or "N/A"
            analysis_row["Team"] = analysis_row.get("Team") or mm.get("Team") or "N/A"
            analysis_row["Manager Email"] = analysis_row.get("Manager Email") or mm.get("Email") or "N/A"

        analysis_row["Media Link"] = file_name

        sheets.write_analysis_result(gsheets_sheet, analysis_row, config)
        sheets.update_ledger(gsheets_sheet, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        try:
            sheets.update_ledger(gsheets_sheet, file_id, "Failed", str(e), config, file_name)
        except Exception as e2:
            logging.error(f"ERROR writing failure to ledger: {e2}")
        raise
