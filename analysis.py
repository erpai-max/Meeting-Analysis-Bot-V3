import logging
import re
import os
import json
import sys
from typing import Dict, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
from faster_whisper import WhisperModel
from openai import OpenAI

# Import utility modules
import gdrive
import sheets

# =======================
# PII Redaction
# =======================
def redact_pii(transcript: str) -> str:
    """Basic redaction of emails and phone numbers."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    phone_pattern = r'(\+\d{1,2}\s?)?(\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4}'
    
    redacted_transcript = re.sub(email_pattern, '[EMAIL_REDACTED]', transcript)
    redacted_transcript = re.sub(phone_pattern, '[PHONE_REDACTED]', redacted_transcript)
    return redacted_transcript

# =======================
# Transcription
# =======================
def transcribe_audio(file_content: 'io.BytesIO', original_filename: str, config: Dict) -> Optional[Tuple[str, float]]:
    """Transcribes audio and returns transcript and duration."""
    import tempfile 
    logging.info("Starting transcription process...")
    whisper_model = config['analysis']['whisper_model']
    
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(original_filename)[1], delete=False) as temp_file:
        temp_file.write(file_content.read())
        temp_file_path = temp_file.name
    try:
        model = WhisperModel(whisper_model, device="cpu", compute_type="int8")
        logging.info(f"Transcribing {temp_file_path} with model '{whisper_model}'...")
        segments, info = model.transcribe(temp_file_path, beam_size=5)
        transcript = " ".join(segment.text for segment in segments).strip()
        duration_seconds = info.duration
        logging.info(f"SUCCESS: Transcription completed. Duration: {duration_seconds:.2f}s. Length: {len(transcript)} chars.")
        return transcript, duration_seconds
    except Exception as e:
        logging.error(f"ERROR: Transcription failed: {e}")
        return None, 0.0
    finally:
        os.remove(temp_file_path)
        logging.info(f"Cleaned up temporary file: {temp_file_path}")

# =======================
# AI Analysis and Data Processing (Combined for Robustness)
# =======================
SIX_CORE = [
    "Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness", "Negotiation Strength", "Rebuttal Handling",
]

def _to_num(s):
    try:
        return float(str(s).strip())
    except (ValueError, TypeError):
        return None

def _normalize_and_enrich(raw_rec: Dict, member_name: str, file: Dict, duration_seconds: float, config: Dict) -> Dict:
    """
    This is the definitive fix. It takes the raw AI output and performs all cleaning,
    normalization, and enrichment in one safe function.
    """
    if not raw_rec:
        return {}

    # Step 1: Aggressively clean keys to prevent KeyErrors
    cleaned_rec = {str(k).strip().strip('"'): v for k, v in raw_rec.items()}

    # Step 2: Recompute scores for consistency
    nums = [_to_num(cleaned_rec.get(k, "")) for k in SIX_CORE]
    valid_nums = [n for n in nums if n is not None]
    if len(valid_nums) > 0:
        avg = sum(valid_nums) / len(valid_nums)
        cleaned_rec["Total Score"] = str(Decimal(avg).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))
        cleaned_rec["% Score"] = str(int(round(avg * 10)))
    else:
        cleaned_rec["Total Score"] = ""
        cleaned_rec["% Score"] = ""

    # Step 3: Coerce enums
    for k in ["Overall Sentiment", "Overall Client Sentiment"]:
        if str(cleaned_rec.get(k, "")).strip() not in ["Positive", "Neutral", "Negative"]:
            cleaned_rec[k] = ""
    
    # Step 4: Enrich with context from folders and filename
    file_name = file.get('name', '')
    manager_map = config.get('manager_map', {})
    manager_emails = config.get('manager_emails', {})

    cleaned_rec['Owner'] = member_name
    manager_info = manager_map.get(member_name, {})
    manager_name = manager_info.get('Manager', '')
    cleaned_rec['Manager'] = manager_name
    cleaned_rec['Team'] = manager_info.get('Team', '')
    cleaned_rec['Email Id'] = manager_info.get('Email', '')
    cleaned_rec['Manager Email'] = manager_emails.get(manager_name, '')
    cleaned_rec['Media Link'] = file.get('webViewLink', '')
    if duration_seconds and not cleaned_rec.get('Meeting duration (min)'):
        cleaned_rec['Meeting duration (min)'] = f"{duration_seconds / 60:.2f}"

    # Step 5: Enrich with data from filename if AI result is empty
    if not str(cleaned_rec.get('Kibana ID', '')).strip():
        kibana_match = re.search(r'(8[a-f0-9]{31})', file_name, re.IGNORECASE)
        if kibana_match: cleaned_rec['Kibana ID'] = kibana_match.group(1)
    if not str(cleaned_rec.get('Date', '')).strip():
        date_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})', file_name)
        if date_match: cleaned_rec['Date'] = date_match.group(0).replace('-', '/')
    if not str(cleaned_rec.get('Meeting Type', '')).strip():
        fn_lower = file_name.lower()
        if 'fresh' in fn_lower: cleaned_rec['Meeting Type'] = 'Fresh'
        elif 'followup' in fn_lower: cleaned_rec['Meeting Type'] = 'Followup'
        elif 'closure' in fn_lower: cleaned_rec['Meeting Type'] = 'Closure'
        elif 'renewal' in fn_lower: cleaned_rec['Meeting Type'] = 'Renewal'
    if not str(cleaned_rec.get('Society Name', '')).strip():
        name_part = re.split(r'[_|\- ]+(ASP|ERP|DEMO|FRESH|\d{2}[-/]\d{2})', file_name, flags=re.IGNORECASE)
        if name_part and name_part[0]: cleaned_rec['Society Name'] = name_part[0].strip().replace('_', ' ').replace('.', ' ')

    # Step 6: Final type conversion to string for Sheets
    for k, v in cleaned_rec.items():
        if isinstance(v, (int, float, bool)) or v is None:
            cleaned_rec[k] = str(v)
            
    return cleaned_rec

def analyze_with_openrouter(prompt: str, config: Dict) -> Optional[Dict]:
    """Fallback analysis function using OpenRouter."""
    logging.warning("Attempting failover to OpenRouter...")
    # ... [Implementation remains the same]
    return None # Placeholder

def analyze_transcript(transcript: str, owner_name: str, config: Dict):
    """Analyzes transcript with Gemini and fails over to OpenRouter on quota errors."""
    logging.info("Starting analysis with Gemini...")
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logging.error("CRITICAL: GEMINI_API_KEY environment variable not found.")
        return None
    genai.configure(api_key=gemini_key)
    prompt_template = config.get('gemini_prompt', '')
    if not prompt_template:
        logging.error("CRITICAL: gemini_prompt not found in config.yaml.")
        return None
    prompt = prompt_template.format(owner_name=owner_name, transcript=transcript)
    
    try:
        model = genai.GenerativeModel(config['analysis']['gemini_model'])
        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        clean_json_string = response.text.replace('```json', '').replace('```', '').strip()
        raw_json = json.loads(clean_json_string)
        logging.info("SUCCESS: Analysis with Gemini complete.")
        return raw_json
    except google_exceptions.ResourceExhausted as e:
        logging.warning(f"Gemini API quota exceeded: {e}. Attempting failover to OpenRouter.")
        return analyze_with_openrouter(prompt, config)
    except Exception as e:
        logging.error(f"ERROR: Primary Gemini API analysis failed: {e}")
        return analyze_with_openrouter(prompt, config)

# =======================
# Main Processing Function
# =======================
def process_single_file(drive_service, gsheets_client, file_meta: Dict, member_name: str, config: Dict):
    """Orchestrates the processing of a single media file."""
    
    file_id = file_meta.get("id")
    file_name = file_meta.get("name", "Unknown Filename")

    file_content = gdrive.download_file(drive_service, file_id)

    transcript, duration_sec = None, 0.0
    transcribe_result = transcribe_audio(file_content, file_name, config)
    if transcribe_result:
        transcript, duration_sec = transcribe_result

    if not transcript:
        raise ValueError("Transcription failed or produced an empty transcript.")

    if config['analysis'].get('redact_pii', False):
        logging.info("PII redaction is enabled. Redacting transcript...")
        transcript = redact_pii(transcript)

    raw_analysis_data = analyze_transcript(transcript, member_name, config)

    if raw_analysis_data == "RATE_LIMIT_EXCEEDED":
        logging.warning("API quota exceeded on primary and fallback. Stopping workflow.")
        sys.exit(0)

    if not raw_analysis_data:
        raise ValueError("AI analysis failed on primary and fallback providers.")

    # All cleaning and enrichment happens in this one robust function
    final_data = _normalize_and_enrich(raw_analysis_data, member_name, file_meta, duration_sec, config)
    
    sheets.write_results(gsheets_client, final_data, config)
    sheets.stream_to_bigquery(final_data, config)

