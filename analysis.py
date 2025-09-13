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
    import tempfile # Import here to keep it self-contained
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
# Data Normalization
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

def normalize_record(rec: Dict):
    """Cleans, validates, and re-computes scores for a raw analysis record."""
    if not rec:
        return {}

    # This is the fix: Aggressively clean keys by stripping spaces, newlines, and quotes
    cleaned_rec = {str(k).strip().strip('"'): v for k, v in rec.items()}

    # Recompute Total and % Score for consistency
    nums = [_to_num(cleaned_rec.get(k, "")) for k in SIX_CORE]
    valid_nums = [n for n in nums if n is not None]
    if len(valid_nums) > 0:
        avg = sum(valid_nums) / len(valid_nums)
        cleaned_rec["Total Score"] = str(Decimal(avg).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))
        cleaned_rec["% Score"] = str(int(round(avg * 10)))
    else:
        cleaned_rec["Total Score"] = ""
        cleaned_rec["% Score"] = ""

    # Coerce sentiment enums
    for k in ["Overall Sentiment", "Overall Client Sentiment"]:
        if str(cleaned_rec.get(k, "")).strip() not in ["Positive", "Neutral", "Negative"]:
            cleaned_rec[k] = ""

    # Ensure all final values are strings
    for k, v in cleaned_rec.items():
        if isinstance(v, (int, float)):
            cleaned_rec[k] = str(v)
            
    return cleaned_rec

# =======================
# AI Analysis with Failover
# =======================
def analyze_with_openrouter(prompt: str, config: Dict) -> Optional[Dict]:
    """Fallback analysis function using OpenRouter."""
    logging.warning("Gemini quota likely exceeded. Failing over to OpenRouter...")
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    if not openrouter_key:
        logging.error("CRITICAL: OPENROUTER_API_KEY not set. Cannot use failover.")
        return None
        
    try:
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_key,
        )
        completion = client.chat.completions.create(
            model=config['analysis']['openrouter_model_name'],
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        raw_json = json.loads(completion.choices[0].message.content)
        logging.info("SUCCESS: Analysis with OpenRouter complete.")
        return raw_json
    except Exception as e:
        logging.error(f"ERROR: OpenRouter analysis failed: {e}")
        return None

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
    
    raw_json = None
    try:
        model = genai.GenerativeModel(config['analysis']['gemini_model'])
        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        clean_json_string = response.text.replace('```json', '').replace('```', '').strip()
        raw_json = json.loads(clean_json_string)
        logging.info("SUCCESS: Analysis with Gemini complete.")
    except google_exceptions.ResourceExhausted as e:
        logging.warning(f"Gemini API quota exceeded: {e}. Attempting failover to OpenRouter.")
        raw_json = analyze_with_openrouter(prompt, config)
    except Exception as e:
        logging.error(f"ERROR: Primary Gemini API analysis failed: {e}")
        logging.info("Attempting failover to OpenRouter due to non-quota error.")
        raw_json = analyze_with_openrouter(prompt, config)

    if not raw_json:
        return None # Both primary and failover failed

    normalized_data = normalize_record(raw_json)
    logging.info("Data normalization complete.")
    return normalized_data

# =======================
# Data Enrichment
# =======================
def enrich_data_from_context(analysis_data: Dict, member_name: str, file: Dict, duration_seconds: float, config: Dict) -> Dict:
    """Fills in missing data from context (filename, folder structure, etc.)."""
    logging.info("Enriching data with context...")
    
    file_name = file.get('name', '')
    manager_map = config.get('manager_map', {})
    manager_emails = config.get('manager_emails', {})

    analysis_data['Owner'] = member_name
    manager_info = manager_map.get(member_name, {})
    manager_name = manager_info.get('Manager', '')
    analysis_data['Manager'] = manager_name
    analysis_data['Team'] = manager_info.get('Team', '')
    analysis_data['Email Id'] = manager_info.get('Email', '')
    analysis_data['Manager Email'] = manager_emails.get(manager_name, '')

    analysis_data['Media Link'] = file.get('webViewLink', '')
    if duration_seconds and not analysis_data.get('Meeting duration (min)'):
        analysis_data['Meeting duration (min)'] = f"{duration_seconds / 60:.2f}"

    if not str(analysis_data.get('Kibana ID', '')).strip():
        kibana_match = re.search(r'(8[a-f0-9]{31})', file_name, re.IGNORECASE)
        if kibana_match: analysis_data['Kibana ID'] = kibana_match.group(1)

    if not str(analysis_data.get('Date', '')).strip():
        date_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})', file_name)
        if date_match: analysis_data['Date'] = date_match.group(0).replace('-', '/')

    if not str(analysis_data.get('Meeting Type', '')).strip():
        fn_lower = file_name.lower()
        if 'fresh' in fn_lower: analysis_data['Meeting Type'] = 'Fresh'
        elif 'followup' in fn_lower: analysis_data['Meeting Type'] = 'Followup'
        elif 'closure' in fn_lower: analysis_data['Meeting Type'] = 'Closure'
        elif 'renewal' in fn_lower: analysis_data['Meeting Type'] = 'Renewal'

    if not str(analysis_data.get('Society Name', '')).strip():
        name_part = re.split(r'[_|\- ]+(ASP|ERP|DEMO|FRESH|\d{2}[-/]\d{2})', file_name, flags=re.IGNORECASE)
        if name_part and name_part[0]: analysis_data['Society Name'] = name_part[0].strip().replace('_', ' ').replace('.', ' ')

    return analysis_data

# =======================
# Main Processing Function
# =======================
def process_single_file(drive_service, file_meta: Dict, member_name: str, config: Dict):
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

    analysis_data = analyze_transcript(transcript, member_name, config)

    if not analysis_data:
        raise ValueError("Gemini analysis failed or returned no data.")

    enriched_data = enrich_data_from_context(analysis_data, member_name, file_meta, duration_sec, config)
    
    return enriched_data

