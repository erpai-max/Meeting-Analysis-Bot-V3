import logging
import io
import json
import os
import re
import sys
import tempfile  # <-- THIS IS THE FIX
from typing import Dict, Tuple, Optional

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
from tenacity import retry, stop_after_attempt, wait_exponential

# --- Dependencies ---
from faster_whisper import WhisperModel

# =======================
# PII Redaction
# =======================
def redact_pii(text: str) -> str:
    """Redacts email addresses and phone numbers from a string."""
    text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[REDACTED EMAIL]', text)
    text = re.sub(r'(\+91[\-\s]?)?[789]\d{9}', '[REDACTED PHONE]', text)
    return text

# =======================
# Transcription
# =======================
def transcribe_audio(file_content: io.BytesIO, original_filename: str, config: Dict) -> Tuple[Optional[str], float]:
    """Transcribes audio and returns the transcript and duration."""
    logging.info("Starting transcription process...")
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(original_filename)[1], delete=False) as temp_file:
        temp_file.write(file_content.read())
        temp_file_path = temp_file.name

    try:
        model_size = config.get('analysis', {}).get('whisper_model', 'tiny.en')
        logging.info(f"Initializing Whisper model ({model_size})...")
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        
        logging.info(f"Transcribing {temp_file_path}...")
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
# Gemini Analysis with Retry Logic
# =======================
RETRY_CONFIG_GEMINI = {
    'wait': wait_exponential(multiplier=2, min=5, max=60),
    'stop': stop_after_attempt(3),
    'retry_error_callback': lambda retry_state: "RATE_LIMIT_EXCEEDED" if isinstance(retry_state.outcome.exception(), google_exceptions.ResourceExhausted) else None,
}

@retry(**RETRY_CONFIG_GEMINI)
def analyze_transcript(transcript: str, owner_name: str, config: Dict):
    """Analyzes the transcript using Gemini API, with quota handling."""
    logging.info("Starting analysis with Gemini...")
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_api_key:
        logging.error("CRITICAL: GEMINI_API_KEY environment variable not found.")
        return None

    genai.configure(api_key=gemini_api_key)
    
    if config.get('analysis', {}).get('redact_pii', False):
        logging.info("PII redaction is enabled. Redacting transcript...")
        transcript = redact_pii(transcript)

    try:
        model = genai.GenerativeModel(config.get('analysis', {}).get('gemini_model', 'gemini-1.5-flash'))
        prompt = config['gemini_prompt'].format(
            owner_name=owner_name,
            transcript=transcript
        )
        
        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        logging.info("SUCCESS: Analysis with Gemini complete.")
        return json.loads(response.text)
    except google_exceptions.ResourceExhausted as e:
        logging.error(f"ERROR: Gemini API analysis failed with 429 Quota Exceeded. Will stop this run.")
        raise
    except Exception as e:
        logging.error(f"ERROR: Gemini API analysis failed: {e}")
        raise

# =======================
# Data Enrichment
# =======================
def enrich_data_from_context(analysis_data: Dict, member_name: str, file_meta: Dict, duration_seconds: float, config: Dict) -> Dict:
    """Overrides empty values in analysis_data with info from file context."""
    logging.info("Enriching data with context from filename and folder structure...")
    
    file_name = file_meta.get("name", "")
    manager_map = config.get('manager_map', {})

    analysis_data['Owner'] = member_name
    manager_info = manager_map.get(member_name, {})
    analysis_data['Manager'] = manager_info.get('Manager', '')
    analysis_data['Team'] = manager_info.get('Team', '')
    analysis_data['Email Id'] = manager_info.get('Email', '')

    analysis_data['Media Link'] = file_meta.get('webViewLink', '')
    if duration_seconds:
        analysis_data['Meeting duration (min)'] = f"{duration_seconds / 60:.2f}"

    pipe_parts = [p.strip() for p in file_name.split('|')]
    if len(pipe_parts) == 4 and re.match(r'8[a-f0-9]{31}', pipe_parts[0], re.IGNORECASE):
        if not analysis_data.get('Kibana ID'): analysis_data['Kibana ID'] = pipe_parts[0]
        if not analysis_data.get('Meeting Type'): analysis_data['Meeting Type'] = pipe_parts[1]
        analysis_data['Team'] = pipe_parts[2]
        if not analysis_data.get('Product Pitch'): analysis_data['Product Pitch'] = pipe_parts[3]
    else:
        if not analysis_data.get('Kibana ID'):
            kibana_match = re.search(r'(8[a-f0-9]{31})', file_name, re.IGNORECASE)
            if kibana_match: analysis_data['Kibana ID'] = kibana_match.group(1)

        if not analysis_data.get('Date'):
            date_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})', file_name)
            if date_match: analysis_data['Date'] = date_match.group(0).replace('-', '/')

        if not analysis_data.get('Meeting Type'):
            fn_lower = file_name.lower()
            if 'fresh' in fn_lower: analysis_data['Meeting Type'] = 'Fresh'
            elif 'followup' in fn_lower: analysis_data['Meeting Type'] = 'Followup'
            elif 'closure' in fn_lower: analysis_data['Meeting Type'] = 'Closure'
            elif 'general' in fn_lower: analysis_data['Meeting Type'] = 'General'
            elif 'renewal' in fn_lower: analysis_data['Meeting Type'] = 'Renewal'
            
        if not analysis_data.get('Society Name'):
            name_part = re.split(r'[_|\- ]+(ASP|ERP|DEMO|FRESH|PAID|RENEWAL|\d{2}[-/]\d{2})', file_name, flags=re.IGNORECASE)
            if name_part and name_part[0]:
                analysis_data['Society Name'] = name_part[0].strip().replace('_', ' ').replace('.', ' ')

    return analysis_data

