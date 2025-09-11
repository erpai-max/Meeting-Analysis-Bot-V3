import logging
import re
import sys
import tempfile
import io
import os
import json
from typing import Dict, Optional, Tuple

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
from faster_whisper import WhisperModel

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
def transcribe_audio(file_content: io.BytesIO, original_filename: str, config: Dict) -> Optional[Tuple[str, float]]:
    """Transcribes audio and returns transcript and duration."""
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
# Gemini Analysis
# =======================
def analyze_transcript(transcript: str, owner_name: str, config: Dict):
    """Analyzes transcript with Gemini. Returns analysis dict or special string on quota error."""
    logging.info("Starting analysis with Gemini...")
    
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logging.error("CRITICAL: GEMINI_API_KEY environment variable not found.")
        return None
        
    genai.configure(api_key=gemini_key)
    
    prompt_template = config['gemini_prompt']
    prompt = prompt_template.format(owner_name=owner_name, transcript=transcript)
    
    try:
        model = genai.GenerativeModel(config['analysis']['gemini_model'])
        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        logging.info("SUCCESS: Analysis with Gemini complete.")
        return json.loads(response.text)
    except google_exceptions.ResourceExhausted as e:
        logging.error(f"ERROR: Gemini API analysis failed with 429 Quota Exceeded: {e}")
        return "RATE_LIMIT_EXCEEDED"
    except Exception as e:
        logging.error(f"ERROR: Gemini API analysis failed: {e}")
        return None

# =======================
# Data Enrichment
# =======================
def enrich_data(analysis_data: Dict, member_name: str, file: Dict, duration_seconds: float, config: Dict) -> Dict:
    """Fills in missing data from context (filename, folder structure, etc.)."""
    logging.info("Enriching data with context...")
    
    file_name = file.get('name', '')
    manager_map = config.get('manager_map', {})
    manager_emails = config.get('manager_emails', {}) # Get the new manager email map

    # 1. Enrich from folder context
    analysis_data['Owner'] = member_name
    manager_info = manager_map.get(member_name, {})
    manager_name = manager_info.get('Manager', '')
    analysis_data['Manager'] = manager_name
    analysis_data['Team'] = manager_info.get('Team', '')
    analysis_data['Email Id'] = manager_info.get('Email', '')
    
    # NEW: Add Manager's Email
    analysis_data['Manager Email'] = manager_emails.get(manager_name, '')

    # 2. Enrich from file metadata
    analysis_data['Media Link'] = file.get('webViewLink', '')
    if duration_seconds:
        analysis_data['Meeting duration (min)'] = f"{duration_seconds / 60:.2f}"

    # 3. Enrich from filename if AI analysis is empty
    pipe_parts = [p.strip() for p in file_name.split('|')]
    if len(pipe_parts) == 4 and re.match(r'8[a-f0-9]{31}', pipe_parts[0], re.IGNORECASE):
        analysis_data['Kibana ID'] = analysis_data.get('Kibana ID') or pipe_parts[0]
        analysis_data['Meeting Type'] = analysis_data.get('Meeting Type') or pipe_parts[1]
        analysis_data['Team'] = analysis_data.get('Team') or pipe_parts[2]
        analysis_data['Product Pitch'] = analysis_data.get('Product Pitch') or pipe_parts[3]
    else:
        # Fallback parsing
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
            
        if not analysis_data.get('Society Name'):
            name_part = re.split(r'[_|\- ]+(ASP|ERP|DEMO|FRESH|\d{2}[-/]\d{2})', file_name, flags=re.IGNORECASE)
            if name_part and name_part[0]: analysis_data['Society Name'] = name_part[0].strip().replace('_', ' ')

    return analysis_data

# =======================
# Main Processing Function
# =======================
def process_single_file(drive_service, gsheets_client, file, member_name, config):
    """Orchestrates the processing of a single media file."""
    
    file_id = file.get("id")
    processed_folder_id = config['google_drive']['processed_folder_id']

    # 1. Download
    file_content = gdrive.download_file(drive_service, file_id)

    # 2. Transcribe
    transcript, duration_sec = None, 0.0
    transcribe_result = transcribe_audio(file_content, file.get('name', ''), config)
    if transcribe_result:
        transcript, duration_sec = transcribe_result

    if not transcript:
        raise ValueError("Transcription failed or produced an empty transcript.")

    # 3. Redact PII if enabled
    if config['analysis'].get('redact_pii', False):
        logging.info("PII redaction is enabled. Redacting transcript...")
        transcript = redact_pii(transcript)

    # 4. Analyze with Gemini
    analysis_data = analyze_transcript(transcript, member_name, config)

    if analysis_data == "RATE_LIMIT_EXCEEDED":
        logging.warning("Gemini API quota exceeded. Stopping the current workflow run.")
        logging.warning("Remaining files will be processed on the next scheduled run.")
        sys.exit(0) # Exit successfully, leaving file to be reprocessed

    if not analysis_data:
        raise ValueError("Gemini analysis failed or returned no data.")

    # 5. Enrich and Write to Sheet
    enriched_data = enrich_data(analysis_data, member_name, file, duration_sec, config)
    sheets.write_results(gsheets_client, enriched_data, config)

    # 6. Move to Processed folder and Update Ledger
    gdrive.move_file(drive_service, file_id, file['parents'][0], processed_folder_id)
    sheets.update_ledger(gsheets_client, file_id, "Success", "", config)

