import logging
import json
import tempfile
import os
import re
from typing import Dict, Any

from faster_whisper import WhisperModel
import google.generativeai as genai

import sheets

# =======================
# Transcription
# =======================
def transcribe_audio(file_content, file_name: str, config: Dict) -> str:
    """Transcribes audio using Whisper."""
    logging.info("Starting transcription process...")
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(file_name)[1], delete=False) as temp_file:
        temp_file.write(file_content.read())
        temp_file_path = temp_file.name

    try:
        model_size = config["analysis"].get("whisper_model", "tiny.en")
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        segments, _ = model.transcribe(temp_file_path, beam_size=5)
        transcript = " ".join(seg.text for seg in segments).strip()
        logging.info(f"SUCCESS: Transcription completed. Length: {len(transcript)} chars.")
        return transcript
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        raise
    finally:
        os.remove(temp_file_path)
        logging.info(f"Cleaned up temporary file: {temp_file_path}")


# =======================
# AI Analysis
# =======================
def analyze_transcript(transcript: str, owner_name: str, config: Dict) -> Dict[str, Any]:
    """Analyzes transcript using Gemini with ERP/ASP rich prompt."""
    logging.info("Starting analysis with Gemini...")

    model_name = config["analysis"].get("gemini_model", "gemini-1.5-flash")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not found in environment.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    prompt = f"""
    ### ROLE AND GOAL ###
    You are an expert sales meeting analyst for our company, specializing in Society Management software. 
    Analyze the transcript of a sales meeting handled by '{owner_name}'.

    ### CONTEXT: PRODUCTS ###
    - **ERP (₹12 + 18% GST per flat/month)**: Comprehensive society management software.
    - **ASP (₹22.5 + 18% GST per flat/month)**: Managed accounting service with dedicated accountant.

    ### TRANSCRIPT ###
    {transcript}

    ### TASK ###
    Identify if ERP, ASP, or both were pitched. Extract structured fields.

    ### OUTPUT ###
    Return ONLY valid JSON with these fields:
    {json.dumps(config["sheets_headers"], indent=2)}
    """

    response = model.generate_content(
        prompt,
        generation_config={"response_mime_type": "application/json"},
    )
    try:
        data = json.loads(response.text)
        logging.info("SUCCESS: Gemini analysis complete.")
        return data
    except Exception as e:
        logging.error(f"ERROR parsing Gemini response: {e}")
        raise


# =======================
# Context Enrichment
# =======================
def enrich_data(analysis_data: Dict[str, Any], member_name: str, file_name: str, config: Dict) -> Dict[str, Any]:
    """Adds missing context (owner, team, manager, email, inferred date/society)."""
    manager_map = config.get("manager_map", {})
    enriched = dict(analysis_data)

    enriched["Owner (Who handled the meeting)"] = member_name
    m_info = manager_map.get(member_name, {})
    enriched["Manager"] = m_info.get("Manager", "")
    enriched["Team"] = m_info.get("Team", "")
    enriched["Email Id"] = m_info.get("Email", "")

    # Infer date from filename if missing
    if not enriched.get("Date"):
        date_match = re.search(r"(\d{2}[-/]\d{2}[-/]\d{2,4})", file_name)
        if date_match:
            enriched["Date"] = date_match.group(1)

    # Infer visit type from filename
    fn_lower = file_name.lower()
    if not enriched.get("Visit Type"):
        if "asp" in fn_lower:
            enriched["Visit Type"] = "ASP Demo"
        elif "erp" in fn_lower:
            enriched["Visit Type"] = "ERP Demo"
        elif "training" in fn_lower:
            enriched["Visit Type"] = "Training"
        elif "follow" in fn_lower:
            enriched["Visit Type"] = "Follow Up"

    # Infer society name
    if not enriched.get("Society Name"):
        parts = re.split(r"[-|_]", file_name)
        if parts:
            enriched["Society Name"] = parts[0].strip()

    return enriched


# =======================
# Orchestration
# =======================
def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: Dict):
    """Handles the full pipeline for one file."""
    file_id = file_meta["id"]
    file_name = file_meta["name"]

    try:
        # Step 1: Download
        fh = drive_service.files().get_media(fileId=file_id).execute()
        file_content = tempfile.SpooledTemporaryFile()
        file_content.write(fh)
        file_content.seek(0)

        # Step 2: Transcribe
        transcript = transcribe_audio(file_content, file_name, config)

        # Step 3: Analyze
        analysis_data = analyze_transcript(transcript, member_name, config)

        # Step 4: Enrich
        enriched = enrich_data(analysis_data, member_name, file_name, config)

        # Step 5: Write to Sheets
        sheets.write_results(gsheets_client, enriched, config)

        # Step 6: Update Ledger
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config)

        logging.info(f"SUCCESS: Completed processing for {file_name}")

    except Exception as e:
        logging.error(f"ERROR processing file {file_name}: {e}")
        raise
