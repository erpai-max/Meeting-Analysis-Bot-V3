import logging
import json
import os
import tempfile
import re
from typing import Dict, Any, Optional

from faster_whisper import WhisperModel
import google.generativeai as genai

import gdrive
import sheets

# =======================
# Transcription
# =======================
def transcribe_audio(file_content, original_filename: str, model_name: str) -> Optional[str]:
    logging.info("Starting transcription process...")
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(original_filename)[1], delete=False) as temp_file:
        temp_file.write(file_content.read())
        temp_file_path = temp_file.name
    try:
        model = WhisperModel(model_name, device="cpu", compute_type="int8")
        logging.info(f"Transcribing {temp_file_path} with model '{model_name}'...")
        segments, _ = model.transcribe(temp_file_path, beam_size=5)
        transcript = " ".join(segment.text for segment in segments).strip()
        logging.info(f"SUCCESS: Transcription completed. Length: {len(transcript)} chars")
        return transcript
    except Exception as e:
        logging.error(f"ERROR: Transcription failed: {e}")
        return None
    finally:
        os.remove(temp_file_path)
        logging.info(f"Cleaned up temporary file: {temp_file_path}")


# =======================
# Analysis with Gemini
# =======================
def analyze_transcript(transcript: str, owner_name: str, config: Dict[str, Any]) -> Optional[Dict[str, str]]:
    logging.info("Starting analysis with Gemini...")

    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logging.error("CRITICAL: GEMINI_API_KEY not set.")
        return None

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel(config["analysis"]["gemini_model"])

    # Full rich prompt with ERP + ASP scope + JSON schema
    prompt = f"""
    ### ROLE AND GOAL ###
    You are an expert sales meeting analyst for our company, specializing in **ERP** and **ASP** solutions for housing societies.
    Analyze the transcript of a meeting handled by **{owner_name}**.

    ### PRODUCT CONTEXT ###
    ---
    **ERP (Enterprise Resource Planning)**
    * Self-service software for society management
    * Price: ₹12 + 18% GST per flat/month
    * Key differentiators:
      - Instant settlement, low gateway charges
      - Tally integration, GST/TDS/e-invoicing, bank reconciliation
      - 350+ billing combos, late fee calc, reminders, maker-checker
      - Asset mgmt (QR code), inventory mgmt, preventive maintenance
      - Virtual accounts, role-based access

    **ASP (Accounting Services as a Product)**
    * Done-for-you accounting service using our ERP
    * Price: ₹22.5 + 18% GST per flat/month
    * Scope:
      - Dedicated accountant
      - Billing + receipts + bookkeeping
      - Bank reconciliation, suspense entries
      - Financial reports, audit coordination
      - Vendor mgmt, inventory, amenities booking
      - Includes ERP features + managed services

    ---
    ### INPUT: TRANSCRIPT ###
    {transcript}
    ---

    ### TASK ###
    1. Identify if meeting was about ERP, ASP, or both.
    2. Extract structured insights.
    3. If info is missing, use "" (empty string).
    4. Multi-point fields (e.g., Key Discussion Points) → bullet list:
       - Point 1
       - Point 2

    ### OUTPUT RULES ###
    - Only output valid JSON
    - No text outside JSON
    - All values as strings (even numbers)

    ### SCHEMA ###
    {{
      "Date": "", "POC Name": "", "Society Name": "", "Visit Type": "", "Meeting Type": "",
      "Amount Value": "", "Months": "", "Deal Status": "", "Vendor Leads": "", "Society Leads": "",
      "Opening Pitch Score": "", "Product Pitch Score": "", "Cross-Sell / Opportunity Handling": "",
      "Closing Effectiveness": "", "Negotiation Strength": "", "Rebuttal Handling": "",
      "Overall Sentiment": "", "Total Score": "", "% Score": "", "Risks / Unresolved Issues": "",
      "Improvements Needed": "", "Owner (Who handled the meeting)": "{owner_name}", "Email Id": "",
      "Kibana ID": "", "Manager": "", "Produc Pitch": "", "Team": "", "Media Link": "", "Doc Link": "",
      "Suggestions & Missed Topics": "", "Pre-meeting brief": "", "Meeting duration (min)": "",
      "Rapport Building": "", "Improvement Areas": "", "Product Knowledge Displayed": "",
      "Call Effectiveness and Control": "", "Next Step Clarity and Commitment": "",
      "Missed Opportunities": "", "Key Discussion Points": "", "Key Questions": "",
      "Competition Discussion": "", "Action items": "", "Positive Factors": "", "Negative Factors": "",
      "Customer Needs": "", "Overall Client Sentiment": "", "Feature Checklist Coverage": "",
      "Manager Email": ""
    }}
    """

    try:
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        logging.info("SUCCESS: Analysis with Gemini complete.")
        return json.loads(response.text)
    except Exception as e:
        logging.error(f"ERROR: Gemini analysis failed: {e}")
        return None


# =======================
# Orchestrator
# =======================
def process_single_file(drive_service, gsheets_client, file_meta: Dict, member_name: str, config: Dict[str, Any]):
    """Download, transcribe, analyze, and store results for a single file."""
    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown Filename")

    file_content = gdrive.download_file(drive_service, file_id)
    transcript = transcribe_audio(file_content, file_name, config["analysis"]["whisper_model"])

    if not transcript:
        raise RuntimeError("Transcription failed")

    if config["analysis"].get("redact_pii"):
        logging.info("Redacting PII from transcript (basic masking)...")
        transcript = re.sub(r"\b\d{10}\b", "[REDACTED_PHONE]", transcript)

    analysis_data = analyze_transcript(transcript, member_name, config)
    if not analysis_data:
        raise RuntimeError("Analysis failed")

    # Write to Google Sheets
    sheets.append_results(gsheets_client, analysis_data, config)
    # Stream to BigQuery
    try:
        from google.cloud import bigquery
        bq_client = bigquery.Client()
        sheets.stream_to_bigquery(bq_client, analysis_data, config)
    except Exception as e:
        logging.warning(f"BigQuery streaming skipped: {e}")
