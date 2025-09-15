import os
import json
import logging
import sheets
from google.cloud import bigquery
from google.oauth2 import service_account
from faster_whisper import WhisperModel
import google.generativeai as genai
from openai import OpenAI

# -----------------------
# Gemini Prompt Template
# -----------------------
GEMINI_PROMPT = """
### ROLE
You are a senior **Sales Conversation Analyst** for society-management software (ERP + ASP).  

### PRODUCT CONTEXT
ERP (₹12 + 18% GST per flat / month):
- Tally import/export, e-invoicing, bank reconciliation, vendor accounting, budgeting, 350+ bill combinations
- Purchase-order approvals, asset tagging via QR, inventory, meter reading → auto invoices
- Maker-checker billing approvals, reminders, late fee calc
- UPI/cards with in-house gateway, virtual accounts per unit
- Preventive maintenance, role-based access, defaulter tracking
- GST/TDS reports, balance sheet, dashboards  

ASP (₹22.5 + 18% GST per flat / month):
- Managed accounting: computerized bills/receipts
- Bookkeeping for all incomes/expenses
- Bank reconciliation + suspense follow-up
- Non-audited financial reports, finalisation support, audit coordination
- Vendor & PO/WO management, inventory, amenities booking
- Dedicated remote accountant, annual data backup  

### TASK
1. Parse the transcript.  
2. Fill in all fields in the schema below.  
3. If data is not explicitly available, leave as `""`.  
4. For multiple points, use **bulleted string format**:

### OUTPUT RULES
- Output must be **valid JSON only** (no explanations, no markdown).  
- Strictly follow the keys and order in the schema.  
- All values must be strings, even numbers (e.g., `"85"`, `"3.5"`).  
"""

# -----------------------
# Transcription
# -----------------------
def transcribe_audio(file_path: str, config: dict) -> str:
    """Transcribes audio using Faster-Whisper."""
    model_size = config["analysis"].get("whisper_model", "tiny.en")
    try:
        model = WhisperModel(model_size)
        segments, _ = model.transcribe(file_path)
        transcript = " ".join([s.text for s in segments])
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
        return transcript
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return ""

# -----------------------
# AI Analysis
# -----------------------
def analyze_transcript(transcript: str, config: dict) -> dict:
    """Calls Gemini API first; falls back to OpenRouter if quota fails."""
    raw_text = ""
    try:
        # --- Try Gemini first ---
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))

        response = model.generate_content(GEMINI_PROMPT + "\n\nTranscript:\n" + transcript)
        raw_text = response.text.strip()
        logging.info("SUCCESS: Gemini response received.")

    except Exception as e:
        logging.warning(f"Gemini failed, falling back to OpenRouter: {e}")
        try:
            client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.environ.get("OPENROUTER_API_KEY"))
            resp = client.chat.completions.create(
                model=config["analysis"].get("openrouter_model_name", "nousresearch/nous-hermes-2-mistral-7b-dpo"),
                messages=[{"role": "system", "content": GEMINI_PROMPT},
                          {"role": "user", "content": transcript}],
                response_format={"type": "json_object"}
            )
            raw_text = resp.choices[0].message.content.strip()
            logging.info("SUCCESS: OpenRouter fallback response received.")
        except Exception as e2:
            logging.error(f"Both Gemini and OpenRouter failed: {e2}")
            return {}

    # --- Parse JSON safely ---
    try:
        if raw_text.startswith("```"):
            raw_text = raw_text.strip("`")
            if raw_text.lower().startswith("json"):
                raw_text = raw_text[4:].strip()

        return json.loads(raw_text)
    except Exception as e:
        logging.error(f"ERROR parsing AI JSON output: {e}")
        return {}

# -----------------------
# BigQuery
# -----------------------
def write_to_bigquery(record: dict, config: dict):
    """Inserts a normalized record into BigQuery table with service account credentials."""
    try:
        project_id = config["google_bigquery"]["project_id"]
        dataset_id = config["google_bigquery"]["dataset_id"]
        table_id = config["google_bigquery"]["table_id"]

        # Use the same GCP_SA_KEY env var
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            logging.error("GCP_SA_KEY not found in environment.")
            return
        creds_info = json.loads(gcp_key_str)
        creds = service_account.Credentials.from_service_account_info(creds_info)

        client = bigquery.Client(project=project_id, credentials=creds)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        normalized = sheets.normalize_for_bigquery(record)
        errors = client.insert_rows_json(table_ref, [normalized])

        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
        else:
            logging.info("SUCCESS: Row inserted into BigQuery.")
    except Exception as e:
        logging.error(f"ERROR inserting into BigQuery: {e}")

# -----------------------
# Main File Processor
# -----------------------
def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    """Processes one file: download → transcribe → analyze → save to Sheets & BigQuery."""
    from gdrive import download_file

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id)

        # Step 1: Transcription
        transcript = transcribe_audio(local_path, config)

        # Step 2: AI Analysis
        analysis_data = analyze_transcript(transcript, config)
        if not analysis_data:
            raise ValueError("Empty analysis result")

        # Step 3: Add metadata
        analysis_data["Owner (Who handled the meeting)"] = member_name
        analysis_data["File Name"] = file_name
        analysis_data["File ID"] = file_id

        # Step 4: Write to Google Sheets
        sheets.write_analysis_result(gsheets_client, analysis_data, config)

        # Step 5: Write to BigQuery
        write_to_bigquery(analysis_data, config)

        # Step 6: Log success in ledger
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        # Always log failure in ledger
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config)
        raise
