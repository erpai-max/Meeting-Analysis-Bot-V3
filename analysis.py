import os
import json
import logging
import sheets
from google.cloud import bigquery
from faster_whisper import WhisperModel
import google.generativeai as genai
import httpx

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
4. For multiple points, use **bulleted string format**.

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
# AI Analysis with Failover
# -----------------------
def analyze_transcript(transcript: str, config: dict) -> dict:
    """Analyze transcript using Gemini API, fallback to OpenRouter if quota exceeded."""
    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))
        response = model.generate_content(GEMINI_PROMPT + "\n\nTranscript:\n" + transcript)
        raw_text = response.text.strip()
    except Exception as e:
        logging.warning(f"Gemini failed: {e}. Trying OpenRouter fallback...")
        try:
            openrouter_key = os.environ.get("OPENROUTER_API_KEY")
            if not openrouter_key:
                raise RuntimeError("No OpenRouter API key set")

            headers = {
                "Authorization": f"Bearer {openrouter_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": config["analysis"].get(
                    "openrouter_model_name",
                    "nousresearch/nous-hermes-2-mistral-7b-dpo"
                ),
                "messages": [
                    {"role": "system", "content": GEMINI_PROMPT},
                    {"role": "user", "content": transcript},
                ],
            }
            resp = httpx.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=60)
            resp.raise_for_status()
            raw_text = resp.json()["choices"][0]["message"]["content"]
        except Exception as e2:
            logging.error(f"Fallback also failed: {e2}")
            return {}

    # Ensure only JSON is returned
    raw_text = raw_text.strip()
    if raw_text.startswith("```"):
        raw_text = raw_text.strip("`")
        if raw_text.lower().startswith("json"):
            raw_text = raw_text[4:].strip()

    try:
        analysis_data = json.loads(raw_text)
        logging.info("SUCCESS: Parsed AI analysis JSON output.")
        return analysis_data
    except Exception as e:
        logging.error(f"ERROR parsing AI output: {e}")
        return {}

# -----------------------
# BigQuery (Safe for Free Tier)
# -----------------------
def write_to_bigquery(record: dict, config: dict):
    """Inserts a normalized record into BigQuery table (skips if free-tier blocks streaming)."""
    try:
        project_id = config["google_bigquery"]["project_id"]
        dataset_id = config["google_bigquery"]["dataset_id"]
        table_id = config["google_bigquery"]["table_id"]

        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        normalized = sheets.normalize_for_bigquery(record)
        errors = client.insert_rows_json(table_ref, [normalized])

        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
        else:
            logging.info("SUCCESS: Row inserted into BigQuery.")
    except Exception as e:
        if "Streaming insert is not allowed" in str(e):
            logging.warning("⚠️ BigQuery streaming insert blocked (free tier). Skipping insert.")
        else:
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
        local_path = download_file(drive_service, file_id, file_name)

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

        # Step 5: Write to BigQuery (skipped if free tier blocks)
        write_to_bigquery(analysis_data, config)

        # Step 6: Log success in ledger
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        raise
