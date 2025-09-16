import os
import json
import logging
import sheets
from faster_whisper import WhisperModel
import google.generativeai as genai

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
2. Fill in **all fields in the schema below**.
3. If data is not explicitly available, leave as "".
4. For multiple points, use **bulleted string format**.

### OUTPUT RULES
- Output must be **valid JSON only** (no explanations, no markdown).
- Strictly follow the keys and order in the schema.
- All values must be strings, even numbers (e.g., "85", "3.5").
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
    """Calls Gemini (fallback OpenRouter) to analyze transcript and return structured JSON."""
    if not transcript.strip():
        return {}

    prompt = GEMINI_PROMPT + "\n\nTranscript:\n" + transcript

    # Try Gemini first
    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))
        response = model.generate_content(prompt)
        raw_text = response.text.strip()

        # Clean code fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.strip("`")
            if raw_text.lower().startswith("json"):
                raw_text = raw_text[4:].strip()

        analysis_data = json.loads(raw_text)
        logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
    except Exception as e:
        logging.error(f"Gemini failed, trying OpenRouter: {e}")
        try:
            import openai
            openai.api_key = os.environ.get("OPENROUTER_API_KEY")
            response = openai.ChatCompletion.create(
                model=config["analysis"]["openrouter_model_name"],
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = response["choices"][0]["message"]["content"].strip()
            analysis_data = json.loads(raw_text)
            logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
        except Exception as e2:
            logging.error(f"OpenRouter also failed: {e2}")
            return {}

    # Normalize to default headers
    normalized = {}
    for h in sheets.DEFAULT_HEADERS:
        normalized[h] = str(analysis_data.get(h, "") or "")

    return normalized


# -----------------------
# Main File Processor
# -----------------------
def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    """Processes one file: download → transcribe → analyze → save to Sheets."""
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

        # Step 5: Update ledger
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        raise
