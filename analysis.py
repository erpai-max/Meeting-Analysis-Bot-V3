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
### ROLE AND GOAL ###
You are an expert sales meeting analyst for our company, specializing in Society Management software.

Analyze the provided meeting transcript, score the sales representative, and extract 47 structured fields.
If a field is not explicitly available, return "N/A".

Your output MUST be valid JSON only.
"""

# -----------------------
# Transcription
# -----------------------
def transcribe_audio(file_path: str, config: dict) -> str:
    """Transcribes audio using Faster-Whisper with chunking for long files."""
    model_size = config["analysis"].get("whisper_model", "tiny.en")
    try:
        model = WhisperModel(model_size)
        segments, _ = model.transcribe(file_path, chunk_size=30)  # FIXED: chunking
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

    # Normalize with headers
    normalized = {}
    for h in sheets.DEFAULT_HEADERS:
        normalized[h] = str(analysis_data.get(h, "") or "N/A")

    # FIXED: check for empty result
    if all(v == "N/A" for v in normalized.values()):
        logging.error("AI returned empty analysis result.")
        return {}

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
