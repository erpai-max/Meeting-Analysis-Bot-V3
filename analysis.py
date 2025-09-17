# analysis.py
import os
import json
import logging
import time
import re
from typing import Dict
import sheets
from faster_whisper import WhisperModel
import google.generativeai as genai

GEMINI_PROMPT = """
### ROLE AND GOAL ###
You are an expert sales meeting analyst for our company, specializing in Society Management software. Your goal is to meticulously analyze a sales meeting transcript, score the performance of the sales representative '{owner_name}', and extract key business information. Your analysis must differentiate whether the primary product discussed was our ERP solution or our ASP offering.

### CONTEXT: PRODUCT AND PRICING INFORMATION ###
---
... (Include your full product context here in your prompt.txt file) ...
---

### INPUT: MEETING TRANSCRIPT ###
{transcript}

### TASK AND INSTRUCTIONS ###
Analyze the transcript. Return a single JSON object exactly matching the schema (fill "N/A" if missing).
"""

def transcribe_audio(file_path: str, config: dict) -> str:
    """Transcribes audio using faster-whisper without unsupported kwargs."""
    model_size = config.get("analysis", {}).get("whisper_model", "tiny.en")
    try:
        model = WhisperModel(model_size)
        segments, _ = model.transcribe(file_path)
        transcript = " ".join([s.text.strip() for s in segments if getattr(s, "text", None)])
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
        return transcript
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return ""

def _clean_json_text(raw_text: str) -> str:
    """Remove backticks or fences and extract JSON payload."""
    text = raw_text.strip()
    # Remove triple backticks and leading 'json' markers
    if text.startswith("```"):
        text = text.strip("`")
        text = text.lstrip(" \n")
        if text.lower().startswith("json"):
            text = text[text.lower().find("json") + 4:].strip()
    return text

def analyze_transcript(transcript: str, owner_name: str, config: dict) -> Dict[str, str]:
    """Call Gemini first, then OpenRouter (openai) fallback. Return normalized dict matching sheet headers."""
    if not transcript or not transcript.strip():
        return {}

    prompt = GEMINI_PROMPT.replace("{owner_name}", owner_name).replace("{transcript}", transcript)

    # Try Gemini
    try:
        gem_key = os.environ.get("GEMINI_API_KEY")
        if not gem_key:
            raise ValueError("GEMINI_API_KEY not set")
        genai.configure(api_key=gem_key)
        model_name = config.get("analysis", {}).get("gemini_model", "gemini-1.5-flash")
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        raw = resp.text or resp.json or ""
        raw = _clean_json_text(raw)
        analysis_data = json.loads(raw)
        logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
    except Exception as e:
        logging.error(f"Gemini failed: {e}. Trying OpenRouter/OpenAI fallback.")
        # Try OpenRouter/OpenAI fallback
        try:
            import openai
            openai.api_key = os.environ.get("OPENROUTER_API_KEY")
            if not openai.api_key:
                raise ValueError("OPENROUTER_API_KEY not set")
            response = openai.ChatCompletion.create(
                model=config.get("analysis", {}).get("openrouter_model_name"),
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
            )
            raw = response["choices"][0]["message"]["content"].strip()
            raw = _clean_json_text(raw)
            analysis_data = json.loads(raw)
            logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
        except Exception as e2:
            logging.error(f"OpenRouter fallback failed: {e2}")
            return {}

    # Normalize to expected sheet headers
    normalized = {}
    for h in sheets.DEFAULT_HEADERS:
        val = analysis_data.get(h) if isinstance(analysis_data, dict) else None
        if val is None:
            # some older prompt versions use slightly different key names; try basic mapping
            val = analysis_data.get(h.replace(" (Who handled the meeting)", "")) if isinstance(analysis_data, dict) else None
        normalized[h] = str(val) if val not in (None, "") else "N/A"

    return normalized

def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    from gdrive import download_file, move_file
    file_id = file_meta.get("id")
    file_name = file_meta.get("name", "Unknown")
    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        transcript = transcribe_audio(local_path, config)
        if not transcript:
            raise ValueError("Transcription empty")

        analysis_data = analyze_transcript(transcript, member_name, config)
        if not analysis_data:
            raise ValueError("Empty analysis result")

        # Add metadata
        analysis_data["Owner (Who handled the meeting)"] = member_name
        analysis_data["Media Link"] = file_name
        analysis_data["Date"] = analysis_data.get("Date", "N/A")
        # Write result + ledger
        sheets.write_analysis_result(gsheets_client, analysis_data, config)
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        # Move file to processed folder
        try:
            move_file(drive_service, file_id, file_meta.get("parents", [None])[0], config["google_drive"]["processed_folder_id"])
        except Exception as e:
            logging.warning(f"Could not move file to processed: {e}")

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        try:
            sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        except Exception as e2:
            logging.error(f"ERROR updating ledger after failure: {e2}")
        # quarantine the file
        try:
            from gdrive import quarantine_file
            quarantine_file(drive_service, file_id, file_meta.get("parents", [None])[0], str(e), config)
        except Exception as e3:
            logging.error(f"ERROR quarantining file: {e3}")
        raise
