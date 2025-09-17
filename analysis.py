# analysis.py
import os
import json
import logging
import time
from typing import Dict, Any

import sheets
from faster_whisper import WhisperModel
import google.generativeai as genai

# Try to import openai for OpenRouter fallback if available
try:
    import openai
except Exception:
    openai = None

# GEMINI prompt - keep concise here, you can move a longer prompt to prompt.txt
GEMINI_PROMPT = """
### ROLE
You are an expert sales meeting analyst for society-management software (ERP + ASP).

### TASK
1) Parse the transcript and extract the fields exactly matching the sheet headers.
2) If a field is not present, return "N/A" (as a string).
3) Output exactly one JSON object and nothing else.
"""

# Transcription
def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribe using faster-whisper. Use only positional args to avoid API mismatch.
    Returns concatenated transcript string (empty string on failure).
    """
    model_size = config.get("analysis", {}).get("whisper_model", "tiny.en")
    try:
        model = WhisperModel(model_size)
        # call with positional argument only to avoid unexpected keyword errors
        result = model.transcribe(file_path)
        # result may be tuple (segments, info) or object depending on version
        if isinstance(result, tuple) and len(result) >= 1:
            segments = result[0]
        else:
            segments = result

        # segments might be iterable of objects with .text
        if hasattr(segments, "__iter__"):
            transcript = " ".join([getattr(s, "text", str(s)) for s in segments])
        else:
            transcript = str(segments)

        transcript = transcript.strip()
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
        return transcript
    except TypeError as te:
        logging.error(f"ERROR during transcription (type error): {te}")
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
    return ""

# Clean AI raw text to JSON string
def _extract_json_text(raw: str) -> str:
    if not raw:
        return ""
    txt = raw.strip()
    # remove code fences
    if txt.startswith("```") and txt.endswith("```"):
        txt = txt.strip("`")
    # if begins with "json" after fences, strip that
    if txt.lower().lstrip().startswith("json"):
        # remove leading "json" + optional colon
        txt = txt.lstrip()
        if txt.lower().startswith("json:"):
            txt = txt[5:].lstrip()
        elif txt.lower().startswith("json"):
            txt = txt[4:].lstrip()
    return txt

# Parse AI output to dict
def _parse_ai_output(raw_text: str) -> Dict[str, Any]:
    txt = _extract_json_text(raw_text)
    if not txt:
        return {}
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        # sometimes model returns single quotes or Python dict-like string; try eval safely
        try:
            # Only as fallback: replace single-quotes with double and try again
            alt = txt.replace("'", '"')
            return json.loads(alt)
        except Exception:
            logging.error("Failed to parse AI output as JSON")
            return {}

# Call Gemini (google.generativeai) with prompt + transcript
def analyze_with_gemini(prompt: str, transcript: str, config: dict) -> Dict[str, Any]:
    try:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not set")
        genai.configure(api_key=api_key)
        model_name = config.get("analysis", {}).get("gemini_model", "gemini-1.5-flash")
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt + "\n\nTranscript:\n" + transcript)
        raw_text = getattr(response, "text", str(response)).strip()
        parsed = _parse_ai_output(raw_text)
        logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
        return parsed
    except Exception as e:
        logging.error(f"Gemini analysis failed: {e}")
        return {}

# Fallback to OpenRouter (OpenAI-compatible)
def analyze_with_openrouter(prompt: str, transcript: str, config: dict) -> Dict[str, Any]:
    if openai is None:
        logging.error("OpenAI library not present; cannot use OpenRouter fallback.")
        return {}
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        logging.error("OPENROUTER_API_KEY not set; skipping OpenRouter fallback.")
        return {}
    try:
        openai.api_key = key
        model_name = config.get("analysis", {}).get("openrouter_model_name")
        if not model_name:
            logging.error("openrouter_model_name missing in config.")
            return {}
        # Use ChatCompletion create (OpenAI compatible) as fallback
        response = openai.ChatCompletion.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt + "\n\nTranscript:\n" + transcript}],
            max_tokens=1500,
        )
        raw_text = response["choices"][0]["message"]["content"].strip()
        parsed = _parse_ai_output(raw_text)
        logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
        return parsed
    except Exception as e:
        logging.error(f"OpenRouter fallback failed: {e}")
        return {}

# Main analyze function: try Gemini then OpenRouter
def analyze_transcript(transcript: str, config: dict) -> Dict[str, str]:
    if not transcript or not transcript.strip():
        logging.warning("Empty transcript provided to analyze_transcript.")
        return {}

    prompt = config.get("analysis", {}).get("rich_prompt", GEMINI_PROMPT)
    # If the user stored a richer prompt in config, use it; otherwise use default
    if not prompt:
        prompt = GEMINI_PROMPT

    # First try Gemini
    parsed = analyze_with_gemini(prompt, transcript, config)
    if parsed:
        return _normalize_ai_output(parsed)
    # Fallback
    parsed = analyze_with_openrouter(prompt, transcript, config)
    if parsed:
        return _normalize_ai_output(parsed)
    return {}

# Normalize AI dict to ensure all sheet headers are present (strings)
def _normalize_ai_output(ai_dict: Dict[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    headers = sheets.DEFAULT_HEADERS
    for h in headers:
        # Try exact key, case-insensitive, or short-key match
        val = None
        if h in ai_dict:
            val = ai_dict[h]
        else:
            # case-insensitive match
            for k in ai_dict.keys():
                if str(k).strip().lower() == h.strip().lower():
                    val = ai_dict.get(k)
                    break
        if val is None or (isinstance(val, str) and val.strip() == ""):
            normalized[h] = "N/A"
        else:
            # all values must be strings
            if isinstance(val, (dict, list)):
                try:
                    normalized[h] = json.dumps(val, ensure_ascii=False)
                except Exception:
                    normalized[h] = str(val)
            else:
                normalized[h] = str(val)
    return normalized

# Main file processing function used by main.py
def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    """
    Downloads the file (via gdrive.download_file), transcribes, analyzes, writes to sheet and updates ledger.
    """
    from gdrive import download_file, move_file, quarantine_file  # local imports to avoid circular deps

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # Transcribe
        logging.info(f"Processing audio file for transcription: {file_name}")
        transcript = transcribe_audio(local_path, config)
        if not transcript:
            raise ValueError("Empty transcript")

        # Analyze
        logging.info("Running AI analysis on transcript...")
        ai_result = analyze_transcript(transcript, config)
        if not ai_result:
            raise ValueError("Empty analysis result")

        # Add metadata fields expected by sheet
        ai_result["Owner (Who handled the meeting)"] = member_name or ai_result.get("Owner (Who handled the meeting)", "N/A")
        ai_result["Media Link"] = file_name
        ai_result["Kibana ID"] = ai_result.get("Kibana ID", "N/A")

        # Write to Google Sheets
        sheets.write_analysis_result(gsheets_client, ai_result, config)

        # Update ledger as processed
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        # Attempt to ledger the failure
        try:
            sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        except Exception as e2:
            logging.error(f"ERROR updating ledger for failure case: {e2}")
        # Move to quarantine folder
        try:
            quarantine_file(drive_service, file_id, file_meta.get("parents", [None])[0], str(e), config)
        except Exception as move_ex:
            logging.error(f"Could not quarantine file {file_name}: {move_ex}")
        raise
