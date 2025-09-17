import os
import re
import json
import logging
from typing import Dict, Any
from faster_whisper import WhisperModel
import google.generativeai as genai

import sheets
from gdrive import move_to_processed, quarantine_file


# -----------------------
# Helpers
# -----------------------

def _load_prompt(owner_name: str) -> str:
    """Load prompt.txt and inject owner_name placeholder if present."""
    try:
        with open("prompt.txt", "r", encoding="utf-8") as f:
            prompt = f.read()
        return prompt.replace("{owner_name}", owner_name)
    except Exception as e:
        logging.warning(f"Could not read prompt.txt; using minimal inline prompt. Error: {e}")
        return (
            "You are an expert sales meeting analyst. "
            "Return a single JSON object with the exact 47 keys I provide. "
            "If a field is not present, return \"N/A\". "
            "All values must be strings."
        )


def _clean_and_parse_json(raw_text: str) -> Dict[str, Any]:
    """
    Try very hard to extract a single JSON object from a model response.
    Handles code fences, leading text, trailing commentary, etc.
    """
    if not raw_text:
        raise ValueError("Empty model response")

    txt = raw_text.strip()

    # Strip typical fences: ```json ... ``` or ``` ...
    if txt.startswith("```"):
        # remove leading and trailing ```
        txt = re.sub(r"^```(?:json)?", "", txt, flags=re.IGNORECASE).strip()
        txt = re.sub(r"```$", "", txt).strip()

    # Find the first {...} block
    start = txt.find("{")
    end = txt.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = txt[start : end + 1]
    else:
        # As a last resort, try to balance braces quickly
        braces = 0
        start_idx = None
        for i, ch in enumerate(txt):
            if ch == "{":
                if braces == 0:
                    start_idx = i
                braces += 1
            elif ch == "}":
                braces -= 1
                if braces == 0 and start_idx is not None:
                    candidate = txt[start_idx : i + 1]
                    break
        else:
            raise ValueError("Could not locate a JSON object in model response")

    # Now parse
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as e:
        # Gentle repairs for very common issues:
        repaired = candidate

        # Remove trailing commas before } or ]
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)

        # Replace fancy quotes
        repaired = repaired.replace("“", '"').replace("”", '"').replace("’", "'")

        # Sometimes models put keys quoted twice or with stray newlines before the key
        repaired = re.sub(r'\n\s*(")', r'\1', repaired)

        try:
            return json.loads(repaired)
        except Exception as e2:
            logging.error(f"JSON parse failed. First 200 chars:\n{candidate[:200]}")
            raise ValueError(f"Failed to parse JSON from model output: {e2}") from e


def _normalize_to_headers(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Map model JSON (whatever it returns) to our exact 47 headers,
    converting everything to strings and filling missing with 'N/A'.
    """
    normalized: Dict[str, str] = {}
    for h in sheets.DEFAULT_HEADERS:
        val = data.get(h, "N/A")
        # Force string for all values
        try:
            s = str(val).strip()
            normalized[h] = s if s else "N/A"
        except Exception:
            normalized[h] = "N/A"
    return normalized


# -----------------------
# Transcription
# -----------------------

def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribe with Faster-Whisper (no unsupported kwargs).
    If language is not English, we auto-translate to English for analysis.
    """
    model_size = config["analysis"].get("whisper_model", "small")
    device = config["analysis"].get("whisper_device", "cpu")
    compute_type = "int8" if device == "cpu" else "float16"

    logging.info(f"Loading faster-whisper: {model_size} on {device}")
    model = WhisperModel(model_size, device=device, compute_type=compute_type)

    # We’ll run in translate mode so you get English text for the prompt
    logging.info("Processing audio with Faster-Whisper (task=translate, beam_size=3)")
    segments, info = model.transcribe(
        file_path,
        task="translate",
        beam_size=3,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 500},
    )

    if info and getattr(info, "language", None):
        try:
            logging.info(f"Detected language '{info.language}' with probability {getattr(info, 'language_probability', 0):.2f}")
        except Exception:
            logging.info(f"Detected language '{info.language}'")

    text_chunks = [seg.text for seg in segments]
    transcript = " ".join(text_chunks).strip()

    if not transcript:
        logging.warning("Transcription produced empty text.")
    else:
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
    return transcript


# -----------------------
# LLM Analysis
# -----------------------

def _call_gemini(prompt: str, transcript: str, model_name: str) -> str:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name)
    resp = model.generate_content(prompt.replace("{transcript}", transcript))
    # Some SDK versions expose .text, others in candidates. Handle both.
    text = getattr(resp, "text", None)
    if not text and getattr(resp, "candidates", None):
        parts = getattr(resp.candidates[0], "content", None)
        if parts and getattr(parts, "parts", None):
            text = "".join(getattr(p, "text", "") for p in parts.parts)
    if not text:
        raise ValueError("Gemini returned empty text")
    return text.strip()


def _call_openrouter(prompt: str, transcript: str, model_name: str) -> str:
    from openai import OpenAI
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not set")

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
    completion = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt.replace("{transcript}", transcript)}],
        temperature=0.2,
    )
    return completion.choices[0].message.content.strip()


def analyze_transcript(transcript: str, owner_name: str, config: dict) -> Dict[str, str]:
    """
    Call LLM (Gemini first, then OpenRouter fallback), extract JSON robustly,
    and map to the 47 headers.
    """
    if not transcript.strip():
        return _normalize_to_headers({})  # all N/A

    prompt = _load_prompt(owner_name)

    # 1) Try Gemini
    try:
        model_name = config["analysis"].get("gemini_model", "gemini-1.5-flash")
        raw = _call_gemini(prompt, transcript, model_name)
        parsed = _clean_and_parse_json(raw)
        logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
        return _normalize_to_headers(parsed)
    except Exception as e:
        logging.error(f"Gemini failed, trying OpenRouter: {e}")

    # 2) Try OpenRouter
    try:
        or_model = config["analysis"].get(
            "openrouter_model_name", "nousresearch/nous-hermes-2-mistral-7b-dpo"
        )
        raw = _call_openrouter(prompt, transcript, or_model)
        parsed = _clean_and_parse_json(raw)
        logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
        return _normalize_to_headers(parsed)
    except Exception as e2:
        logging.error(f"OpenRouter also failed. Falling back to all N/A. Error: {e2}")
        return _normalize_to_headers({})


# -----------------------
# Main File Processor
# -----------------------

def process_single_file(drive_service, gsheets, file_meta, member_name: str, config: dict):
    """
    download → transcribe → analyze → write to Sheets → move to Processed
    On failure, move to Quarantined.
    """
    from gdrive import download_file

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # Step 1: Transcribe
        transcript = transcribe_audio(local_path, config)

        # Step 2: Analyze (LLM)
        analysis_data = analyze_transcript(transcript, member_name, config)

        # Step 3: Add/override metadata
        analysis_data["Owner (Who handled the meeting)"] = member_name or "N/A"
        analysis_data["Media Link"] = file_name or "N/A"   # helps you trace source
        # (We no longer push BigQuery, so we do not add File ID/Name into sheet columns unless you add headers)

        # Step 4: Write to Google Sheets
        sheets.write_analysis_result(gsheets, analysis_data, config)

        # Step 5: Move to Processed folder
        try:
            move_to_processed(drive_service, file_id, config)
        except Exception as e_move:
            logging.warning(f"Processed successfully, but could not move to Processed: {e_move}")

        # Step 6: Ledger (success)
        try:
            sheets.update_ledger(gsheets, file_id, "Processed", "", config, file_name)
        except Exception as e_ledger:
            logging.warning(f"Processed but could not update ledger: {e_ledger}")

        logging.info(f"SUCCESS: Completed processing of {file_name}")

    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        # Quarantine + ledger on failure
        try:
            quarantine_file(drive_service, file_id, file_meta.get("parents", [""])[0] if file_meta.get("parents") else "", str(e), config)
        except Exception as qe:
            logging.error(f"Could not quarantine file {file_name}: {qe}")

        try:
            sheets.update_ledger(gsheets, file_id, "Failed", str(e), config, file_name)
        except Exception as e_ledger:
            logging.error(f"ERROR updating ledger for file {file_name}: {e_ledger}")

        # Re-raise so main can log as “Unhandled error …”
        raise
