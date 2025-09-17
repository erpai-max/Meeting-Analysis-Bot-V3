# analysis.py
import os
import json
import logging
import re
from typing import Dict, Any

import google.generativeai as genai
from faster_whisper import WhisperModel

# Local modules
import sheets

# default logger already configured by main.py; ensure logger available
logger = logging.getLogger(__name__)


# -----------------------
# Helpers
# -----------------------
def load_prompt_from_file(path: str = "prompt.txt") -> str:
    """Load the long prompt template from a file (keeps code clean)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.error("Could not load prompt.txt: %s", e)
        raise


def extract_first_json(text: str) -> Any:
    """
    Attempts to find and parse the first JSON object/array in `text`.
    Uses JSONDecoder.raw_decode to be robust against surrounding text/code fences.
    Returns the parsed Python object or raises ValueError.
    """
    if not text or not isinstance(text, str):
        raise ValueError("Empty response text")

    decoder = json.JSONDecoder()
    # Try scanning for either '{' or '[' start positions
    for i, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            obj, end = decoder.raw_decode(text[i:])
            return obj
        except Exception:
            continue

    # fallback: try to strip common markdown fences and parse entire text
    stripped = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.IGNORECASE)
    try:
        return json.loads(stripped)
    except Exception as e:
        raise ValueError(f"Could not parse JSON from model response: {e}")


def safe_json_loads_from_response(resp_text: str) -> Dict[str, Any]:
    """Return dict or raise ValueError."""
    obj = extract_first_json(resp_text)
    if isinstance(obj, dict):
        return obj
    # If model returned a list with a single dict, accept it
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj[0]
    raise ValueError("JSON parsed but not an object/dict")


def get_header_list(config: Dict) -> list:
    """Return canonical header list from sheets module or config fallback."""
    try:
        headers = getattr(sheets, "DEFAULT_HEADERS")
        if headers and isinstance(headers, list):
            return headers
    except Exception:
        pass
    # fallback to config
    return config.get("sheets_headers", [])


def find_best_value_for_key(analysis_obj: Dict[str, Any], target_key: str) -> str:
    """
    Given the model output dict and a target header name (sheet header),
    attempt multiple ways to find the value: exact match, common aliases,
    normalized keys (lower/no-punct), and short forms (Owner vs Owner (...)).
    Always returns a string (or "N/A").
    """
    # 1) direct
    if target_key in analysis_obj:
        val = analysis_obj[target_key]
        return "" if val is None else str(val)

    # 2) try a few alias transforms
    def normalize(k: str) -> str:
        return re.sub(r"[^a-z0-9]", "", k.lower())

    norm_target = normalize(target_key)

    # common alias map (keys the model might produce -> sheet header)
    common_aliases = {
        "owner": ["owner", "whohandledthemeeting", "handledby", "owner_whohandledthemeeting", "owner(whohandledthemeeting)"],
        "email": ["email", "emailid", "email_id", "email id"],
        "manager": ["manager", "mgr"],
        "meetingduration": ["meetingduration", "meeting duration (min)", "meeting duration (min)"],
        "percentscore": ["%score", "percent score", "percent_score", "percentage"]
    }

    # 3) scan keys from analysis_obj using normalization
    for k, v in analysis_obj.items():
        if normalize(k) == norm_target:
            return "" if v is None else str(v)

    # 4) try simple substring matches
    for k, v in analysis_obj.items():
        if norm_target in normalize(k) or normalize(k) in norm_target:
            return "" if v is None else str(v)

    # 5) try mapping from a few well-known keys
    for alias_list in common_aliases.values():
        for alias in alias_list:
            if alias in analysis_obj:
                return "" if analysis_obj[alias] is None else str(analysis_obj[alias])

    # not found
    return "N/A"


# -----------------------
# Transcription
# -----------------------
def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribes audio using Faster-Whisper.
    Configurable via config['analysis']['whisper_model'] and optional device/compute.
    Returns transcript string (empty string on failure).
    """
    model_size = config.get("analysis", {}).get("whisper_model", "tiny.en")
    device = config.get("analysis", {}).get("whisper_device", "cpu")
    compute_type = config.get("analysis", {}).get("whisper_compute_type", None)  # e.g., "int8"
    try:
        logger.info("Loading Whisper model: %s (device=%s, compute=%s)", model_size, device, compute_type)
        # Construct WhisperModel with optional compute_type if available
        if compute_type:
            model = WhisperModel(model_size, device=device, compute_type=compute_type)
        else:
            model = WhisperModel(model_size, device=device)
    except Exception as e:
        logger.error("Failed to load Whisper model: %s", e)
        raise

    try:
        # call transcribe; keep arguments simple for compatibility
        logger.info("Processing audio with Whisper: %s", file_path)
        segments, _ = model.transcribe(file_path, beam_size=5)
        transcript = " ".join(getattr(s, "text", str(s)) for s in segments).strip()
        logger.info("SUCCESS: Transcribed %d words.", len(transcript.split()))
        return transcript
    except TypeError as e:
        # some versions may expect different signature; try call without beam_size
        logger.warning("Transcribe call signature mismatch, retrying without beam_size: %s", e)
        try:
            segments, _ = model.transcribe(file_path)
            transcript = " ".join(getattr(s, "text", str(s)) for s in segments).strip()
            logger.info("SUCCESS: Transcribed %d words (retry).", len(transcript.split()))
            return transcript
        except Exception as e2:
            logger.error("ERROR during transcription retry: %s", e2)
            return ""
    except Exception as e:
        logger.error("ERROR during transcription: %s", e)
        return ""


# -----------------------
# AI Analysis (Gemini -> OpenRouter fallback)
# -----------------------
def analyze_transcript_with_ai(transcript: str, owner_name: str, config: dict) -> Dict[str, str]:
    """
    Call Gemini model (via google.generativeai). If it fails (quota/other), try OpenRouter via openai-compatible client.
    Returns the parsed JSON dict.
    """
    if not transcript or not transcript.strip():
        logger.warning("Empty transcript provided to AI analysis.")
        return {}

    prompt_template = load_prompt_from_file(config.get("prompt_path", "prompt.txt"))
    prompt = prompt_template.replace("{owner_name}", owner_name).replace("{transcript}", transcript)

    # Try Gemini
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if gemini_key:
        try:
            genai.configure(api_key=gemini_key)
            model_name = config.get("analysis", {}).get("gemini_model", "gemini-1.5-flash")
            logger.info("Calling Gemini model %s", model_name)
            model = genai.GenerativeModel(model_name)
            # No guarantee of response_mime_type; handle raw text
            response = model.generate_content(prompt)
            raw_text = getattr(response, "text", str(response)).strip()
            parsed = safe_json_loads_from_response(raw_text)
            logger.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
            return parsed
        except Exception as e:
            logger.error("Gemini call failed: %s", e)

    # Fallback to OpenRouter-compatible endpoint via openai client
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    openrouter_model = config.get("analysis", {}).get("openrouter_model_name")
    if openrouter_key and openrouter_model:
        try:
            logger.info("Attempting OpenRouter fallback via openai client (model=%s)", openrouter_model)
            import openai
            openai.api_key = openrouter_key
            # Use ChatCompletion for longer prompts
            resp = openai.ChatCompletion.create(
                model=openrouter_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.0,
            )
            raw_text = resp["choices"][0]["message"]["content"]
            parsed = safe_json_loads_from_response(raw_text)
            logger.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
            return parsed
        except Exception as e2:
            logger.error("OpenRouter fallback failed: %s", e2)

    # If we reach here, both failed
    logger.error("AI analysis failed for both Gemini and OpenRouter.")
    return {}


# -----------------------
# Normalize & Persist
# -----------------------
def normalize_to_sheet_headers(analysis_obj: Dict[str, Any], config: dict, owner_name: str, file_meta: Dict[str, Any]) -> Dict[str, str]:
    """
    Build a dict keyed by sheet headers (exact names) with string values.
    If sheet header not present in AI output -> "N/A"
    Also inject Owner, File Name, File ID where possible.
    """
    headers = get_header_list(config)
    normalized = {}

    for h in headers:
        # priority: exact key match, else attempt heuristics
        val = None
        # 1) direct
        if h in analysis_obj:
            val = analysis_obj[h]
        else:
            # 2) try model's commonly used keys
            val = find_best_value_for_key(analysis_obj, h)

        if val is None or (isinstance(val, str) and val.strip() == ""):
            # Accept "N/A" as per your requirement
            normalized[h] = "N/A"
        else:
            # Ensure string
            normalized[h] = str(val)

    # Ensure Owner header present (some sheets use "Owner (Who handled the meeting)")
    # Try to set Owner header if model gave "Owner" or otherwise use owner_name from folder
    owner_header_candidates = [h for h in headers if "owner" in h.lower()]
    if owner_header_candidates:
        owner_hdr = owner_header_candidates[0]
        if normalized.get(owner_hdr, "N/A") in ("N/A", ""):
            normalized[owner_hdr] = owner_name or "N/A"

    # Add Media Link / File Name / File ID fields if present as headers
    try:
        # file_meta will typically contain 'name' and 'id'
        if "Media Link" in normalized and (not normalized["Media Link"] or normalized["Media Link"] == "N/A"):
            normalized["Media Link"] = file_meta.get("name", "N/A")
    except Exception:
        pass

    # Always safe-guard required fields
    if "Date" in normalized and not normalized["Date"]:
        normalized["Date"] = "N/A"

    return normalized


# -----------------------
# Main processing entrypoint (called by main.py)
# -----------------------
def process_single_file(drive_service, gsheets_client, file_meta: dict, member_name: str, config: dict):
    """
    Orchestrates a single file processing run:
      - download via gdrive.download_file (imported at runtime)
      - transcribe
      - run AI analysis
      - write to sheet + ledger
    """
    # lazy import to avoid circular issues
    from gdrive import download_file

    file_id = file_meta.get("id")
    file_name = file_meta.get("name", "Unknown")

    try:
        logger.info("Downloading file: %s", file_name)
        local_path = download_file(drive_service, file_id, file_name)

        # Transcribe
        transcript = transcribe_audio(local_path, config)
        if not transcript:
            raise ValueError("Empty transcript")

        # AI analysis
        analysis_obj = analyze_transcript_with_ai(transcript, member_name, config)
        if not analysis_obj:
            raise ValueError("Empty analysis result")

        # Normalise and write
        normalized = normalize_to_sheet_headers(analysis_obj, config, member_name, file_meta)
        sheets.write_analysis_result(gsheets_client, normalized, config)

        # ledger: ensure update_ledger signature matches your sheets module
        try:
            sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)
        except TypeError:
            # older signature: (gsheets_client,file_id,status,error,config)
            try:
                sheets.update_ledger(gsheets_client, file_id, "Processed", "", config)
            except Exception:
                logger.exception("Could not update ledger with either signature.")

        logger.info("SUCCESS: Completed processing of %s", file_name)

    except Exception as e:
        logger.error("ERROR processing %s: %s", file_name, e)
        # write to ledger as Failed + move to quarantine handled by main
        try:
            sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        except Exception:
            logger.exception("Failed to update ledger after error.")
        # re-raise so main.py can handle quarantine
        raise
