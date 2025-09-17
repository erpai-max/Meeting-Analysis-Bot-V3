import os
import re
import json
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime
from faster_whisper import WhisperModel
import google.generativeai as genai

import sheets
from gdrive import move_to_processed, quarantine_file


# ----------------------------------
# Prompt loader
# ----------------------------------
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


# ----------------------------------
# JSON cleaning / parsing helpers
# ----------------------------------
def _clean_and_parse_json(raw_text: str) -> Dict[str, Any]:
    """
    Extract a single JSON object from an LLM response.
    Handles code fences, extra commentary, trailing commas, fancy quotes, etc.
    """
    if not raw_text:
        raise ValueError("Empty model response")

    txt = raw_text.strip()

    # Strip code fences like ```json ... ```
    if txt.startswith("```"):
        txt = re.sub(r"^```(?:json)?", "", txt, flags=re.IGNORECASE).strip()
        txt = re.sub(r"```$", "", txt).strip()

    # Prefer the largest {...} block
    start = txt.find("{")
    end = txt.rfind("}")
    candidate = None
    if start != -1 and end != -1 and end > start:
        candidate = txt[start : end + 1]
    else:
        # Try to balance braces greedily
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

    if not candidate:
        raise ValueError("Could not locate a JSON object in model response")

    # First parse attempt
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        # Gentle repairs
        repaired = candidate

        # remove trailing commas before } or ]
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)

        # replace fancy quotes
        repaired = repaired.replace("“", '"').replace("”", '"').replace("’", "'")

        # remove stray newlines before opening quotes
        repaired = re.sub(r'\n\s*(")', r'\1', repaired)

        # sometimes percent fields come as numbers; leave that to normalization

        try:
            return json.loads(repaired)
        except Exception as e2:
            logging.error("JSON parse failed. First 300 chars:\n" + candidate[:300])
            raise ValueError(f"Failed to parse JSON from model output: {e2}") from e2


# ----------------------------------
# Filename heuristics (useful fallbacks)
# ----------------------------------
def _infer_from_filename(file_name: str) -> Dict[str, str]:
    """
    Guess some fields using common naming patterns like:
    'Vasudha l DEMO l Ahmedabad l 24-08-25 l.mp3'
    """
    out: Dict[str, str] = {}
    if not file_name:
        return out

    stem = os.path.splitext(file_name)[0]

    # Normalize separators: treat '|', '-', '_', ' l ' as boundaries
    parts = re.split(r"\s*(?:\||-|–|—|_| l )\s*", stem)
    parts = [p.strip() for p in parts if p.strip()]

    # Society Name -> very often first chunk
    if parts:
        out["Society Name"] = parts[0]

    joined = " ".join(parts).lower()

    # Meeting type hints
    if "demo" in joined:
        out["Meeting Type"] = "ERP Pitch"
    if "training" in joined or "support" in joined:
        out["Meeting Type"] = "Training / Issue Resolution"
    if "renewal" in joined or "commercial" in joined or "commercials" in joined:
        out["Meeting Type"] = "Renewal / Commercials"

    # Visit type hints (very rough)
    if "zoom" in joined or "meet" in joined or "google meet" in joined:
        out["Visit Type"] = "Virtual"
    elif "call" in joined or "phone" in joined:
        out["Visit Type"] = "Phone"
    elif "demo" in joined:
        # default guess
        out["Visit Type"] = out.get("Visit Type", "Onsite")

    # Date (DD-MM-YY/YYYY or DD_MM_YYYY or 31-08-25)
    m = re.search(r"(\d{1,2})[-_/](\d{1,2})[-_/](\d{2,4})", stem)
    if m:
        d, mo, y = m.groups()
        if len(y) == 2:
            y = "20" + y
        try:
            dt = datetime(int(y), int(mo), int(d))
            out["Date"] = dt.strftime("%Y-%m-%d")
        except Exception:
            # leave as N/A if invalid
            pass

    return out


# ----------------------------------
# Score utilities
# ----------------------------------
SCORE_KEYS = [
    "Opening Pitch Score",
    "Product Pitch Score",
    "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness",
    "Negotiation Strength",
]

def _to_int_or_none(x: Any) -> int | None:
    try:
        s = str(x).strip()
        if not s or s.upper() == "N/A":
            return None
        # remove trailing % if someone put that
        s = s.replace("%", "")
        n = int(float(s))
        return n
    except Exception:
        return None

def _sanitize_and_compute_scores(row: Dict[str, str]) -> None:
    """
    Ensure scores are valid 1..10 strings, default to '2' if missing,
    compute Total Score and % Score strings.
    """
    fixed: List[int] = []
    for k in SCORE_KEYS:
        val = _to_int_or_none(row.get(k, ""))
        if val is None:
            val = 2  # your rule: minimum if missing/unclear
        # clamp
        val = max(1, min(10, val))
        row[k] = str(val)
        fixed.append(val)

    total = sum(fixed)
    row["Total Score"] = str(total)

    pct = round((total / 50.0) * 100.0, 1)
    # normalize as string with percent sign
    row["% Score"] = f"{pct}%"

    # Overall Sentiment default if missing
    if not row.get("Overall Sentiment") or row["Overall Sentiment"].strip().upper() == "N/A":
        # crude heuristic: choose Neutral if no strong signal
        row["Overall Sentiment"] = "Neutral"


# ----------------------------------
# Config helpers (Manager mapping)
# ----------------------------------
def _fill_manager_info(row: Dict[str, str], owner_name: str, config: dict) -> None:
    """
    Fill Manager, Team, Manager Email from config.manager_map if available.
    """
    try:
        mgr_map: Dict[str, Dict[str, str]] = config.get("manager_map", {})
        # case-insensitive key match
        key = None
        for k in mgr_map.keys():
            if k.lower() == (owner_name or "").lower():
                key = k
                break

        if key:
            info = mgr_map[key]
            if not row.get("Manager") or row["Manager"].strip().upper() == "N/A":
                row["Manager"] = str(info.get("Manager", "N/A") or "N/A")
            if not row.get("Team") or row["Team"].strip().upper() == "N/A":
                row["Team"] = str(info.get("Team", "N/A") or "N/A")
            if not row.get("Manager Email") or row["Manager Email"].strip().upper() == "N/A":
                row["Manager Email"] = str(info.get("Email", "N/A") or "N/A")
    except Exception as e:
        logging.debug(f"Manager map fill skipped: {e}")


# ----------------------------------
# Normalize to 47 headers
# ----------------------------------
def _normalize_to_headers(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Map arbitrary model JSON to our exact 47 headers.
    Convert everything to strings and fill missing with 'N/A'.
    """
    normalized: Dict[str, str] = {}
    for h in sheets.DEFAULT_HEADERS:
        val = data.get(h, "N/A")
        try:
            s = str(val).strip()
            normalized[h] = s if s else "N/A"
        except Exception:
            normalized[h] = "N/A"
    return normalized


# ----------------------------------
# Transcription
# ----------------------------------
def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribe with Faster-Whisper and auto-translate to English.
    """
    model_size = config["analysis"].get("whisper_model", "small")
    device = config["analysis"].get("whisper_device", "cpu")
    compute_type = "int8" if device == "cpu" else "float16"

    logging.info(f"Loading faster-whisper: {model_size} on {device}")
    model = WhisperModel(model_size, device=device, compute_type=compute_type)

    # Translate → English so the prompt is stable across languages
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
            logging.info(
                f"Detected language '{info.language}' with probability "
                f"{getattr(info, 'language_probability', 0):.2f}"
            )
        except Exception:
            logging.info(f"Detected language '{info.language}'")

    text_chunks = [seg.text for seg in segments]
    transcript = " ".join(text_chunks).strip()

    if not transcript:
        logging.warning("Transcription produced empty text.")
    else:
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
    return transcript


# ----------------------------------
# LLM calls
# ----------------------------------
def _call_gemini(prompt: str, transcript: str, model_name: str) -> str:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name)
    resp = model.generate_content(prompt.replace("{transcript}", transcript))
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
    Use Gemini (fallback OpenRouter) → robust JSON parse →
    normalize to 47 headers → repair/compute scores → enrich metadata.
    """
    if not transcript.strip():
        # Empty transcript → all N/A, but still compute scores (they’ll become min)
        row = _normalize_to_headers({})
        _sanitize_and_compute_scores(row)
        return row

    prompt = _load_prompt(owner_name)

    # 1) Gemini first
    parsed: Dict[str, Any] | None = None
    try:
        model_name = config["analysis"].get("gemini_model", "gemini-1.5-flash")
        raw = _call_gemini(prompt, transcript, model_name)
        parsed = _clean_and_parse_json(raw)
        logging.info("SUCCESS: Parsed AI analysis JSON output (Gemini).")
    except Exception as e:
        logging.error(f"Gemini failed, trying OpenRouter: {e}")

    # 2) Fallback to OpenRouter
    if parsed is None:
        try:
            or_model = config["analysis"].get(
                "openrouter_model_name", "nousresearch/nous-hermes-2-mistral-7b-dpo"
            )
            raw = _call_openrouter(prompt, transcript, or_model)
            parsed = _clean_and_parse_json(raw)
            logging.info("SUCCESS: Parsed AI analysis JSON output (OpenRouter).")
        except Exception as e2:
            logging.error(f"OpenRouter also failed. Returning all N/A. Error: {e2}")
            row = _normalize_to_headers({})
            _sanitize_and_compute_scores(row)
            return row

    # Normalize to headers
    row = _normalize_to_headers(parsed)

    # Enrich / repair
    # Owner name (force)
    if not row.get("Owner (Who handled the meeting)") or row["Owner (Who handled the meeting)"].strip().upper() == "N/A":
        row["Owner (Who handled the meeting)"] = owner_name or "N/A"

    # Filename heuristics for Society/Type/Date/etc.
    # (We’ll fill this at process_single_file stage because we know file_name there.)

    # Fix/compute scores (+ Total, %)
    _sanitize_and_compute_scores(row)

    # Ensure % fields look like percent
    pct_s = row.get("% Score", "").strip()
    if pct_s and not pct_s.endswith("%"):
        try:
            # if it's a number string, append %
            float(pct_s)
            row["% Score"] = pct_s + "%"
        except Exception:
            # leave as-is
            pass

    return row


# ----------------------------------
# Main: process a single file
# ----------------------------------
def process_single_file(drive_service, gsheets, file_meta, member_name: str, config: dict):
    """
    download → transcribe → analyze → fill fallbacks → write to Sheets → move to Processed
    On failure, move to Quarantined and log to ledger.
    """
    from gdrive import download_file

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # Step 1: Transcription (multilingual → English)
        transcript = transcribe_audio(local_path, config)

        # Step 2: LLM analysis
        row = analyze_transcript(transcript, member_name, config)

        # Step 3: Fallbacks from filename (fill only where N/A)
        inferred = _infer_from_filename(file_name)
        for k, v in inferred.items():
            if row.get(k, "N/A").strip().upper() in ("", "N/A"):
                row[k] = v

        # Step 4: Manager/Team details from config
        _fill_manager_info(row, member_name, config)

        # Extra traceability in sheet
        row["Media Link"] = file_name or "N/A"

        # Step 5: Write to Google Sheets
        sheets.write_analysis_result(gsheets, row, config)

        # Step 6: Move to Processed (best-effort retries are inside move_to_processed)
        try:
            move_to_processed(drive_service, file_id, config)
        except Exception as e_move:
            logging.warning(f"Processed successfully, but could not move to Processed: {e_move}")

        # Step 7: Ledger success
        try:
            sheets.update_ledger(gsheets, file_id, "Processed", "", config, file_name)
        except Exception as e_ledger:
            logging.warning(f"Processed but could not update ledger: {e_ledger}")

        logging.info(f"SUCCESS: Completed processing of {file_name}")

    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")

        # Quarantine (best-effort)
        try:
            quarantine_file(
                drive_service,
                file_id,
                file_meta.get("parents", [""])[0] if file_meta.get("parents") else "",
                str(e),
                config,
            )
        except Exception as qe:
            logging.error(f"Could not quarantine file {file_name}: {qe}")

        # Ledger failure
        try:
            sheets.update_ledger(gsheets, file_id, "Failed", str(e), config, file_name)
        except Exception as e_ledger:
            logging.error(f"ERROR updating ledger for file {file_name}: {e_ledger}")

        # Re-raise so main logs “Unhandled error …”
        raise
