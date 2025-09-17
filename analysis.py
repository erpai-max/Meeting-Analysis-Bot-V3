import os
import json
import uuid
import logging
from typing import Dict, Any

from faster_whisper import WhisperModel

# Local modules
import sheets


# =======================
# JSON helpers
# =======================
def _extract_json_object(text: str) -> str | None:
    """
    Extract the first balanced JSON object from text.
    Handles code fences and ignores any prose around it.
    """
    if not text:
        return None

    t = text.strip()
    if t.startswith("```"):
        # Strip triple backticks block
        t = t.strip("`").strip()
        if t.lower().startswith("json"):
            t = t[4:].lstrip()

    start = t.find("{")
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(t)):
        c = t[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return t[start : i + 1]
    return None  # no balanced block found


def _coerce_score(val: str, default_min: int = 2) -> int:
    """Cast score to int in [1..10], with a minimum default if empty/invalid."""
    try:
        n = int(str(val).strip())
    except Exception:
        n = default_min
    if n < default_min:
        n = default_min
    if n > 10:
        n = 10
    return n


def _compute_totals(row: Dict[str, str]) -> None:
    """Compute Total Score and % Score from the five component scores."""
    s1 = _coerce_score(row.get("Opening Pitch Score", ""))
    s2 = _coerce_score(row.get("Product Pitch Score", ""))
    s3 = _coerce_score(row.get("Cross-Sell / Opportunity Handling", ""))
    s4 = _coerce_score(row.get("Closing Effectiveness", ""))
    s5 = _coerce_score(row.get("Negotiation Strength", ""))
    total = s1 + s2 + s3 + s4 + s5
    row["Total Score"] = str(total)
    row["% Score"] = f"{(total / 50.0) * 100:.2f}%"


# =======================
# Transcription
# =======================
def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribes audio using faster-whisper.
    - Uses VAD noise trimming
    - If translate_non_english=True, runs task='translate' to English
    """
    model_size = config.get("analysis", {}).get("whisper_model", "small")
    device = config.get("analysis", {}).get("whisper_device", "cpu")
    compute_type = config.get("analysis", {}).get("whisper_compute_type", "auto")
    translate_non_english = config.get("analysis", {}).get("translate_non_english", True)

    try:
        logging.info(f"Loading faster-whisper: {model_size} on {device}")
        model = WhisperModel(model_size, device=device, compute_type=compute_type)

        # Two-pass: detect language quickly, then run full with chosen task
        segments, info = model.transcribe(
            file_path,
            vad_filter=True,
            beam_size=3,
            task="transcribe",  # detect first pass implies task doesn't matter here
        )
        # If non-English and allowed, use translate on a second run
        task = "translate" if (translate_non_english and info and info.language and info.language != "en") else "transcribe"
        if task == "translate":
            logging.info(f"Detected language '{info.language}' (p≈{getattr(info, 'language_probability', 0):.2f}); re-running with task=translate")
            segments, info = model.transcribe(
                file_path,
                vad_filter=True,
                beam_size=3,
                task="translate",
            )

        words = []
        for seg in segments:
            if seg and getattr(seg, "text", ""):
                words.append(seg.text.strip())
        transcript = " ".join(words).strip()

        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
        return transcript
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return ""


# =======================
# AI Analysis
# =======================
def analyze_transcript(transcript: str, config: dict, owner_name: str = "") -> Dict[str, str]:
    """
    Calls Gemini (fallback OpenRouter) to analyze transcript and return a dict
    containing ALL headers from sheets.DEFAULT_HEADERS.

    - Robust JSON extraction (balanced braces)
    - Saves raw AI output to /tmp if parsing fails
    - Fills missing fields as "N/A"
    - Computes Total Score and % Score
    """
    DEFAULT_HEADERS = sheets.DEFAULT_HEADERS  # single source of truth

    # Empty transcript → return fully N/A row but with owner & basic defaults
    if not transcript or not transcript.strip():
        row = {h: "N/A" for h in DEFAULT_HEADERS}
        if owner_name:
            row["Owner (Who handled the meeting)"] = owner_name
        if row.get("Overall Sentiment", "N/A") in ("", "N/A"):
            row["Overall Sentiment"] = "Neutral"
        _compute_totals(row)
        return row

    # Build prompt
    prompt_path = config.get("analysis", {}).get("prompt_path", "prompt.txt")
    try:
        with open(prompt_path, "r", encoding="utf-8") as f:
            base_prompt = f.read()
    except Exception:
        base_prompt = (
            "Return only one JSON object with the required fields. "
            "If a value is unknown, use 'N/A'."
        )
    prompt = base_prompt.format(owner_name=owner_name, transcript=transcript)

    raw_text = ""

    # --- Try Gemini first ---
    try:
        import google.generativeai as genai

        gemini_key = os.environ.get("GEMINI_API_KEY")
        if not gemini_key:
            raise RuntimeError("GEMINI_API_KEY not set")

        genai.configure(api_key=gemini_key)
        model_name = config.get("analysis", {}).get("gemini_model", "gemini-1.5-flash")
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(prompt)
        raw_text = (resp.text or "").strip()
    except Exception as e:
        logging.error(f"Gemini failed: {e}")

    # --- Fallback to OpenRouter if Gemini is empty/failed ---
    if not raw_text:
        try:
            # Prefer new-style OpenAI client
            try:
                from openai import OpenAI

                client = OpenAI(
                    api_key=os.environ.get("OPENROUTER_API_KEY"),
                    base_url="https://openrouter.ai/api/v1",
                )
                or_model = config.get("analysis", {}).get(
                    "openrouter_model_name", "nousresearch/nous-hermes-2-mistral-7b-dpo"
                )
                resp = client.chat.completions.create(
                    model=or_model,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_text = (resp.choices[0].message.content or "").strip()
            except Exception:
                # Legacy fallback
                import openai as openai_legacy

                openai_legacy.api_key = os.environ.get("OPENROUTER_API_KEY")
                openai_legacy.base_url = "https://openrouter.ai/api/v1"
                or_model = config.get("analysis", {}).get(
                    "openrouter_model_name", "nousresearch/nous-hermes-2-mistral-7b-dpo"
                )
                resp = openai_legacy.ChatCompletion.create(
                    model=or_model,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_text = resp["choices"][0]["message"]["content"].strip()
        except Exception as e2:
            logging.error(f"OpenRouter failed: {e2}")

    # --- Parse JSON; if it fails, save raw and proceed with N/A row ---
    parsed = None
    if raw_text:
        try:
            snippet = _extract_json_object(raw_text)
            if snippet:
                parsed = json.loads(snippet)
            else:
                raise ValueError("No balanced JSON object found in AI output.")
        except Exception as e:
            fn = f"/tmp/ai_raw_{uuid.uuid4().hex}.txt"
            try:
                with open(fn, "w", encoding="utf-8") as f:
                    f.write(raw_text)
                logging.warning(f"AI JSON parse failed ({e}). Raw saved to {fn}")
            except Exception:
                logging.warning(f"AI JSON parse failed ({e}). Raw could not be saved.")
            parsed = None

    # --- Normalize to required headers ---
    row = {h: "N/A" for h in DEFAULT_HEADERS}
    if isinstance(parsed, dict):
        for h in DEFAULT_HEADERS:
            row[h] = str(parsed.get(h, "N/A") or "N/A")

    # Ensure owner name present
    if owner_name and row.get("Owner (Who handled the meeting)", "N/A") in ("", "N/A"):
        row["Owner (Who handled the meeting)"] = owner_name

    # Default sentiment if missing
    if row.get("Overall Sentiment", "N/A") in ("", "N/A"):
        row["Overall Sentiment"] = "Neutral"

    # Compute/repair totals
    _compute_totals(row)

    return row


# =======================
# Main Single-File Pipeline
# =======================
def process_single_file(drive_service, gsheets_client, file_meta: Dict[str, Any], member_name: str, config: dict):
    """
    Process one file: download → transcribe → analyze → save to Sheets.
    - Always updates ledger (Processed/Failed), raising on failure so caller can quarantine.
    """
    from gdrive import download_file  # local import to avoid cycles

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # 1) Transcription
        transcript = transcribe_audio(local_path, config)

        # 2) AI Analysis (returns all headers with N/A where missing)
        analysis_row = analyze_transcript(transcript, config, owner_name=member_name)

        # 3) Enrich with known metadata from config
        #    Try to fill Team/Manager/Manager Email from config if they're still N/A
        mgr_map = config.get("manager_map", {})
        if member_name in mgr_map:
            if analysis_row.get("Team", "N/A") in ("", "N/A"):
                analysis_row["Team"] = mgr_map[member_name].get("Team", "N/A")
            if analysis_row.get("Manager", "N/A") in ("", "N/A"):
                analysis_row["Manager"] = mgr_map[member_name].get("Manager", "N/A")
            if analysis_row.get("Manager Email", "N/A") in ("", "N/A"):
                analysis_row["Manager Email"] = mgr_map[member_name].get("Email", "N/A")

        # 4) Fill Media Link with file name if still empty
        if analysis_row.get("Media Link", "N/A") in ("", "N/A"):
            analysis_row["Media Link"] = file_name

        # 5) Write to Google Sheets
        sheets.write_analysis_result(gsheets_client, analysis_row, config)

        # 6) Ledger success
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        # Ledger failure (main will handle quarantine move)
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        raise
