# --- keep your imports ---
import os
import json
import logging
import sheets
from faster_whisper import WhisperModel
import google.generativeai as genai

# -------------- Transcription --------------
def transcribe_audio(file_path: str, config: dict) -> str:
    """
    Transcribes (or translates-to-English) audio using Faster-Whisper.
    """
    model_size = config["analysis"].get("whisper_model", "small")
    task = config["analysis"].get("whisper_task", "translate")   # "translate" or "transcribe"
    beam_size = int(config["analysis"].get("whisper_beam_size", 3))

    initial_prompt = (
        "Indian society management, ERP, ASP, accounting, Tally, GST, "
        "bank reconciliation, maker-checker, billing combinations, maintenance charges, "
        "late fee calculation, virtual accounts, vendor, PO/WO, preventive maintenance."
    )

    try:
        logging.info(f"Loading faster-whisper: {model_size} on cpu")
        model = WhisperModel(model_size, device="cpu")

        logging.info(f"Processing audio with Faster-Whisper (task={task}, beam_size={beam_size})")
        segments, info = model.transcribe(
            file_path,
            task=task,
            vad_filter=True,
            vad_parameters={"min_silence_duration_ms": 500},
            beam_size=beam_size,
            best_of=beam_size,
            initial_prompt=initial_prompt,
            condition_on_previous_text=False
        )

        # IMPORTANT: no raw '%' in logging format strings (use f-string instead)
        if info and getattr(info, "language", None) is not None:
            prob = float(getattr(info, "language_probability", 0.0))
            logging.info(f"Detected language '{info.language}' with probability {prob:.2f}")

        transcript_parts = []
        for s in segments:
            if getattr(s, "text", ""):
                transcript_parts.append(s.text.strip())

        transcript = " ".join(transcript_parts).strip()
        logging.info(f"SUCCESS: Transcribed {len(transcript.split())} words.")
        return transcript
    except TypeError as te:
        logging.error(f"Transcription TypeError (retrying minimal): {te}")
        try:
            segments, info = model.transcribe(file_path, task=task)
            transcript = " ".join(s.text.strip() for s in segments if getattr(s, "text", "").strip())
            logging.info(f"SUCCESS (retry minimal): {len(transcript.split())} words.")
            return transcript
        except Exception as e2:
            logging.error(f"Retry transcription failed: {e2}")
            return ""
    except Exception as e:
        logging.error(f"ERROR during transcription: {e}")
        return ""


# -------------- AI Analysis --------------
def analyze_transcript(transcript: str, config: dict) -> dict:
    """Calls Gemini (fallback OpenRouter) to analyze transcript and return structured JSON."""
    if not transcript.strip():
        return {}

    # Load prompt text from file (prompt.txt sits next to the script)
    try:
        with open("prompt.txt", "r", encoding="utf-8") as f:
            prompt_template = f.read()
    except Exception:
        # Safe fallback if file not found
        prompt_template = (
            "You are a sales meeting analyst. Output JSON only with the requested keys. "
            "If something is missing, use 'N/A'.\n\n{transcript}"
        )

    prompt = prompt_template.format(owner_name=config.get("owner_name", "N/A"), transcript=transcript)

    # Try Gemini first
    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))
        response = model.generate_content(prompt)
        raw_text = (response.text or "").strip()

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

    # Normalize to sheet headers (fill N/A for any missing)
    normalized = {}
    for h in sheets.DEFAULT_HEADERS:
        val = analysis_data.get(h, "")
        normalized[h] = str(val if val not in (None, "", []) else "N/A")

    return normalized


# -------------- Main per-file pipeline --------------
def process_single_file(drive_service, gsheets_client, file_meta, member_name: str, config: dict):
    """
    Processes one file: download → normalize → transcribe → analyze → write to Sheets → move to Processed.
    """
    from gdrive import download_file, move_to_processed

    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown")

    try:
        logging.info(f"Downloading file: {file_name}")
        local_path = download_file(drive_service, file_id, file_name)

        # (Optional) normalize with ffmpeg if your main.py doesn't already normalize
        # If main.py already does ffmpeg, you can remove the following two lines.
        # from gdrive import normalize_audio_16k
        # local_path = normalize_audio_16k(local_path)

        # Step 1: Transcription
        transcript = transcribe_audio(local_path, config)

        # Step 2: AI Analysis
        analysis_data = analyze_transcript(transcript, config)
        if not analysis_data:
            raise ValueError("Empty analysis result")

        # Step 3: Add metadata
        analysis_data["Owner (Who handled the meeting)"] = analysis_data.get("Owner (Who handled the meeting)", "N/A") or member_name
        analysis_data["Media Link"] = file_name
        analysis_data["Manager Email"] = analysis_data.get("Manager Email", "N/A")

        # Step 4: Write to Google Sheets
        sheets.write_analysis_result(gsheets_client, analysis_data, config)

        # Step 5: Update ledger & move to processed
        sheets.update_ledger(gsheets_client, file_id, "Processed", "", config, file_name)

        try:
            move_to_processed(drive_service, file_id, config)
        except Exception as move_err:
            logging.error(f"Could not move file to Processed folder: {move_err}")

        logging.info(f"SUCCESS: Completed processing of {file_name}")
    except Exception as e:
        logging.error(f"ERROR processing {file_name}: {e}")
        from gdrive import quarantine_file
        try:
            quarantine_file(drive_service, file_id, "", str(e), config)
        except Exception as qe:
            logging.error(f"ERROR quarantining file {file_name}: {qe}")
        sheets.update_ledger(gsheets_client, file_id, "Failed", str(e), config, file_name)
        raise
