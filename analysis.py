# --- Quiet gRPC/absl logs BEFORE importing Google/gRPC libraries ---
import os
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_CPP_VERBOSITY", "NONE")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3")

import io
import re
import json
import logging
import time # Import the time module for the sleep function
import datetime as dt
from typing import Dict, Any, Tuple, List, Set, Optional

import google.generativeai as genai
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import local modules for a self-contained analysis script
import gdrive
import sheets

# =========================
# Exceptions
# =========================
class QuotaExceeded(Exception):
    """Raised when Gemini quota/rate-limit is hit."""
    pass

# =========================
# Quota detection helper
# =========================
def _is_quota_error(e: Exception) -> bool:
    msg = str(e).lower()
    keywords = ["quota", "rate limit", "resourceexhausted", "429"]
    return any(k in msg for k in keywords)

# =========================
# Constants & Utility Helpers
# =========================
DEFAULT_MODEL_NAME = "gemini-1.5-flash"

def _init_gemini():
    """Initializes the Gemini client with the API key from environment variables."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("CRITICAL: GEMINI_API_KEY environment variable is not set.")
    genai.configure(api_key=api_key)
    logging.info("Gemini API configured successfully.")

def _get_model(config: Dict[str, Any]) -> str:
    """Gets the model name from the config, with a fallback to the default."""
    return config.get("google_llm", {}).get("model", DEFAULT_MODEL_NAME)

def _load_master_prompt(config: Dict[str, Any]) -> str:
    """Loads the main analysis prompt from prompt.txt."""
    prompt_path = os.path.join(os.getcwd(), "prompt.txt")
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    logging.warning("prompt.txt not found. Using a generic fallback prompt.")
    return "Act as an expert business analyst and extract insights from the provided transcript."

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=2, min=2, max=20),
       retry=retry_if_exception_type((QuotaExceeded, RuntimeError)))
def _gemini_one_shot(file_path: str, mime_type: str, master_prompt: str, model_name: str) -> Dict[str, Any]:
    """
    Uploads a file, waits for it to become ACTIVE, then generates content in a single call.
    This is the robust method to avoid race conditions and API errors like 'ragStoreName'.
    """
    try:
        model = genai.GenerativeModel(model_name)
        
        logging.info(f"Uploading file '{os.path.basename(file_path)}' to Gemini API...")
        uploaded_file = genai.upload_file(path=file_path, mime_type=mime_type)
        
        # --- FIX FOR "ragStoreName" and "not ACTIVE" ERRORS ---
        # The file upload is asynchronous. We must wait for the file to be 'ACTIVE'
        # before we can use it. This loop checks the status.
        logging.info(f"File uploaded. State: {uploaded_file.state.name}. Waiting for it to become ACTIVE...")
        timeout_seconds = 300 # 5 minute timeout
        start_time = time.time()
        while uploaded_file.state.name == "PROCESSING":
            if time.time() - start_time > timeout_seconds:
                raise RuntimeError(f"File '{uploaded_file.name}' was stuck in PROCESSING state for over {timeout_seconds} seconds.")
            time.sleep(10) # Wait for 10 seconds before checking again
            uploaded_file = genai.get_file(uploaded_file.name)
            logging.info(f"File state: {uploaded_file.state.name}")

        if uploaded_file.state.name != "ACTIVE":
            raise RuntimeError(f"File processing failed on Google's servers with final state: {uploaded_file.state.name}")
        # --- END OF FIX ---

        logging.info("File is ACTIVE. Generating content...")
        resp = model.generate_content(
            [uploaded_file, {"text": master_prompt}],
            generation_config={
                "temperature": 0.2,
                "response_mime_type": "application/json",
            },
        )

        if getattr(resp, "prompt_feedback", None) and getattr(resp.prompt_feedback, "block_reason", None):
            raise RuntimeError(f"Prompt blocked by safety settings: {resp.prompt_feedback.block_reason}")
            
        raw = (resp.text or "").strip().strip("` ").removeprefix("json").lstrip(":").strip()
        data = json.loads(raw)
        
        if not isinstance(data, dict):
            raise RuntimeError("AI model output was not a valid JSON object.")
            
        return data
        
    except json.JSONDecodeError as je:
        raise RuntimeError(f"Failed to parse JSON from model: {je}") from je
    except Exception as e:
        if _is_quota_error(e):
            logging.error("Quota exceeded during ONE-SHOT call.")
            raise QuotaExceeded(str(e))
        # Re-raise other exceptions to be handled by the main loop (e.g., PERMISSION_DENIED)
        raise

def _augment_with_manager_info(analysis_obj: Dict[str, Any], member_name: str, config: Dict[str, Any]) -> None:
    """Fills in Owner, Manager, Team, and Email details from the config.yaml manager_map."""
    analysis_obj["Owner (Who handled the meeting)"] = member_name
    manager_map = config.get("manager_map", {})
    member_details = manager_map.get(member_name, {})
    
    analysis_obj["Manager"] = member_details.get("Manager", "N/A")
    analysis_obj["Team"] = member_details.get("Team", "N/A")
    analysis_obj["Email Id"] = member_details.get("Email", "N/A")

    manager_emails = config.get("manager_emails", {})
    manager_email = manager_emails.get(analysis_obj["Manager"], "N/A")
    analysis_obj["Manager Email"] = manager_email

def process_single_file(drive_service, gsheets_sheet, file_meta: Dict[str, Any], member_name: str, config: Dict[str, Any]):
    """
    Orchestrates the download, analysis, and result logging for a single media file.
    This function is called by main.py for each new file found.
    """
    _init_gemini()
    file_id = file_meta["id"]
    file_name = file_meta.get("name", "Unknown Filename")
    mime_type = file_meta.get("mimeType", "")
    
    logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")
    
    local_path = ""
    try:
        # Step 1: Download the file from Google Drive
        # NOTE: The "PERMISSION_DENIED" error in your log indicates the Google Drive API
        # might not be enabled in your Google Cloud project for the service account being used.
        # Please ensure the 'Google Drive API' is enabled.
        local_path = gdrive.download_file(drive_service, file_id, file_name)
        
        # Step 2: Prepare the prompt and model name
        master_prompt = _load_master_prompt(config).format(owner_name=member_name)
        model_name = _get_model(config)

        # Step 3: Call the updated, robust Gemini function
        analysis_obj = _gemini_one_shot(local_path, mime_type, master_prompt, model_name)

        # Step 4: Enrich the AI output with internal data
        _augment_with_manager_info(analysis_obj, member_name, config)
        
        # You can add other enrichments here if needed (e.g., duration, date)

        # Step 5: Write the final, complete result to Google Sheets
        sheets.write_analysis_result(gsheets_sheet, analysis_obj, config)

        logging.info(f"SUCCESS: Finished processing and saved results for {file_name}")

    finally:
        # Step 6: Clean up the downloaded file from the /tmp directory
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logging.info(f"Cleaned up temporary file: {local_path}")
            except OSError as e:
                logging.error(f"Error cleaning up temporary file {local_path}: {e}")

