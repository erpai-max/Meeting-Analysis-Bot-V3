# --- Quiet gRPC/absl logs BEFORE importing Google/gRPC libraries ---
import os
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_CPP_VERBOSITY", "NONE")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3")

import yaml
import logging
import json
import sys
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

import gdrive
import analysis
import sheets
# Import QuotaExceeded specifically for stop-on-quota handling
try:
    from analysis import QuotaExceeded
except ImportError:
    # Define a dummy exception if analysis module might not be fully loaded yet
    class QuotaExceeded(Exception):
        pass
    logging.warning("Could not import QuotaExceeded from analysis module initially.")


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_google(config):
    """Authenticate Drive + Sheets. Return (drive_service, Spreadsheet)."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("GCP_SA_KEY environment variable not set")

        creds_info = json.loads(gcp_key_str)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        drive_service = build("drive", "v3", credentials=creds)
        logging.info("SUCCESS: Authenticated Google Drive")

        client = gspread.authorize(creds)
        sheet = client.open_by_key(config["google_sheets"]["sheet_id"])
        # Ensure tabs exist early
        sheets.ensure_tabs_exist(sheet, config)
        logging.info("SUCCESS: Authenticated Google Sheets")

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, sheet
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}", exc_info=True)
        return None, None

def export_data_for_dashboard(gsheets_sheet, config):
    """Exports data from the results tab to JSON for the dashboard."""
    # Check if dashboard export is enabled in config
    dashboard_config = config.get("dashboard", {})
    if not dashboard_config or not isinstance(dashboard_config, dict):
        logging.info("Dashboard configuration missing or invalid. Skipping export.")
        return

    output_dir = dashboard_config.get("output_dir", "docs")
    filename = dashboard_config.get("filename", "dashboard_data.json")
    output_path = os.path.join(output_dir, filename)

    logging.info(f"Exporting latest data for the dashboard to {output_path}...")
    try:
        all_records = sheets.get_all_results(gsheets_sheet, config)

        # Apply column stripping if configured
        strip_columns = dashboard_config.get("strip_columns", [])
        if strip_columns and isinstance(strip_columns, list):
            logging.info(f"Stripping columns before export: {', '.join(strip_columns)}")
            cleaned_records = []
            for record in all_records:
                cleaned_record = {k: v for k, v in record.items() if k not in strip_columns}
                cleaned_records.append(cleaned_record)
            all_records = cleaned_records # Use the cleaned data

        os.makedirs(output_dir, exist_ok=True) # Ensure output directory exists

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=2, ensure_ascii=False)
        logging.info(f"SUCCESS: Exported {len(all_records)} records to {output_path}.")

        # Optionally copy dashboard HTML
        if dashboard_config.get("copy_html_from_root", False):
            try:
                import shutil
                source_html = "dashboard.html"
                dest_html = os.path.join(output_dir, "index.html")
                if os.path.exists(source_html):
                    shutil.copyfile(source_html, dest_html)
                    logging.info(f"Copied {source_html} to {dest_html}")
                else:
                    logging.warning(f"{source_html} not found in root, cannot copy.")
            except Exception as copy_e:
                logging.error(f"Error copying dashboard HTML: {copy_e}")

    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}", exc_info=True)

def retry_quarantined_files(drive_service, gsheets_sheet, config):
    """Move quarantined files back for retry after cool-off window."""
    logging.info("Checking quarantined files for retry...")
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]
        parent_id = config["google_drive"]["parent_folder_id"] # Move back to main parent
        hours = int(config.get("quarantine", {}).get("auto_retry_after_hours", 24))
        cooloff_secs = hours * 3600

        if cooloff_secs <= 0:
            logging.info("Auto-retry for quarantined files is disabled (hours <= 0).")
            return

        # Query for files directly in the quarantine folder
        files = drive_service.files().list(
            q=f"'{quarantine_id}' in parents and trashed=false",
            fields="files(id, name, modifiedTime, parents)" # Use modifiedTime as proxy for quarantine time
        ).execute().get("files", [])

        now_epoch = time.time()

        for file in files:
            modified_time_str = file.get("modifiedTime") # Drive API uses modifiedTime
            file_id = file["id"]
            file_name = file["name"]

            if modified_time_str:
                try:
                    # Parse RFC 3339 timestamp (handle potential Z and fractional seconds)
                    modified_dt = dt.datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                    modified_epoch = modified_dt.timestamp()

                    if (now_epoch - modified_epoch) > cooloff_secs:
                        logging.info(f"Retrying quarantined file (past {hours}h cool-off): {file_name} (ID: {file_id})")
                        try:
                            # Move from quarantine back to the main parent folder
                            gdrive.move_file(drive_service, file_id, quarantine_id, parent_id)
                            # Update ledger to reflect the retry attempt
                            sheets.update_ledger(gsheets_sheet, file_id, "Pending", # Reset status
                                                 f"Moved back for auto-retry after {hours}h", config, file_name)
                        except Exception as move_e:
                            logging.error(f"ERROR: Could not move quarantined file {file_name} back for retry: {move_e}")
                except ValueError as parse_e:
                     logging.warning(f"Could not parse modifiedTime '{modified_time_str}' for file {file_name}: {parse_e}")
            else:
                 logging.warning(f"File {file_name} in quarantine has no modifiedTime, cannot check cool-off.")

    except Exception as e:
        logging.error(f"ERROR while retrying quarantined files: {e}", exc_info=True)


def main():
    logging.info("--- Starting Meeting Analysis Bot v5 (Google LLM–only) ---")

    # Determine config file path
    config_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
    if not os.path.exists(config_path):
        logging.error(f"CRITICAL: Configuration file ({config_path}) not found. Exiting.")
        sys.exit(1)

    # Load configuration
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logging.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"CRITICAL: Could not load or parse config file '{config_path}': {e}. Exiting.", exc_info=True)
        sys.exit(1)

    # --- RAG ID Check ---
    # The rag_store_name is now loaded directly from config.yaml
    rag_id_from_config = config.get("rag_store_name")
    if not rag_id_from_config or rag_id_from_config == "your_rag_store_id_here":
         logging.warning("rag_store_name not found or is placeholder in config.yaml. RAG features will not be used.")
         # Ensure analysis.py doesn't try to use the placeholder by setting it to None if invalid
         config["rag_store_name"] = None
    else:
         logging.info(f"Using RAG Store ID from config: {rag_id_from_config}")
         # Ensure analysis.py has access to the correct RAG_STORE_ID
         analysis.RAG_STORE_ID = rag_id_from_config


    # Processing limits
    proc_conf = config.get("processing", {})
    max_files = int(proc_conf.get("max_files_per_run", 999999)) # Default high
    per_file_sleep = float(proc_conf.get("sleep_between_files_sec", 0.0)) # Default none
    processed_this_run = 0

    # --- Authentication ---
    drive_service, gsheets_sheet = authenticate_google(config)
    if not drive_service or not gsheets_sheet:
        sys.exit(1) # Authentication failure is critical

    # --- Initial Setup & Ledger Check ---
    # ensure_tabs_exist called within authenticate_google now
    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_sheet, config)
        logging.info(f"Found {len(processed_file_ids)} previously processed/quarantined file IDs in ledger.")
    except Exception as e:
        logging.warning(f"Could not read processed file IDs from ledger: {e}. Will process all found files.", exc_info=True)
        processed_file_ids = set() # Use empty set if ledger read fails


    # --- Retry Quarantined Files ---
    retry_quarantined_files(drive_service, gsheets_sheet, config)

    # --- Discover Files ---
    parent_folder_id = config["google_drive"]["parent_folder_id"]
    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)
    if not team_folders:
         logging.warning("No team member folders found under the parent folder.")

    # --- Main Processing Loop ---
    logging.info("Starting to check discovered team folders for new files...")
    stop_processing = False # Flag to stop loop gracefully

    for member_name, folder_id in team_folders.items():
        if stop_processing:
            logging.info("Stopping further folder checks due to previous error or limit.")
            break

        logging.info(f"--- Checking folder for team member: {member_name} (ID: {folder_id}) ---")
        try:
            # Pass the set of known processed IDs
            files_to_process = gdrive.get_files_to_process(drive_service, folder_id, processed_file_ids)
            logging.info(f"Found {len(files_to_process)} new media file(s) for {member_name}.")

            for file_meta in files_to_process:
                # Check limits before processing each file
                if stop_processing:
                     break
                if processed_this_run >= max_files:
                    logging.info(f"Reached max_files_per_run limit ({max_files}). Stopping processing.")
                    stop_processing = True
                    break

                file_id = file_meta["id"]
                file_name = file_meta.get("name", f"Unknown_{file_id}")
                logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

                try:
                    # Call the main analysis function from analysis.py
                    analysis.process_single_file(drive_service, gsheets_sheet, file_meta, member_name, config)

                    # If successful, move to processed folder
                    processed_folder_id = config["google_drive"]["processed_folder_id"]
                    try:
                         gdrive.move_file(drive_service, file_id, folder_id, processed_folder_id)
                         logging.info(f"Successfully moved {file_name} to Processed folder.")
                         # Add to processed set immediately to prevent reprocessing if ledger update fails
                         processed_file_ids.add(file_id)
                    except Exception as move_e:
                         logging.error(f"ERROR moving {file_name} to Processed folder after successful analysis: {move_e}. File remains in source, ledger updated.")
                         # Ledger was already updated to 'Processed' in _write_success

                except QuotaExceeded as qe:
                    logging.error(f"Quota exceeded during processing of {file_name} - stopping this run.")
                    try:
                        error_msg = f"Gemini quota exceeded; run stopped ({str(qe)[:100]})"
                        gdrive.quarantine_file(drive_service, file_id, folder_id, error_msg, config)
                        sheets.update_ledger(gsheets_sheet, file_id, "Quarantined", error_msg, config, file_name)
                    except Exception as q_err:
                        logging.error(f"ERROR quarantining file {file_name} after quota error: {q_err}")
                    stop_processing = True # Set flag to stop outer loop

                except Exception as e:
                    # Catch other errors from analysis.process_single_file or unexpected issues
                    logging.error(f"CRITICAL failure processing file {file_name}: {e}", exc_info=True)
                    try:
                        # Use concise error message for quarantining/ledger
                        error_summary = f"{type(e).__name__}: {str(e)[:150]}"
                        gdrive.quarantine_file(drive_service, file_id, folder_id, error_summary, config)
                        sheets.update_ledger(gsheets_sheet, file_id, "Quarantined", error_summary, config, file_name)
                    except Exception as q_err:
                         logging.error(f"ERROR quarantining file {file_name} after critical error: {q_err}")
                    # Optionally decide whether to stop all processing on any critical error
                    # stop_processing = True

                processed_this_run += 1
                # Sleep between files if configured and not stopping
                if per_file_sleep > 0 and not stop_processing:
                    logging.debug(f"Sleeping for {per_file_sleep} seconds...")
                    time.sleep(per_file_sleep)

        except Exception as folder_e:
            logging.error(f"CRITICAL ERROR processing folder for {member_name} (ID: {folder_id}): {folder_e}", exc_info=True)
            # Decide if errors processing one folder should stop the whole run
            # stop_processing = True

    # --- Final Export ---
    export_data_for_dashboard(gsheets_sheet, config)

    logging.info(f"--- Main execution finished. Processed {processed_this_run} files this run. ---")

if __name__ == "__main__":
    main()
