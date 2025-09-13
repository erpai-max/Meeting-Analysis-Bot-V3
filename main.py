import os
import yaml
import logging
import json
import sys
import traceback
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

# Import utility modules
import gdrive
import analysis
import sheets

# =======================
# Logging
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =======================
# Authentication
# =======================
def authenticate_google_services():
    """Authenticates with Google services and returns separate clients for Drive and Sheets."""
    logging.info("Attempting to authenticate with Google services...")
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            logging.error("CRITICAL: GCP_SA_KEY environment variable not found.")
            return None, None

        creds_info = json.loads(gcp_key_str)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)

        drive_service = build("drive", "v3", credentials=creds)
        gsheets_client = gspread.authorize(creds)

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        traceback.print_exc()
        return None, None

# =======================
# Data Export for Dashboard
# =======================
def export_data_for_dashboard(gsheets_client, config):
    """Fetches all data from the results sheet and saves it as a JSON file."""
    logging.info("Exporting latest data for the dashboard...")
    try:
        all_records = sheets.get_all_results(gsheets_client, config)
        if all_records:
            with open("dashboard_data.json", "w", encoding="utf-8") as f:
                json.dump(all_records, f, indent=2, ensure_ascii=False)
            logging.info(f"SUCCESS: Exported {len(all_records)} records to dashboard_data.json.")
        else:
            logging.warning("No records found in the sheet to export for the dashboard.")
    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}")
        traceback.print_exc()

# =======================
# Quarantine Retry Logic
# =======================
def handle_quarantined_files(drive_service, gsheets_client, processed_file_ids, config):
    """Moves quarantined files back for reprocessing if older than 1 day."""
    logging.info("Checking quarantined files for retry...")
    quarantine_folder = config["google_drive"]["quarantine_folder_id"]

    for file_id, status_info in processed_file_ids.items():
        if isinstance(status_info, dict):
            status = status_info.get("status", "").lower()
            timestamp_str = status_info.get("timestamp", "")
        else:
            # Backward compatibility (old ledger format)
            status = str(status_info).lower()
            timestamp_str = ""

        if status != "quarantined":
            continue

        try:
            if timestamp_str:
                quarantine_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                if datetime.utcnow() - quarantine_time < timedelta(days=1):
                    continue  # still waiting
            else:
                logging.warning(f"No timestamp for quarantined file {file_id}, skipping.")
                continue

            # Get original folder from ledger
            target_folder = status_info.get("original_folder")
            if not target_folder:
                logging.warning(f"No original folder info for {file_id}, cannot retry.")
                continue

            # Move file back to original folder
            logging.info(f"Retrying quarantined file {file_id} → moving back to {target_folder}")
            gdrive.move_file(drive_service, file_id, quarantine_folder, target_folder)

            # Update ledger
            sheets.update_ledger(gsheets_client, file_id, "Retry", "Moved back after 1 day", config)

        except Exception as e:
            logging.error(f"Failed to retry quarantined file {file_id}: {e}")
            traceback.print_exc()

# =======================
# Main Orchestrator
# =======================
def main():
    """Main function to orchestrate the entire analysis pipeline."""
    logging.info("--- Starting Meeting Analysis Bot v4.4 ---")

    # Load config file
    try:
        if os.path.exists("config.yml"):
            config_path = "config.yml"
        elif os.path.exists("config.yaml"):
            config_path = "config.yaml"
        else:
            logging.error("CRITICAL: config.yml or config.yaml not found. Exiting.")
            sys.exit(1)

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logging.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"CRITICAL: Could not load config file: {e}. Exiting.")
        traceback.print_exc()
        sys.exit(1)

    # Authenticate
    drive_service, gsheets_client = authenticate_google_services()
    if not drive_service or not gsheets_client:
        sys.exit(1)

    # Get ledger
    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_client, config)
        logging.info(f"Found {len(processed_file_ids)} file IDs in the ledger.")
    except Exception as e:
        logging.error(f"CRITICAL: Could not retrieve processed file ledger: {e}. Exiting.")
        traceback.print_exc()
        sys.exit(1)

    # Handle quarantine retry first
    handle_quarantined_files(drive_service, gsheets_client, processed_file_ids, config)

    # Discover team folders
    parent_folder_id = config.get("google_drive", {}).get("parent_folder_id")
    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)

    if not team_folders:
        logging.warning("No team folders were discovered. Nothing to process.")
    else:
        logging.info(f"Folder discovery complete. Found {len(team_folders)} total team folders.")
        logging.info("Starting to check discovered team folders...")

        for member_name, folder_id in team_folders.items():
            logging.info(f"--- Checking folder for team member: {member_name} (ID: {folder_id}) ---")
            try:
                files_to_process = gdrive.get_files_to_process(drive_service, folder_id, processed_file_ids)
                logging.info(f"Found {len(files_to_process)} new media file(s) for {member_name}.")

                if not files_to_process:
                    continue

                for file_meta in files_to_process:
                    file_id = file_meta["id"]
                    file_name = file_meta.get("name", "Unknown Filename")

                    logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")
                    try:
                        # Main processing logic is handled in analysis.py
                        analysis.process_single_file(drive_service, gsheets_client, file_meta, member_name, config)
                    except Exception as e:
                        error_message = f"{type(e).__name__}: {str(e)}"
                        logging.error(f"Unhandled error in main loop for file {file_name}: {error_message}")
                        traceback.print_exc()
                        gdrive.quarantine_file(drive_service, file_id, folder_id, error_message, config)
                        sheets.update_ledger(gsheets_client, file_id, "Quarantined", error_message, config)

            except Exception as e:
                logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")
                traceback.print_exc()

    # After processing all files, export the data for the dashboard
    export_data_for_dashboard(gsheets_client, config)

    logging.info("--- Main execution finished ---")

if __name__ == "__main__":
    main()
