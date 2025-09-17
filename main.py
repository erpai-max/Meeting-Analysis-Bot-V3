# main.py
import os
import json
import yaml
import logging
import sys
import time
import traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

# Import utility modules (your updated modules)
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

APP_VERSION = "v5"

# =======================
# Authentication
# =======================
def authenticate_google_services():
    """Authenticates with Google services and returns Drive + Sheets clients."""
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
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=scopes
        )

        drive_service = build("drive", "v3", credentials=creds)
        gsheets_client = gspread.authorize(creds)

        logging.info("SUCCESS: Authenticated Google Drive")
        logging.info("SUCCESS: Authenticated Google Sheets")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        logging.debug(traceback.format_exc())
        return None, None

# =======================
# Data Export for Dashboard
# =======================
def export_data_for_dashboard(gsheets_client, config):
    """Fetches all data from the results sheet and saves it as a JSON file for dashboard.html."""
    logging.info("Exporting latest data for the dashboard...")
    try:
        all_records = sheets.get_all_results(gsheets_client, config)
        if all_records:
            with open("dashboard_data.json", "w") as f:
                json.dump(all_records, f, indent=2, ensure_ascii=False)
            logging.info(
                f"SUCCESS: Exported {len(all_records)} records to dashboard_data.json."
            )
        else:
            logging.warning("No records found in the sheet to export for the dashboard.")
    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}")
        logging.debug(traceback.format_exc())

# =======================
# Quarantine Retry Handler
# =======================
def retry_quarantined_files(drive_service, gsheets_client, config):
    """Moves quarantined files back after 1 day for re-processing."""
    logging.info("Checking quarantined files for retry...")
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]
        parent_id = config["google_drive"]["parent_folder_id"]

        query = f"'{quarantine_id}' in parents and trashed = false"
        files = drive_service.files().list(
            q=query, fields="files(id, name, createdTime, parents)"
        ).execute().get("files", [])

        for file in files:
            created_time = file.get("createdTime")
            file_id = file["id"]
            file_name = file.get("name", "Unknown")
            # Only retry if file has been in quarantine > 24h
            if created_time:
                try:
                    created_epoch = time.mktime(
                        time.strptime(created_time[:19], "%Y-%m-%dT%H:%M:%S")
                    )
                except Exception:
                    # fallback: if parsing fails, skip retry for safety
                    logging.debug(f"Could not parse createdTime for {file_name}: {created_time}")
                    continue

                if (time.time() - created_epoch) > 86400:
                    logging.info(f"Retrying quarantined file: {file_name} (ID: {file_id})")
                    try:
                        gdrive.move_file(drive_service, file_id, quarantine_id, parent_id)
                        sheets.update_ledger(
                            gsheets_client,
                            file_id,
                            "Moved back for retry",
                            "Auto-retry after 1 day",
                            config,
                            file_name,
                        )
                    except Exception as e:
                        logging.error(f"ERROR: Could not move quarantined file {file_name}: {e}")
                        logging.debug(traceback.format_exc())
    except Exception as e:
        logging.error(f"ERROR while retrying quarantined files: {e}")
        logging.debug(traceback.format_exc())

# =======================
# Main Orchestrator
# =======================
def main():
    """Main function to orchestrate the entire analysis pipeline."""
    logging.info(f"--- Starting Meeting Analysis Bot {APP_VERSION} ---")

    # Load config
    config_path = None
    if os.path.exists("config.yaml"):
        config_path = "config.yaml"
    elif os.path.exists("config.yml"):
        config_path = "config.yml"

    if not config_path:
        logging.error("CRITICAL: config.yaml/.yml not found. Exiting.")
        sys.exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        logging.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"CRITICAL: Could not load config file: {e}. Exiting.")
        logging.debug(traceback.format_exc())
        sys.exit(1)

    # Authenticate
    drive_service, gsheets_client = authenticate_google_services()
    if not drive_service or not gsheets_client:
        logging.error("CRITICAL: Google services authentication failed. Exiting.")
        sys.exit(1)

    # Get processed file IDs (ledger)
    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_client, config)
    except Exception as e:
        logging.error(f"CRITICAL: Could not retrieve processed file ledger: {e}. Exiting.")
        logging.debug(traceback.format_exc())
        sys.exit(1)

    # Retry quarantined files if eligible
    retry_quarantined_files(drive_service, gsheets_client, config)

    # Discover team folders
    try:
        parent_folder_id = config["google_drive"]["parent_folder_id"]
    except KeyError:
        logging.error("CRITICAL: google_drive.parent_folder_id not set in config.yaml. Exiting.")
        sys.exit(1)

    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)

    if not team_folders:
        logging.warning("No team folders were discovered. Nothing to process.")
    else:
        logging.info("Starting to check discovered team folders...")
        for member_name, folder_id in team_folders.items():
            logging.info(f"--- Checking folder for team member: {member_name} (ID: {folder_id}) ---")
            try:
                files_to_process = gdrive.get_files_to_process(
                    drive_service, folder_id, processed_file_ids
                )
                logging.info(f"Found {len(files_to_process)} new media file(s) for {member_name}.")

                if not files_to_process:
                    continue

                for file_meta in files_to_process:
                    file_id = file_meta.get("id")
                    file_name = file_meta.get("name", "Unknown Filename")
                    logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

                    try:
                        analysis.process_single_file(
                            drive_service, gsheets_client, file_meta, member_name, config
                        )
                    except Exception as e:
                        error_message = f"Unhandled error in main loop for file {file_name}: {e}"
                        logging.error(error_message)
                        logging.debug(traceback.format_exc())

                        # Quarantine and update ledger (always include the filename)
                        try:
                            gdrive.quarantine_file(
                                drive_service, file_id, folder_id, str(e), config
                            )
                        except Exception as qerr:
                            logging.error(f"ERROR quarantining file {file_name}: {qerr}")
                            logging.debug(traceback.format_exc())

                        try:
                            sheets.update_ledger(
                                gsheets_client, file_id, "Quarantined", str(e), config, file_name
                            )
                        except Exception as uerr:
                            logging.error(f"ERROR updating ledger for {file_name}: {uerr}")
                            logging.debug(traceback.format_exc())
            except Exception as e:
                logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")
                logging.debug(traceback.format_exc())

    # After processing, export the data for the dashboard
    export_data_for_dashboard(gsheets_client, config)

    logging.info("--- Main execution finished ---")


if __name__ == "__main__":
    main()
