import os
import yaml
import logging
import json
import sys
import time
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

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
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
                json.dump(all_records, f, indent=2)
            logging.info(
                f"SUCCESS: Exported {len(all_records)} records to dashboard_data.json."
            )
        else:
            logging.warning("No records found in the sheet to export for the dashboard.")
    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}")

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
            file_name = file["name"]

            # Only retry if file has been in quarantine > 24h
            if created_time:
                created_epoch = time.mktime(
                    time.strptime(created_time[:19], "%Y-%m-%dT%H:%M:%S")
                )
                if (time.time() - created_epoch) > 86400:
                    logging.info(
                        f"Retrying quarantined file: {file_name} (ID: {file_id})"
                    )
                    try:
                        gdrive.move_file(
                            drive_service, file_id, quarantine_id, parent_id
                        )
                        sheets.update_ledger(
                            gsheets_client,
                            file_id,
                            "Moved back for retry",
                            "Auto-retry after 1 day",
                            config,
                            file_name,   # ✅ pass file_name
                        )
                    except Exception as e:
                        logging.error(
                            f"ERROR: Could not move quarantined file {file_name}: {e}"
                        )
    except Exception as e:
        logging.error(f"ERROR while retrying quarantined files: {e}")

# =======================
# Main Orchestrator
# =======================
def main():
    """Main function to orchestrate the entire analysis pipeline."""
    logging.info("--- Starting Meeting Analysis Bot v4.5 ---")

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
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logging.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"CRITICAL: Could not load config file: {e}. Exiting.")
        sys.exit(1)

    # Authenticate
    drive_service, gsheets_client = authenticate_google_services()
    if not drive_service or not gsheets_client:
        sys.exit(1)

    # Get processed file IDs
    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_client, config)
    except Exception as e:
        logging.error(f"CRITICAL: Could not retrieve processed file ledger: {e}. Exiting.")
        sys.exit(1)

    # Retry quarantined files if eligible
    retry_quarantined_files(drive_service, gsheets_client, config)

    # Discover team folders
    parent_folder_id = config["google_drive"]["parent_folder_id"]
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
                    file_id = file_meta["id"]
                    file_name = file_meta.get("name", "Unknown Filename")
                    logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

                    try:
                        analysis.process_single_file(
                            drive_service, gsheets_client, file_meta, member_name, config
                        )
                    except Exception as e:
                        error_message = f"Unhandled error in main loop for file {file_name}: {e}"
                        logging.error(error_message)
                        gdrive.quarantine_file(
                            drive_service, file_id, folder_id, str(e), config
                        )
                        sheets.update_ledger(
                            gsheets_client, file_id, "Quarantined", str(e), config, file_name  # ✅ pass file_name
                        )
            except Exception as e:
                logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")

    # After processing, export the data for the dashboard
    export_data_for_dashboard(gsheets_client, config)

    logging.info("--- Main execution finished ---")


if __name__ == "__main__":
    main()
