import os
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_google(config):
    """Authenticate Drive + Sheets. Return (drive_service, Spreadsheet)."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("GCP_SA_KEY not set")

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
        sheets.ensure_tabs_exist(sheet, config)
        logging.info("SUCCESS: Authenticated Google Sheets")

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, sheet
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        return None, None

def export_data_for_dashboard(gsheets_sheet, config):
    logging.info("Exporting latest data for the dashboard...")
    try:
        all_records = sheets.get_all_results(gsheets_sheet, config)
        with open("dashboard_data.json", "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=2, ensure_ascii=False)
        logging.info(f"SUCCESS: Exported {len(all_records)} records to dashboard_data.json.")
    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}")

def retry_quarantined_files(drive_service, gsheets_sheet, config):
    logging.info("Checking quarantined files for retry...")
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]
        parent_id = config["google_drive"]["parent_folder_id"]

        files = drive_service.files().list(
            q=f"'{quarantine_id}' in parents and trashed=false",
            fields="files(id, name, createdTime, parents)"
        ).execute().get("files", [])

        for file in files:
            created_time = file.get("createdTime")
            file_id = file["id"]
            file_name = file["name"]

            if created_time:
                created_epoch = time.mktime(time.strptime(created_time[:19], "%Y-%m-%dT%H:%M:%S"))
                if (time.time() - created_epoch) > 86400:
                    logging.info(f"Retrying quarantined file: {file_name} (ID: {file_id})")
                    try:
                        gdrive.move_file(drive_service, file_id, quarantine_id, parent_id)
                        sheets.update_ledger(gsheets_sheet, file_id, "Moved back for retry", "Auto-retry after 1 day", config, file_name)
                    except Exception as e:
                        logging.error(f"ERROR: Could not move quarantined file {file_name}: {e}")
    except Exception as e:
        logging.error(f"ERROR while retrying quarantined files: {e}")

def main():
    logging.info("--- Starting Meeting Analysis Bot v5 ---")

    config_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
    if not os.path.exists(config_path):
        logging.error("CRITICAL: config.yaml/.yml not found. Exiting.")
        sys.exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logging.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"CRITICAL: Could not load config file: {e}. Exiting.")
        sys.exit(1)

    drive_service, gsheets_sheet = authenticate_google(config)
    if not drive_service or not gsheets_sheet:
        sys.exit(1)

    # Ensure tabs exist & headers
    sheets.ensure_tabs_exist(gsheets_sheet, config)

    # Read already processed IDs
    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_sheet, config)
    except Exception as e:
        logging.warning(f"Could not read processed ledger: {e}. Continuing with empty list.")
        processed_file_ids = []

    # Retry quarantine
    retry_quarantined_files(drive_service, gsheets_sheet, config)

    parent_folder_id = config["google_drive"]["parent_folder_id"]
    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)

    logging.info("Starting to check discovered team folders...")
    for member_name, folder_id in team_folders.items():
        logging.info(f"--- Checking folder for team member: {member_name} (ID: {folder_id}) ---")
        try:
            files_to_process = gdrive.get_files_to_process(drive_service, folder_id, processed_file_ids)
            logging.info(f"Found {len(files_to_process)} new media file(s) for {member_name}.")

            for file_meta in files_to_process:
                file_id = file_meta["id"]
                file_name = file_meta.get("name", "Unknown Filename")
                logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

                try:
                    analysis.process_single_file(drive_service, gsheets_sheet, file_meta, member_name, config)

                    # Move to processed folder after success
                    processed_folder_id = config["google_drive"]["processed_folder_id"]
                    gdrive.move_file(drive_service, file_id, folder_id, processed_folder_id)

                except Exception as e:
                    logging.error(f"Unhandled error in main loop for file {file_name}: {e}")
                    gdrive.quarantine_file(drive_service, file_id, folder_id, str(e), config)
                    sheets.update_ledger(gsheets_sheet, file_id, "Quarantined", str(e), config, file_name)

        except Exception as e:
            logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")

    export_data_for_dashboard(gsheets_sheet, config)
    logging.info("--- Main execution finished ---")

if __name__ == "__main__":
    main()
