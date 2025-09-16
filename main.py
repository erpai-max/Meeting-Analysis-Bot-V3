import os
import logging
import yaml
import time

# Local imports
import gdrive
import sheets
import analysis

from googleapiclient.discovery import build
from google.oauth2 import service_account

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =======================
# Authenticate Google Services
# =======================
def authenticate_google(config: dict):
    """Authenticate Google API clients (Drive + Sheets)."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY env var")

        creds_info = json.loads(gcp_key_str)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)

        # Drive service
        drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

        # Sheets client
        import gspread
        gsheets_client = gspread.authorize(creds)

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        raise

# =======================
# Main
# =======================
def main():
    logging.info("--- Starting Meeting Analysis Bot v5 ---")

    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    logging.info("Loaded configuration from config.yaml")

    # Auth
    drive_service, gsheets_client = authenticate_google(config)

    # Ensure ledger exists
    sheets.ensure_ledger_exists(gsheets_client, config)

    # Retry quarantined files first
    sheets.retry_quarantined_files(drive_service, gsheets_client, config)

    # Discover team folders
    team_folders = gdrive.discover_team_folders(drive_service, config["google_drive"]["parent_folder_id"])

    logging.info("Starting to check discovered team folders...")
    for member_name, folder_id in team_folders.items():
        logging.info(f"--- Checking folder for team member: {member_name} (ID: {folder_id}) ---")

        # Get processed file IDs from ledger
        processed_ids = sheets.get_processed_file_ids(gsheets_client, config)

        # Find new files
        new_files = gdrive.get_files_to_process(drive_service, folder_id, processed_ids)
        logging.info(f"Found {len(new_files)} new media file(s) for {member_name}.")

        for file_meta in new_files:
            file_name = file_meta.get("name", "Unknown")
            logging.info(f"--- Processing file: {file_name} (ID: {file_meta['id']}) ---")

            try:
                analysis.process_single_file(
                    drive_service,
                    gsheets_client,
                    file_meta,
                    member_name,
                    config,
                )
            except Exception as e:
                logging.error(f"Unhandled error in main loop for file {file_name}: {e}")
                try:
                    gdrive.quarantine_file(
                        drive_service,
                        file_meta["id"],
                        folder_id,
                        str(e),
                        config,
                    )
                    sheets.update_ledger(
                        gsheets_client,
                        file_meta["id"],
                        "Quarantined",
                        str(e),
                        config,
                        file_name,
                    )
                except Exception as qe:
                    logging.error(f"Failed to quarantine {file_name}: {qe}")

    logging.info("--- Main execution finished ---")


if __name__ == "__main__":
    main()
