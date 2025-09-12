import os
import yaml
import logging
import json
import sys
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            with open("dashboard_data.json", "w") as f:
                json.dump(all_records, f, indent=2)
            logging.info(f"SUCCESS: Exported {len(all_records)} records to dashboard_data.json.")
        else:
            logging.warning("No records found in the sheet to export for the dashboard.")
    except Exception as e:
        logging.error(f"ERROR: Could not export data for dashboard: {e}")

# =======================
# Main Orchestrator
# =======================
def main():
    """Main function to orchestrate the entire analysis pipeline."""
    logging.info("--- Starting Meeting Analysis Bot v3 ---")
    
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"CRITICAL: Could not load config.yaml: {e}. Exiting.")
        sys.exit(1)

    drive_service, gsheets_client = authenticate_google_services()
    if not drive_service or not gsheets_client:
        sys.exit(1)

    try:
        processed_file_ids = sheets.get_processed_file_ids(gsheets_client, config)
    except Exception as e:
        logging.error(f"CRITICAL: Could not retrieve processed file ledger: {e}. Exiting.")
        sys.exit(1)
        
    parent_folder_id = config.get('google_drive', {}).get('parent_folder_id')
    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)
    
    if not team_folders:
        logging.warning("No team folders were discovered. Nothing to process.")
    else:
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
                        file_content = gdrive.download_file(drive_service, file_id)
                        transcript, duration_sec = analysis.transcribe_audio(file_content, file_name, config)

                        if transcript:
                            analysis_data = analysis.analyze_transcript(transcript, member_name, config)

                            if analysis_data == "RATE_LIMIT_EXCEEDED":
                                logging.warning("Gemini API quota exceeded. Stopping workflow.")
                                sys.exit(0)

                            if analysis_data:
                                enriched_data = analysis.enrich_data_from_context(analysis_data, member_name, file_meta, duration_sec, config)
                                sheets.write_results(gsheets_client, enriched_data, config)
                                gdrive.move_file(drive_service, file_id, folder_id, config['google_drive']['processed_folder_id'])
                                sheets.update_ledger(gsheets_client, file_id, "Success", "", config)
                            else:
                                raise ValueError("Gemini analysis returned no data.")
                        else:
                            raise ValueError("Transcription failed or produced an empty transcript.")

                    except Exception as e:
                        error_message = f"Error processing file {file_name}: {e}"
                        logging.error(error_message)
                        gdrive.quarantine_file(drive_service, file_id, folder_id, str(e), config)
                        sheets.update_ledger(gsheets_client, file_id, "Quarantined", str(e), config)

            except Exception as e:
                logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")

    # After processing all files, export the data for the dashboard
    export_data_for_dashboard(gsheets_client, config)

    logging.info("--- Main execution finished ---")

if __name__ == "__main__":
    main()

