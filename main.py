import logging
import yaml
import sys
import os

# Import utility modules
import gdrive
import analysis
import sheets

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =======================
# Main Orchestration
# =======================
def main():
    """Main function to run the entire analysis workflow."""
    logging.info("--- Starting Meeting Analysis Bot v3 ---")

    # 1. Load Configuration
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error("CRITICAL: config.yaml not found. Please create it. Exiting.")
        sys.exit(1)

    # 2. Authenticate all Google Services
    drive_service, gsheets_client = gdrive.authenticate_google_services()
    if not drive_service or not gsheets_client:
        logging.error("CRITICAL: Exiting due to authentication failure.")
        sys.exit(1)

    # 3. Get Processed File Ledger
    processed_file_ids = sheets.get_processed_file_ids(gsheets_client, config)
    logging.info(f"Found {len(processed_file_ids)} files in the processed ledger. They will be skipped.")

    # 4. Discover Team Folders
    team_folders = gdrive.discover_team_folders(drive_service, config['google_drive']['parent_folder_id'])
    if not team_folders:
        logging.warning("No team folders were discovered. Exiting.")
        sys.exit(0)

    # 5. Process Files in Each Folder
    for member_name, folder_id in team_folders.items():
        logging.info(f"--- Checking folder for team member: {member_name} ---")
        try:
            files_to_process = gdrive.get_files_to_process(drive_service, folder_id, processed_file_ids)
            logging.info(f"Found {len(files_to_process)} new media file(s) for {member_name}.")

            for file_meta in files_to_process:
                file_id = file_meta["id"]
                file_name = file_meta.get("name", "Unknown Filename")
                logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

                try:
                    # Core Analysis Pipeline for a single file
                    file_content = gdrive.download_file(drive_service, file_id)
                    transcript, duration_sec = analysis.transcribe_audio(file_content, file_name, config)

                    if transcript:
                        analysis_result = analysis.analyze_transcript(transcript, member_name, config)

                        if analysis_result == "RATE_LIMIT_EXCEEDED":
                            logging.warning("Gemini API quota exceeded. Stopping the current workflow run.")
                            sys.exit(0)

                        if analysis_result:
                            enriched_data = analysis.enrich_data_from_context(analysis_result, member_name, file_meta, duration_sec, config)
                            sheets.write_results(gsheets_client, enriched_data, config)
                            gdrive.move_file(drive_service, file_id, folder_id, config['google_drive']['processed_folder_id'])
                            sheets.update_ledger(gsheets_client, file_id, "PROCESSED", "", config)
                        else:
                            raise ValueError("Gemini analysis returned no data.")
                    else:
                        raise ValueError("Transcription failed or produced no text.")

                except Exception as e:
                    logging.error(f"ERROR processing file {file_name}: {e}. Moving to quarantine.")
                    gdrive.quarantine_file(drive_service, file_id, folder_id, str(e), config)
                    sheets.update_ledger(gsheets_client, file_id, "QUARANTINED", str(e), config)

        except Exception as e:
            logging.error(f"CRITICAL ERROR while processing {member_name}'s folder: {e}")

    logging.info("--- Meeting Analysis Bot v3 finished successfully ---")

if __name__ == "__main__":
    main()


