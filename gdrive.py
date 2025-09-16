import io
import os
import logging
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

import analysis

# =======================
# Google Drive Auth
# =======================
def authenticate_gdrive(config):
    """Authenticate with Google Drive API using service account key."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY environment variable")

        creds_info = service_account.Credentials.from_service_account_info(
            eval(gcp_key_str),  # stringified JSON from secrets
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds_info)
        logging.info("SUCCESS: Authenticated Google Drive")
        return service
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate Google Drive: {e}")
        raise

# =======================
# File Download
# =======================
def download_file(service, file_id: str, file_name: str) -> str:
    """Download a file from Google Drive and return local path."""
    local_path = f"/tmp/{file_name.replace(' ', '_')}"
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"Download progress for {file_name}: {int(status.progress() * 100)}%")
        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name}: {e}")
        raise

# =======================
# Move File
# =======================
def move_file(service, file_id: str, old_folder: str, new_folder: str):
    """Move file between Drive folders."""
    try:
        # Remove from old, add to new
        file = service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        service.files().update(
            fileId=file_id,
            addParents=new_folder,
            removeParents=prev_parents,
            fields="id, parents"
        ).execute()
        logging.info(f"SUCCESS: File {file_id} moved from {old_folder} → {new_folder}")
    except Exception as e:
        logging.error(f"ERROR moving file {file_id}: {e}")

# =======================
# Folder Scan + Processing
# =======================
def scan_and_process_all(drive_service, gsheets_client, config):
    """Scan all team folders and process new files."""
    logging.info("Starting to check discovered team folders...")

    parent_id = config["google_drive"]["parent_folder_id"]

    # List city/team folders under parent
    city_folders = drive_service.files().list(
        q=f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder'",
        fields="files(id, name)"
    ).execute().get("files", [])

    for city in city_folders:
        logging.info(f"Found city folder: {city['name']}")
        team_folders = drive_service.files().list(
            q=f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name)"
        ).execute().get("files", [])

        for team in team_folders:
            logging.info(f"  - Discovered team member folder: {team['name']} (ID: {team['id']})")
            process_team_folder(drive_service, gsheets_client, team, config)

# =======================
# Process Team Folder
# =======================
def process_team_folder(drive_service, gsheets_client, team_folder: dict, config: dict):
    """Check files in one team folder and process each."""
    folder_id = team_folder["id"]
    member_name = team_folder["name"]

    try:
        files = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name, mimeType, createdTime)"
        ).execute().get("files", [])
    except Exception as e:
        logging.error(f"ERROR fetching files for {member_name}: {e}")
        return

    media_files = [f for f in files if f["mimeType"].startswith("audio") or f["name"].endswith(".mp3")]

    if not media_files:
        return

    logging.info(f"Found {len(media_files)} new media file(s) for {member_name}.")

    for f in media_files:
        file_id, file_name = f["id"], f["name"]
        logging.info(f"--- Processing file: {file_name} (ID: {file_id}) ---")

        try:
            analysis.process_single_file(drive_service, gsheets_client, f, member_name, config)

            # Move to Processed
            move_file(
                drive_service,
                file_id,
                folder_id,
                config["google_drive"]["processed_folder_id"]
            )
        except Exception as e:
            logging.error(f"ERROR processing {file_name}: {e}")

            # Move to Quarantine
            move_file(
                drive_service,
                file_id,
                folder_id,
                config["google_drive"]["quarantine_folder_id"]
            )
