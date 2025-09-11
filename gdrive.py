import logging
import io
import json
import os
from typing import Dict, Tuple, Optional, List, Set

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, stop_after_attempt, wait_exponential

# =======================
# Authentication
# =======================
def authenticate_google_services() -> Tuple[Optional[object], Optional[object]]:
    """Authenticates with Google services using environment variables."""
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
        gsheets_client = build("sheets", "v4", credentials=creds) # Using googleapiclient for consistency

        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        return None, None

# =======================
# Folder & File Operations with Retry Logic
# =======================
RETRY_CONFIG = {
    'wait': wait_exponential(multiplier=1, min=4, max=60),
    'stop': stop_after_attempt(5),
}

@retry(**RETRY_CONFIG)
def discover_team_folders(drive_service, parent_folder_id: str) -> Dict[str, str]:
    """Dynamically discovers city and then team member subfolders."""
    team_folders = {}
    logging.info(f"Starting folder discovery in parent folder: {parent_folder_id}")

    city_query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    city_folders = drive_service.files().list(q=city_query, fields="files(id, name)").execute().get('files', [])

    if not city_folders:
        logging.warning("No city subfolders found inside the parent folder.")
        return {}

    for city in city_folders:
        logging.info(f"Found city folder: {city['name']}")
        member_query = f"'{city['id']}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        member_folders = drive_service.files().list(q=member_query, fields="files(id, name)").execute().get('files', [])
        
        for member in member_folders:
            logging.info(f"  - Discovered team member folder: {member['name']} (ID: {member['id']})")
            team_folders[member['name']] = member['id']
            
    logging.info(f"Folder discovery complete. Found {len(team_folders)} total team folders.")
    return team_folders

@retry(**RETRY_CONFIG)
def get_files_to_process(drive_service, folder_id: str, processed_ids: Set[str]) -> List[Dict]:
    """Gets all media files in a folder that are not in the processed_ids set."""
    files_to_process = []
    query = f"'{folder_id}' in parents and trashed = false and (mimeType contains 'audio/' or mimeType contains 'video/')"
    fields = "nextPageToken, files(id, name, mimeType, parents, webViewLink)"
    page_token = None
    while True:
        response = drive_service.files().list(q=query, fields=fields, pageToken=page_token).execute()
        for file_meta in response.get("files", []):
            if file_meta['id'] not in processed_ids:
                files_to_process.append(file_meta)
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return files_to_process

@retry(**RETRY_CONFIG)
def download_file(drive_service, file_id: str) -> io.BytesIO:
    """Downloads a file's content into a BytesIO object."""
    logging.info(f"Starting download for file ID: {file_id}")
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            logging.info(f"Download progress: {int(status.progress() * 100)}%")
    fh.seek(0)
    logging.info("SUCCESS: File download complete.")
    return fh

@retry(**RETRY_CONFIG)
def move_file(drive_service, file_id: str, source_folder_id: str, destination_folder_id: str):
    """Moves a file from a source folder to a destination folder."""
    logging.info(f"Moving file {file_id} from {source_folder_id} to {destination_folder_id}")
    drive_service.files().update(
        fileId=file_id,
        addParents=destination_folder_id,
        removeParents=source_folder_id,
        fields="id, parents"
    ).execute()
    logging.info(f"SUCCESS: File {file_id} moved.")

@retry(**RETRY_CONFIG)
def quarantine_file(drive_service, file_id: str, source_folder_id: str, error_message: str, config: Dict):
    """Moves a file to the quarantine folder and adds the error message to its description."""
    quarantine_folder_id = config['google_drive']['quarantine_folder_id']
    logging.warning(f"Quarantining file {file_id} due to error: {error_message}")
    
    # 1. Add error message to the file's description for easy triage
    try:
        body = {'description': f"QUARANTINE_REASON: {error_message[:1000]}"} # Limit error message size
        drive_service.files().update(fileId=file_id, body=body).execute()
        logging.info(f"Updated description for quarantined file {file_id}.")
    except HttpError as e:
        logging.error(f"Could not update description for file {file_id}: {e}")

    # 2. Move the file
    move_file(drive_service, file_id, source_folder_id, quarantine_folder_id)




