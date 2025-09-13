import logging
import io
from typing import Dict, List, Set

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, stop_after_attempt, wait_exponential

import sheets  # ✅ Needed for ledger updates

# =======================
# Constants
# =======================
RETRY_CONFIG = {
    'wait': wait_exponential(multiplier=2, min=5, max=60),
    'stop': stop_after_attempt(5),
    'reraise': True,
}

# =======================
# Folder & File Operations with Retry Logic
# =======================
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

    utility_folders = ["processed meetings", "quarantined meetings"]

    for city in city_folders:
        if city['name'].lower() in utility_folders:
            continue

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
    """Gets all media files in a folder that are not already processed."""
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
    """Moves a file safely from a source folder to a destination folder."""
    logging.info(f"Moving file {file_id} from {source_folder_id} to {destination_folder_id}")
    try:
        # Fetch all parents
        file = drive_service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))

        drive_service.files().update(
            fileId=file_id,
            addParents=destination_folder_id,
            removeParents=prev_parents,
            fields="id, parents"
        ).execute()

        logging.info(f"SUCCESS: File {file_id} moved.")
    except Exception as e:
        logging.error(f"ERROR: Could not move file {file_id}: {e}")
        raise


@retry(**RETRY_CONFIG)
def quarantine_file(drive_service, file_id: str, source_folder_id: str, error_message: str, config: Dict, gsheets_client=None):
    """
    Moves a file to the quarantine folder and records its original folder in Sheets.
    """
    quarantine_folder_id = config['google_drive']['quarantine_folder_id']
    logging.warning(f"Quarantining file {file_id} due to error: {error_message}")

    # 1. Update description in Drive
    try:
        body = {'description': f"QUARANTINE_REASON: {error_message[:1000]}"}  # Trim error message
        drive_service.files().update(fileId=file_id, body=body).execute()
        logging.info(f"Updated description for quarantined file {file_id}.")
    except HttpError as e:
        logging.error(f"Could not update description for file {file_id}: {e}")

    # 2. Move to quarantine
    move_file(drive_service, file_id, source_folder_id, quarantine_folder_id)

    # 3. Update ledger with original folder (for reprocessing later)
    if gsheets_client:
        try:
            sheets.update_ledger(
                gsheets_client,
                file_id,
                "Quarantined",
                error_message,
                config,
                original_folder=source_folder_id
            )
        except Exception as e:
            logging.error(f"Failed to update ledger for quarantined file {file_id}: {e}")
