import os
import io
import time
import logging
import random
from typing import List, Dict, Optional
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError


# -----------------------
# Internal helpers
# -----------------------
def _sanitize_filename(name: str) -> str:
    """
    Make a Drive filename safe for local filesystem.
    Replaces path/illegal characters and collapses spaces.
    """
    if not name:
        return "unnamed"
    bad = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t']
    safe = name
    for ch in bad:
        safe = safe.replace(ch, '_')
    # spaces -> underscores for consistency with your logs
    safe = "_".join(safe.split())
    # optional: cap length
    return safe[:200]


def _sleep_backoff(attempt: int, base: float = 0.8, jitter: float = 0.3):
    """Exponential backoff with jitter."""
    delay = (base * (2 ** attempt)) + random.uniform(0, jitter)
    time.sleep(min(delay, 8.0))


# -----------------------
# Download File
# -----------------------
def download_file(service, file_id: str, file_name: Optional[str] = None) -> str:
    """
    Download a file from Google Drive by file_id.
    Saves to /tmp and returns the local path.
    Includes basic retry on chunk failures.
    """
    safe_name = _sanitize_filename(file_name or f"{file_id}.bin")
    local_path = os.path.join("/tmp", safe_name)

    try:
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        # Use f-string to avoid %-format conflicts
                        pct = int(status.progress() * 100)
                        logging.info(f"Download progress for {file_name or file_id}: {pct}%")
                except HttpError as he:
                    logging.warning(f"Transient download error (will retry): {he}")
                    _sleep_backoff(1)
                except Exception as e:
                    logging.error(f"Download chunk failed: {e}")
                    raise

        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        raise


# -----------------------
# Move File (generic)
# -----------------------
def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
    """
    Move file from one folder to another.
    Uses one update call to remove previous parents and add the new one.
    """
    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        service.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=prev_parents,
            fields="id, parents",
        ).execute()
        logging.info(f"SUCCESS: File {file_id} moved from {old_folder_id} → {new_folder_id}")
    except Exception as e:
        logging.error(f"ERROR moving file {file_id}: {e}")
        raise


def _move_with_retry(service, file_id: str, target_folder_id: str, max_attempts: int = 4):
    """
    Low-level move that fetches current parents and moves file to target folder
    with retries/backoff (handles occasional SSL/HTTP hiccups).
    """
    attempt = 0
    last_err = None
    while attempt < max_attempts:
        try:
            file = service.files().get(fileId=file_id, fields="parents").execute()
            prev_parents = ",".join(file.get("parents", []))
            service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=prev_parents,
                fields="id, parents",
            ).execute()
            logging.info(f"SUCCESS: File {file_id} moved to {target_folder_id}")
            return
        except Exception as e:
            last_err = e
            logging.warning(f"Move attempt {attempt + 1} failed for {file_id}: {e}")
            _sleep_backoff(attempt)
            attempt += 1
    logging.error(f"ERROR moving file {file_id} after {max_attempts} attempts: {last_err}")
    raise last_err


# -----------------------
# Move to Processed
# -----------------------
def move_to_processed(service, file_id: str, config: Dict):
    """
    Move a file to the configured 'processed_folder_id'.
    """
    processed_id = config["google_drive"]["processed_folder_id"]
    _move_with_retry(service, file_id, processed_id)


# -----------------------
# Discover Team Folders
# -----------------------
def discover_team_folders(service, parent_folder_id: str) -> Dict[str, str]:
    """
    Discover team member folders under each city folder inside the parent folder.
    """
    team_folders: Dict[str, str] = {}
    try:
        # Top-level: city folders
        city_folders = service.files().list(
            q=f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
        ).execute().get("files", [])

        for city in city_folders:
            logging.info(f"Found city folder: {city['name']}")

            # Skip the special folders at city level
            # (Some structures put Processed/Quarantined at root—harmless if included.)
            member_folders = service.files().list(
                q=f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)",
            ).execute().get("files", [])

            for mf in member_folders:
                name = mf["name"]
                # Ignore if it looks like a special folder
                if name.lower() in {"processed meetings", "quarantined meetings"}:
                    continue
                logging.info(f"  - Discovered team member folder: {name} (ID: {mf['id']})")
                team_folders[name] = mf["id"]
    except Exception as e:
        logging.error(f"ERROR discovering team folders: {e}")
    return team_folders


# -----------------------
# Get Files To Process
# -----------------------
def get_files_to_process(service, folder_id: str, processed_file_ids: List[str]) -> List[Dict]:
    """
    List all unprocessed media files (audio/video) in a folder, sorted by createdTime (oldest first).
    """
    try:
        query = (
            f"'{folder_id}' in parents and trashed=false "
            f"and (mimeType contains 'audio/' or mimeType contains 'video/')"
        )
        files = service.files().list(
            q=query,
            orderBy="createdTime",
            fields="files(id, name, mimeType, createdTime)",
        ).execute().get("files", [])

        new_files = [f for f in files if f["id"] not in processed_file_ids]
        return new_files
    except Exception as e:
        logging.error(f"ERROR getting files to process from folder {folder_id}: {e}")
        return []


# -----------------------
# Quarantine File
# -----------------------
def quarantine_file(service, file_id: str, current_folder_id: str, error_message: str, config: Dict):
    """
    Move file to quarantine folder and update description with reason.
    Uses retries to avoid transient API/SSL issues.
    """
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]

        # Update description (best-effort)
        try:
            service.files().update(
                fileId=file_id,
                body={"description": f"Quarantined due to error: {error_message}"},
            ).execute()
        except Exception as e:
            logging.warning(f"Could not set quarantine description for {file_id}: {e}")

        _move_with_retry(service, file_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} moved to Quarantined ({quarantine_id})")
    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}")
