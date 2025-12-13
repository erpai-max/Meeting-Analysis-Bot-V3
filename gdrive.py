# ===================================================================
# gdrive.py — Google Drive Helpers
# Stable, Retry-safe, Production-ready (Dec 2025)
# ===================================================================

import os
import io
import time
import logging
import random
from typing import List, Dict, Optional, Collection

from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError


# -------------------------------------------------------------------
# INTERNAL HELPERS
# -------------------------------------------------------------------

def _sanitize_filename(name: str) -> str:
    """
    Make a Drive filename safe for local filesystem.
    """
    if not name:
        return "unnamed"

    bad = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t']
    safe = name
    for ch in bad:
        safe = safe.replace(ch, '_')

    safe = "_".join(safe.split())
    return safe[:200]


def _sleep_backoff(attempt: int, base: float = 0.8, jitter: float = 0.3):
    """
    Exponential backoff with jitter (caps at ~8s).
    """
    delay = (base * (2 ** attempt)) + random.uniform(0, jitter)
    time.sleep(min(delay, 8.0))


# -------------------------------------------------------------------
# DOWNLOAD FILE
# -------------------------------------------------------------------

def download_file(service, file_id: str, file_name: Optional[str] = None) -> str:
    """
    Download a file from Google Drive to /tmp and return local path.
    Includes retry for transient chunk failures.
    """
    safe_name = _sanitize_filename(file_name or f"{file_id}.bin")
    local_path = os.path.join("/tmp", safe_name)

    try:
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            attempt = 0

            while not done:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        pct = int(status.progress() * 100)
                        logging.info(f"Download progress [{safe_name}]: {pct}%")
                except HttpError as he:
                    logging.warning(f"Transient download error, retrying: {he}")
                    _sleep_backoff(attempt)
                    attempt += 1
                except Exception as e:
                    logging.error(f"Download failed: {e}")
                    raise

        logging.info(f"SUCCESS: File downloaded → {local_path}")
        return local_path

    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        raise


# -------------------------------------------------------------------
# MOVE FILE (RETRY-SAFE)
# -------------------------------------------------------------------

def _move_with_retry(service, file_id: str, target_folder_id: str, max_attempts: int = 4):
    """
    Move file to target folder with retries (handles SSL / 5xx issues).
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

            logging.info(f"SUCCESS: File {file_id} moved → {target_folder_id}")
            return

        except Exception as e:
            last_err = e
            logging.warning(f"Move attempt {attempt + 1} failed for {file_id}: {e}")
            _sleep_backoff(attempt)
            attempt += 1

    logging.error(f"ERROR moving file {file_id} after {max_attempts} attempts: {last_err}")
    raise last_err


def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
    """
    Public move API — always retry-safe.
    """
    _move_with_retry(service, file_id, new_folder_id)


def move_to_processed(service, file_id: str, config: Dict):
    """
    Move a file to the configured processed folder.
    """
    processed_id = config["google_drive"]["processed_folder_id"]
    _move_with_retry(service, file_id, processed_id)


# -------------------------------------------------------------------
# DISCOVER TEAM FOLDERS
# -------------------------------------------------------------------

def discover_team_folders(service, parent_folder_id: str) -> Dict[str, str]:
    """
    Discover team member folders under each city folder.
    """
    team_folders: Dict[str, str] = {}

    try:
        city_folders = service.files().list(
            q=f"'{parent_folder_id}' in parents "
              f"and mimeType='application/vnd.google-apps.folder' "
              f"and trashed=false",
            fields="files(id, name)",
        ).execute().get("files", [])

        for city in city_folders:
            logging.info(f"Found city folder: {city['name']}")

            member_folders = service.files().list(
                q=f"'{city['id']}' in parents "
                  f"and mimeType='application/vnd.google-apps.folder' "
                  f"and trashed=false",
                fields="files(id, name)",
            ).execute().get("files", [])

            for mf in member_folders:
                name = mf["name"]
                if name.lower() in {"processed meetings", "quarantined meetings"}:
                    continue

                logging.info(f"  - Discovered team member folder: {name} (ID: {mf['id']})")
                team_folders[name] = mf["id"]

    except Exception as e:
        logging.error(f"ERROR discovering team folders: {e}", exc_info=True)

    return team_folders


# -------------------------------------------------------------------
# GET FILES TO PROCESS
# -------------------------------------------------------------------

def get_files_to_process(
    service,
    folder_id: str,
    processed_file_ids: Collection[str]
) -> List[Dict]:
    """
    List unprocessed audio/video files in a folder, oldest first.
    Skips zero-byte files.
    """
    try:
        query = (
            f"'{folder_id}' in parents and trashed=false "
            f"and (mimeType contains 'audio/' or mimeType contains 'video/')"
        )

        files = service.files().list(
            q=query,
            orderBy="createdTime",
            fields="files(id, name, mimeType, createdTime, size)",
        ).execute().get("files", [])

        new_files = [
            f for f in files
            if f["id"] not in processed_file_ids
            and int(f.get("size", 1)) > 0
        ]

        return new_files

    except Exception as e:
        logging.error(f"ERROR getting files from folder {folder_id}: {e}", exc_info=True)
        return []


# -------------------------------------------------------------------
# QUARANTINE FILE
# -------------------------------------------------------------------

def quarantine_file(
    service,
    file_id: str,
    current_folder_id: str,
    error_message: str,
    config: Dict
):
    """
    Move file to quarantine folder and tag description with reason.
    Retry-safe.
    """
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]

        # Best-effort description update (length-safe)
        try:
            safe_msg = error_message[:300]
            service.files().update(
                fileId=file_id,
                body={"description": f"Quarantined: {safe_msg}"},
            ).execute()
        except Exception as e:
            logging.warning(f"Could not set description for {file_id}: {e}")

        _move_with_retry(service, file_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} moved to Quarantine")

    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}", exc_info=True)
        raise
