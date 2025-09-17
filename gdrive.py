# gdrive.py
import os
import io
import logging
import re
import tempfile
from googleapiclient.http import MediaIoBaseDownload

# sanitize filename to safe local name
def _safe_filename(name: str) -> str:
    if not name:
        return None
    # replace slashes and multiple spaces, keep extension if present
    name = name.strip()
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[^\w\-\.\s]", "_", name)
    name = re.sub(r"\s+", "_", name)
    return name

# -----------------------
# Download File
# -----------------------
def download_file(service, file_id: str, file_name: str = None) -> str:
    """
    Download a file from Google Drive by file_id.
    Returns local path (in temp dir). Raises exception on failure.
    """
    try:
        request = service.files().get_media(fileId=file_id)
        safe_name = _safe_filename(file_name) if file_name else file_id
        # ensure extension if missing: try to fetch mimeType and map common types
        if not safe_name or "." not in safe_name:
            # attempt to get the file metadata for name/mimeType
            try:
                meta = service.files().get(fileId=file_id, fields="name, mimeType").execute()
                meta_name = meta.get("name")
                if meta_name:
                    safe_name = _safe_filename(meta_name)
                else:
                    # fallback with id
                    safe_name = f"{file_id}"
            except Exception:
                safe_name = f"{file_id}"

        local_dir = tempfile.gettempdir()
        local_path = os.path.join(local_dir, safe_name)

        # open file and stream download
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status and status.progress() is not None:
                try:
                    logging.info(f"Download progress for {safe_name}: {int(status.progress() * 100)}%")
                except Exception:
                    # some implementations may not provide progress()
                    logging.debug("Download progress update unavailable")
        fh.close()
        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        # remove partial file if exists
        try:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass
        raise

# -----------------------
# Move File
# -----------------------
def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
    """
    Move file from one folder to another.
    If old_folder_id is None or not present, will attempt to remove all parents and add new parent.
    """
    try:
        # fetch current parents
        file = service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents_list = file.get("parents", []) or []
        prev_parents = ",".join(prev_parents_list)
        # If old_folder_id provided, prefer to remove that; otherwise remove all prev parents
        remove_parents = prev_parents
        if old_folder_id and old_folder_id in prev_parents_list:
            remove_parents = old_folder_id

        service.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=remove_parents,
            fields="id, parents",
        ).execute()
        logging.info(f"SUCCESS: File {file_id} moved to folder {new_folder_id}")
    except Exception as e:
        logging.error(f"ERROR moving file {file_id} to {new_folder_id}: {e}")
        raise

# -----------------------
# Discover Team Folders
# -----------------------
def discover_team_folders(service, parent_folder_id: str) -> dict:
    """
    Discover team member folders under the provided parent folder.
    Returns dict mapping member_name -> folder_id.
    """
    team_folders = {}
    try:
        # first-level: cities or categories
        q = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        city_folders = service.files().list(q=q, fields="files(id, name)").execute().get("files", [])

        for city in city_folders:
            logging.info(f"Found city folder: {city.get('name')}")
            # list member folders inside city
            mq = f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            member_folders = service.files().list(q=mq, fields="files(id, name)").execute().get("files", [])
            for member in member_folders:
                name = member.get("name")
                fid = member.get("id")
                logging.info(f"  - Discovered team member folder: {name} (ID: {fid})")
                team_folders[name] = fid
    except Exception as e:
        logging.error(f"ERROR discovering team folders under {parent_folder_id}: {e}")
    return team_folders

# -----------------------
# Get Files To Process
# -----------------------
def get_files_to_process(service, folder_id: str, processed_file_ids: list) -> list:
    """
    List all unprocessed media files in a given folder.
    Returns list of file metadata dicts.
    """
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        files = service.files().list(q=query, fields="files(id, name, mimeType, createdTime, parents)").execute().get("files", [])

        media_files = []
        for f in files:
            mt = f.get("mimeType", "")
            # some audio files uploaded as generic blobs - include common audio/video mime types
            if mt.startswith("audio/") or mt.startswith("video/") or mt in (
                "application/octet-stream", "audio/mpeg", "audio/mp3", "audio/wav"
            ):
                media_files.append(f)

        new_files = [f for f in media_files if f.get("id") not in (processed_file_ids or [])]
        return new_files
    except Exception as e:
        logging.error(f"ERROR getting files to process from folder {folder_id}: {e}")
        return []

# -----------------------
# Quarantine File
# -----------------------
def quarantine_file(service, file_id: str, current_folder_id: str, error_message: str, config: dict):
    """
    Move file to quarantine folder and update its description with the error message.
    If quarantine folder is missing in config, logs and raises.
    """
    try:
        quarantine_id = config["google_drive"].get("quarantine_folder_id")
        if not quarantine_id:
            raise ValueError("quarantine_folder_id not configured in config")

        # Update description with an error note (truncate to safe length)
        desc = f"Quarantined due to error: {error_message}"
        try:
            service.files().update(fileId=file_id, body={"description": desc}).execute()
        except Exception as e:
            logging.warning(f"Could not update file description for {file_id}: {e}")

        # Move file into quarantine folder
        move_file(service, file_id, current_folder_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} quarantined into {quarantine_id}")
    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}")
        # do not re-raise here to avoid double-failure; caller already logs/handles
