import os
import io
import logging
import re
from googleapiclient.http import MediaIoBaseDownload

def _safe_name(name: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", name or "file.dat")

def download_file(service, file_id: str, file_name: str = None) -> str:
    try:
        request = service.files().get_media(fileId=file_id)
        safe = _safe_name(file_name or f"{file_id}.dat")
        local_path = os.path.join("/tmp", safe)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"Download progress for {file_name or file_id}: {int(status.progress()*100)}%")
        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        raise

def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
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

def discover_team_folders(service, parent_folder_id: str) -> dict:
    team_folders = {}
    try:
        cities = service.files().list(
            q=f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
        ).execute().get("files", [])

        for city in cities:
            logging.info(f"Found city folder: {city['name']}")
            members = service.files().list(
                q=f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)",
            ).execute().get("files", [])
            for member in members:
                logging.info(f"  - Discovered team member folder: {member['name']} (ID: {member['id']})")
                team_folders[member["name"]] = member["id"]
    except Exception as e:
        logging.error(f"ERROR discovering team folders: {e}")
    return team_folders

def get_files_to_process(service, folder_id: str, processed_file_ids: list) -> list:
    try:
        files = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name, mimeType, createdTime, parents)"
        ).execute().get("files", [])

        media_files = [f for f in files if f["mimeType"].startswith(("audio/", "video/"))]
        new_files = [f for f in media_files if f["id"] not in processed_file_ids]
        return new_files
    except Exception as e:
        logging.error(f"ERROR getting files from folder {folder_id}: {e}")
        return []

def quarantine_file(service, file_id: str, current_folder_id: str, error_message: str, config: dict):
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]
        service.files().update(
            fileId=file_id,
            body={"description": f"Quarantined due to error: {error_message}"},
        ).execute()
        move_file(service, file_id, current_folder_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} quarantined with reason: {error_message}")
    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}")
