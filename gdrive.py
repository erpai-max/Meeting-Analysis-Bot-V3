# gdrive.py
import os
import io
import logging
from googleapiclient.http import MediaIoBaseDownload

def download_file(service, file_id: str, file_name: str = None) -> str:
    try:
        request = service.files().get_media(fileId=file_id)
        safe_name = file_name or f"{file_id}.dat"
        # sanitize file name
        safe_name = safe_name.replace("/", "_").replace("\\", "_")
        local_path = os.path.join("/tmp", safe_name)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"Download progress for {safe_name}: {int(status.progress() * 100)}%")
        fh.close()
        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        raise

def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        service.files().update(fileId=file_id, addParents=new_folder_id, removeParents=prev_parents, fields="id, parents").execute()
        logging.info(f"SUCCESS: File {file_id} moved to {new_folder_id}")
    except Exception as e:
        logging.error(f"ERROR moving file {file_id}: {e}")
        raise

def discover_team_folders(service, parent_folder_id: str) -> dict:
    team_folders = {}
    try:
        city_folders = service.files().list(q=f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", fields="files(id, name)").execute().get("files", [])
        for city in city_folders:
            member_folders = service.files().list(q=f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", fields="files(id, name)").execute().get("files", [])
            for member in member_folders:
                team_folders[member["name"]] = member["id"]
    except Exception as e:
        logging.error(f"ERROR discovering team folders: {e}")
    return team_folders

def get_files_to_process(service, folder_id: str, processed_file_ids: list) -> list:
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        files = service.files().list(q=query, fields="files(id, name, mimeType, parents, createdTime)").execute().get("files", [])
        media_files = [f for f in files if f.get("mimeType", "").startswith("audio/") or f.get("mimeType", "").startswith("video/")]
        new_files = [f for f in media_files if f["id"] not in (processed_file_ids or [])]
        return new_files
    except Exception as e:
        logging.error(f"ERROR getting files to process from folder {folder_id}: {e}")
        return []

def quarantine_file(service, file_id: str, current_folder_id: str, error_message: str, config: dict):
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]
        service.files().update(fileId=file_id, body={"description": f"Quarantined due to error: {error_message}"}).execute()
        move_file(service, file_id, current_folder_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} quarantined.")
    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}")
