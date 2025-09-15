import os
import io
import logging
from googleapiclient.http import MediaIoBaseDownload

# -----------------------
# Download File
# -----------------------
def download_file(service, file_id: str, file_name: str = None) -> str:
    """
    Download a file from Google Drive by file_id.
    Saves to /tmp and returns the local path.
    """
    try:
        request = service.files().get_media(fileId=file_id)

        # Default to file_id if no name provided
        safe_name = file_name or f"{file_id}.dat"
        local_path = os.path.join("/tmp", safe_name)

        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(
                    f"Download progress for {file_name or file_id}: {int(status.progress() * 100)}%"
                )
        logging.info(f"SUCCESS: File download complete: {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"ERROR downloading file {file_name or file_id}: {e}")
        raise


# -----------------------
# Move File
# -----------------------
def move_file(service, file_id: str, old_folder_id: str, new_folder_id: str):
    """Move file from one folder to another."""
    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        service.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=prev_parents,
            fields="id, parents",
        ).execute()
        logging.info(f"SUCCESS: File {file_id} moved to {new_folder_id}")
    except Exception as e:
        logging.error(f"ERROR moving file {file_id}: {e}")
        raise


# -----------------------
# Discover Team Folders
# -----------------------
def discover_team_folders(service, parent_folder_id: str) -> dict:
    """Discover city/team folders inside the parent folder."""
    team_folders = {}
    try:
        city_folders = service.files().list(
            q=f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
        ).execute().get("files", [])

        for city in city_folders:
            logging.info(f"Found city folder: {city['name']}")
            member_folders = service.files().list(
                q=f"'{city['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)",
            ).execute().get("files", [])
            for member in member_folders:
                logging.info(
                    f"  - Discovered team member folder: {member['name']} (ID: {member['id']})"
                )
                team_folders[member["name"]] = member["id"]
    except Exception as e:
        logging.error(f"ERROR discovering team folders: {e}")
    return team_folders


# -----------------------
# Get Files To Process
# -----------------------
def get_files_to_process(service, folder_id: str, processed_file_ids: list) -> list:
    """List all unprocessed media files in a folder."""
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        files = service.files().list(
            q=query, fields="files(id, name, mimeType, createdTime)"
        ).execute().get("files", [])

        media_files = [
            f
            for f in files
            if f["mimeType"].startswith("audio/")
            or f["mimeType"].startswith("video/")
        ]

        new_files = [f for f in media_files if f["id"] not in processed_file_ids]
        return new_files
    except Exception as e:
        logging.error(f"ERROR getting files to process from folder {folder_id}: {e}")
        return []


# -----------------------
# Quarantine File
# -----------------------
def quarantine_file(service, file_id: str, current_folder_id: str, error_message: str, config: dict):
    """Move file to quarantine folder and update description."""
    try:
        quarantine_id = config["google_drive"]["quarantine_folder_id"]

        # Update description with error message
        service.files().update(
            fileId=file_id,
            body={"description": f"Quarantined due to error: {error_message}"},
        ).execute()

        move_file(service, file_id, current_folder_id, quarantine_id)
        logging.info(f"SUCCESS: File {file_id} quarantined.")
    except Exception as e:
        logging.error(f"ERROR quarantining file {file_id}: {e}")
