# ===================================================================
# main.py — Meeting Analysis Bot (FREE Gemini, Dec 2025)
# ===================================================================

# --- Quiet gRPC/absl logs BEFORE importing Google/gRPC libraries ---
import os
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_CPP_VERBOSITY", "NONE")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3")

import yaml
import logging
import json
import sys
import time
import datetime as dt

from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

import gdrive
import analysis
import sheets

# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# -------------------------------------------------------------------
# AUTHENTICATION
# -------------------------------------------------------------------
def authenticate_google(config):
    """Authenticate Google Drive + Google Sheets."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("GCP_SA_KEY environment variable not set")

        creds_info = json.loads(gcp_key_str)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]

        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=scopes
        )

        drive_service = build("drive", "v3", credentials=creds)
        logging.info("SUCCESS: Authenticated Google Drive")

        client = gspread.authorize(creds)
        sheet = client.open_by_key(config["google_sheets"]["sheet_id"])
        sheets.ensure_tabs_exist(sheet, config)
        logging.info("SUCCESS: Authenticated Google Sheets")

        return drive_service, sheet

    except Exception as e:
        logging.error(f"CRITICAL: Google authentication failed: {e}", exc_info=True)
        return None, None

# -------------------------------------------------------------------
# DASHBOARD EXPORT
# -------------------------------------------------------------------
def export_data_for_dashboard(gsheets_sheet, config):
    dashboard_cfg = config.get("dashboard", {})
    if not dashboard_cfg:
        logging.info("Dashboard config not found. Skipping export.")
        return

    output_dir = dashboard_cfg.get("output_dir", "docs")
    filename = dashboard_cfg.get("filename", "dashboard_data.json")
    output_path = os.path.join(output_dir, filename)

    try:
        records = sheets.get_all_results(gsheets_sheet, config)

        strip_columns = dashboard_cfg.get("strip_columns", [])
        if strip_columns:
            cleaned = []
            for r in records:
                cleaned.append({k: v for k, v in r.items() if k not in strip_columns})
            records = cleaned

        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        logging.info(f"Dashboard export complete: {output_path}")

        if dashboard_cfg.get("copy_html_from_root", False):
            import shutil
            if os.path.exists("dashboard.html"):
                shutil.copyfile("dashboard.html", os.path.join(output_dir, "index.html"))
                logging.info("Dashboard HTML copied to docs/index.html")

    except Exception as e:
        logging.error(f"Dashboard export failed: {e}", exc_info=True)

# -------------------------------------------------------------------
# QUARANTINE RETRY
# -------------------------------------------------------------------
def retry_quarantined_files(drive_service, gsheets_sheet, config):
    try:
        quarantine_cfg = config.get("quarantine", {})
        hours = int(quarantine_cfg.get("auto_retry_after_hours", 24))
        if hours <= 0:
            return

        quarantine_id = config["google_drive"].get("quarantine_folder_id")
        parent_id = config["google_drive"].get("parent_folder_id")

        if not quarantine_id or not parent_id:
            return

        cooldown = hours * 3600
        now_epoch = time.time()

        files = drive_service.files().list(
            q=f"'{quarantine_id}' in parents and trashed=false",
            fields="files(id,name,modifiedTime)"
        ).execute().get("files", [])

        for f in files:
            modified = f.get("modifiedTime")
            if not modified:
                continue

            modified_dt = dt.datetime.fromisoformat(modified.replace("Z", "+00:00"))
            if now_epoch - modified_dt.timestamp() > cooldown:
                gdrive.move_file(drive_service, f["id"], quarantine_id, parent_id)
                sheets.update_ledger(
                    gsheets_sheet,
                    f["id"],
                    "Pending",
                    f"Auto-retry after {hours}h",
                    config,
                    f["name"]
                )
                logging.info(f"Auto-retried quarantined file: {f['name']}")

    except Exception as e:
        logging.error(f"Error during quarantine retry: {e}", exc_info=True)

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    logging.info("=== Starting Meeting Analysis Bot (FREE Gemini) ===")

    config_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
    if not os.path.exists(config_path):
        logging.error("CRITICAL: config.yaml not found")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    drive_service, gsheets_sheet = authenticate_google(config)
    if not drive_service or not gsheets_sheet:
        sys.exit(1)

    processed_ids = set()
    try:
        processed_ids = sheets.get_processed_file_ids(gsheets_sheet, config)
    except Exception:
        logging.warning("Could not read processed IDs. Will process all files.")

    retry_quarantined_files(drive_service, gsheets_sheet, config)

    parent_folder_id = config["google_drive"]["parent_folder_id"]
    team_folders = gdrive.discover_team_folders(drive_service, parent_folder_id)

    max_files = int(config.get("processing", {}).get("max_files_per_run", 999999))
    sleep_sec = float(config.get("processing", {}).get("sleep_between_files_sec", 1.5))

    processed_this_run = 0

    for member_name, folder_id in team_folders.items():
        logging.info(f"Checking folder for {member_name}")

        files = gdrive.get_files_to_process(drive_service, folder_id, processed_ids)

        for file_meta in files:
            if processed_this_run >= max_files:
                logging.info("Reached max_files_per_run limit.")
                break

            file_id = file_meta["id"]
            file_name = file_meta.get("name", file_id)

            try:
                analysis.process_single_file(
                    drive_service,
                    gsheets_sheet,
                    file_meta,
                    member_name,
                    config
                )

                processed_folder_id = config["google_drive"].get("processed_folder_id")
                if processed_folder_id:
                    gdrive.move_file(drive_service, file_id, folder_id, processed_folder_id)

                processed_ids.add(file_id)
                processed_this_run += 1

            except Exception as e:
                error_summary = f"{type(e).__name__}: {str(e)[:150]}"
                logging.error(f"File failed: {file_name} → {error_summary}")

                try:
                    gdrive.quarantine_file(
                        drive_service,
                        file_id,
                        folder_id,
                        error_summary,
                        config
                    )
                    sheets.update_ledger(
                        gsheets_sheet,
                        file_id,
                        "Quarantined",
                        error_summary,
                        config,
                        file_name
                    )
                except Exception as qe:
                    logging.error(f"Failed to quarantine {file_name}: {qe}")

            if sleep_sec > 0:
                time.sleep(sleep_sec)

    export_data_for_dashboard(gsheets_sheet, config)

    logging.info(f"=== Run completed. Files processed: {processed_this_run} ===")

# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
