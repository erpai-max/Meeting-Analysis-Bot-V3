import logging
import gspread
from typing import Dict, List
from google.oauth2 import service_account
import os
import json

# =======================
# Authenticate Sheets
# =======================
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets API."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY environment variable")

        creds_info = json.loads(gcp_key_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=scopes
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config["google_sheets"]["sheet_id"])
        return sheet
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate with Google Sheets: {e}")
        raise

# =======================
# Write Analysis Results
# =======================
def write_analysis_result(gsheets_client, analysis_data: Dict, config: Dict):
    """Append a normalized analysis row into the Results sheet."""
    try:
        ws = gsheets_client.worksheet(config["google_sheets"]["results_tab_name"])
        headers = config.get("sheets_headers", [])

        row = [analysis_data.get(h, "N/A") or "N/A" for h in headers]

        ws.append_row(row, value_input_option="RAW")
        logging.info(
            f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name','')}'"
        )
    except Exception as e:
        logging.error(f"ERROR writing analysis result: {e}")
        raise

# =======================
# Ledger Update
# =======================
def update_ledger(
    gsheets_client,
    file_id: str,
    status: str,
    error_msg: str,
    config: Dict,
    file_name: str,
):
    """Update the ledger tab with processing status for each file."""
    try:
        ws = gsheets_client.worksheet(config["google_sheets"]["ledger_tab_name"])
        records = ws.get_all_records()

        row_index = None
        for i, r in enumerate(records, start=2):  # row 1 = headers
            if str(r.get("File ID")) == str(file_id):
                row_index = i
                break

        if row_index:
            ws.update_cell(row_index, 3, status)  # Status
            ws.update_cell(row_index, 4, error_msg[:500])  # Error
        else:
            ws.append_row(
                [file_id, file_name, status, error_msg[:500]],
                value_input_option="RAW",
            )

        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for file {file_name}: {e}")

# =======================
# Fetch Processed File IDs
# =======================
def get_processed_file_ids(gsheets_client, config: Dict) -> List[str]:
    """Return all processed file IDs from the ledger."""
    try:
        ws = gsheets_client.worksheet(config["google_sheets"]["ledger_tab_name"])
        records = ws.get_all_records()
        return [str(r.get("File ID", "")).strip() for r in records if r.get("File ID")]
    except Exception as e:
        logging.error(f"ERROR fetching processed file IDs: {e}")
        return []
