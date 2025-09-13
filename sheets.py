import logging
import time
from typing import Dict, List, Any, Set
import datetime

import gspread
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

# =======================
# Retry Config
# =======================
RETRY_CONFIG = {
    "wait": wait_exponential(multiplier=2, min=5, max=60),
    "stop": stop_after_attempt(5),
    "reraise": True,
}

# =======================
# Ledger Functions
# =======================
@retry(**RETRY_CONFIG)
def get_processed_file_ids(gsheets_client: gspread.Client, config: Dict) -> Set[str]:
    """Reads the ledger sheet and returns all previously processed file IDs."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"]["ledger_tab_name"]
        try:
            ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(ledger_tab)
        except Exception:
            logging.warning(f"Ledger tab '{ledger_tab}' not found. Creating it.")
            spreadsheet = gsheets_client.open_by_key(sheet_id)
            ledger_ws = spreadsheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
            ledger_ws.append_row(["File ID", "File Name", "Status", "Error", "Timestamp"])
            return set()

        records = ledger_ws.get_all_records()
        file_ids = {r["File ID"] for r in records if "File ID" in r and r["File ID"]}
        logging.info(f"Found {len(file_ids)} file IDs in the ledger.")
        return file_ids
    except Exception as e:
        logging.error(f"ERROR reading ledger: {e}")
        return set()


@retry(**RETRY_CONFIG)
def update_ledger(
    gsheets_client: gspread.Client,
    file_id: str,
    status: str,
    error: str,
    config: Dict,
):
    """Appends a new row to the ledger for tracking."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"]["ledger_tab_name"]

        try:
            ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(ledger_tab)
        except Exception:
            spreadsheet = gsheets_client.open_by_key(sheet_id)
            ledger_ws = spreadsheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
            ledger_ws.append_row(["File ID", "File Name", "Status", "Error", "Timestamp"])

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ledger_ws.append_row([file_id, "", status, error, timestamp])
        logging.info(f"SUCCESS: Ledger appended new row for file {file_id}.")
    except Exception as e:
        logging.error(f"ERROR updating ledger for {file_id}: {e}")


# =======================
# Results Sheet Functions
# =======================
@retry(**RETRY_CONFIG)
def write_results(gsheets_client: gspread.Client, data: Dict[str, Any], config: Dict):
    """Appends structured analysis results to the Results sheet."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"]["results_tab_name"]
        spreadsheet = gsheets_client.open_by_key(sheet_id)

        try:
            ws = spreadsheet.worksheet(results_tab)
        except Exception:
            logging.warning(f"Results tab '{results_tab}' not found. Creating it.")
            ws = spreadsheet.add_worksheet(title=results_tab, rows="1000", cols="50")
            if "sheets_headers" in config:
                ws.append_row(config["sheets_headers"], value_input_option="USER_ENTERED")

        headers = ws.row_values(1)
        if not headers:
            headers = config.get("sheets_headers", [])
            if headers:
                ws.append_row(headers, value_input_option="USER_ENTERED")

        row = [str(data.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Wrote analysis result for '{data.get('Society Name', 'Unknown')}'")
    except Exception as e:
        logging.error(f"ERROR: Failed to write analysis result: {e}")
        raise


def get_all_results(gsheets_client: gspread.Client, config: Dict) -> List[Dict]:
    """Fetches all rows from the Results sheet for dashboard export."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"]["results_tab_name"]
        ws = gsheets_client.open_by_key(sheet_id).worksheet(results_tab)
        records = ws.get_all_records()
        logging.info(f"Exported {len(records)} rows from Results sheet.")
        return records
    except Exception as e:
        logging.error(f"ERROR: Could not fetch results for dashboard export: {e}")
        return []
