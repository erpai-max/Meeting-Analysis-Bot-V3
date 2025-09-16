import logging
from typing import Dict, List
import datetime
import json
import gspread

# -----------------------
# Ledger Headers
# -----------------------
LEDGER_HEADERS = ["File ID", "File Name", "Status", "Error", "Timestamp"]

# -----------------------
# Ledger Functions
# -----------------------
def get_processed_file_ids(gsheets_client, config) -> List[str]:
    """Reads the ledger sheet and returns all previously processed file IDs."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Processed Ledger")
        ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(ledger_tab)

        records = ledger_ws.get_all_records()
        file_ids = [r.get("File ID") for r in records if r.get("File ID")]
        logging.info(f"Found {len(file_ids)} file IDs in the ledger.")
        return file_ids
    except Exception as e:
        logging.warning(f"Ledger not found or unreadable, returning empty list: {e}")
        return []


def update_ledger(gsheets_client, file_id: str, status: str, error: str, config: Dict, file_name: str = "Unknown"):
    """Appends a new row to the ledger for tracking."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Processed Ledger")

        try:
            ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(ledger_tab)
        except Exception:
            spreadsheet = gsheets_client.open_by_key(sheet_id)
            ledger_ws = spreadsheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
            ledger_ws.append_row(LEDGER_HEADERS)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ledger_ws.append_row([file_id, file_name or "Unknown", status, error, timestamp])
        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for {file_id}: {e}")


# -----------------------
# Main Data Sheet Functions
# -----------------------
def ensure_headers(ws, config: Dict):
    """Ensure headers in the sheet match config.yaml exactly."""
    expected_headers = config["sheets_headers"]
    actual_headers = ws.row_values(1)

    if actual_headers != expected_headers:
        logging.warning("Header mismatch detected → fixing sheet headers.")
        # Clear old headers and rewrite correct ones
        ws.resize(rows=1)  # keep only 1 row
        ws.clear()
        ws.append_row(expected_headers, value_input_option="USER_ENTERED")
        return expected_headers
    return actual_headers


def write_analysis_result(gsheets_client, analysis_data: Dict[str, str], config: Dict):
    """Appends structured analysis results to the main Google Sheet."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Analysis Results")

        # Ensure worksheet exists
        try:
            ws = gsheets_client.open_by_key(sheet_id).worksheet(results_tab)
        except Exception:
            spreadsheet = gsheets_client.open_by_key(sheet_id)
            ws = spreadsheet.add_worksheet(title=results_tab, rows="1000", cols="50")
            ws.append_row(config["sheets_headers"], value_input_option="USER_ENTERED")

        headers = ensure_headers(ws, config)

        # Prepare row
        row = []
        for h in headers:
            val = analysis_data.get(h, "")
            if isinstance(val, (list, dict)):
                val = json.dumps(val, ensure_ascii=False)
            row.append(str(val) if val is not None else "")

        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name', 'Unknown')}'")
    except Exception as e:
        logging.error(f"ERROR: Failed to write analysis result: {e}")


def get_all_results(gsheets_client, config: Dict) -> List[Dict]:
    """Fetches all rows from the Results sheet for dashboard export."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Analysis Results")
        ws = gsheets_client.open_by_key(sheet_id).worksheet(results_tab)

        ensure_headers(ws, config)

        records = ws.get_all_records()
        logging.info(f"Exported {len(records)} rows from Results sheet.")
        return records
    except Exception as e:
        logging.error(f"ERROR: Could not fetch results for dashboard export: {e}")
        return []
