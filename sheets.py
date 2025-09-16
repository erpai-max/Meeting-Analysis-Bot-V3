import logging
from typing import Dict, List
import datetime

# -----------------------
# Default Headers (Google Sheets)
# -----------------------
DEFAULT_HEADERS = [
    "Date", "POC Name", "Society Name", "Visit Type", "Meeting Type",
    "Amount Value", "Months", "Deal Status", "Vendor Leads", "Society Leads",
    "Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness", "Negotiation Strength", "Rebuttal Handling",
    "Overall Sentiment", "Total Score", "% Score",
    "Risks / Unresolved Issues", "Improvements Needed",
    "Owner (Who handled the meeting)", "Email Id", "Kibana ID",
    "Manager", "Product Pitch", "Team", "Media Link", "Doc Link",
    "Suggestions & Missed Topics", "Pre-meeting brief", "Meeting duration (min)",
    "Rapport Building", "Improvement Areas", "Product Knowledge Displayed",
    "Call Effectiveness and Control", "Next Step Clarity and Commitment",
    "Missed Opportunities", "Key Discussion Points", "Key Questions",
    "Competition Discussion", "Action items", "Positive Factors", "Negative Factors",
    "Customer Needs", "Overall Client Sentiment", "Feature Checklist Coverage",
    "Manager Email"
]

LEDGER_HEADERS = ["File ID", "File Name", "Status", "Error", "Timestamp"]

# -----------------------
# Safe Normalization (Optional for JSON)
# -----------------------
HEADER_MAP = {h: h.lower().replace(" ", "_").replace("/", "_") for h in DEFAULT_HEADERS}

def normalize_for_json(record: dict) -> dict:
    """Convert sheet-style headers into snake_case for JSON export."""
    normalized = {}
    for old_key, value in record.items():
        new_key = HEADER_MAP.get(old_key, old_key.lower().replace(" ", "_"))
        normalized[new_key] = value
    return normalized

# -----------------------
# Ledger Functions
# -----------------------
def get_processed_file_ids(gsheets_client, config) -> List[str]:
    """Reads the ledger sheet and returns all previously processed file IDs."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(
            config["google_sheets"].get("ledger_tab_name", "Ledger")
        )
        records = ledger_ws.get_all_records()
        file_ids = [r["File ID"] for r in records if r.get("File ID")]
        logging.info(f"Found {len(file_ids)} file IDs in the ledger.")
        return file_ids
    except Exception as e:
        logging.warning(f"Ledger not found or unreadable, returning empty list: {e}")
        return []

def update_ledger(gsheets_client, file_id: str, status: str, error: str, config: Dict, file_name: str = "Unknown"):
    """Appends a new row to the ledger for tracking."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Ledger")

        try:
            ledger_ws = gsheets_client.open_by_key(sheet_id).worksheet(ledger_tab)
        except Exception:
            spreadsheet = gsheets_client.open_by_key(sheet_id)
            ledger_ws = spreadsheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
            ledger_ws.append_row(LEDGER_HEADERS)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ledger_ws.append_row([file_id, file_name or "Unknown", status, error, timestamp])
        logging.info(f"SUCCESS: Ledger updated for file {file_id} ({file_name}) → {status}")
    except Exception as e:
        logging.error(f"ERROR updating ledger for {file_id}: {e}")

# -----------------------
# Main Data Sheet Functions
# -----------------------
def write_analysis_result(gsheets_client, analysis_data: Dict[str, str], config: Dict):
    """Appends structured analysis results to the main Google Sheet."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Results")
        ws = gsheets_client.open_by_key(sheet_id).worksheet(results_tab)

        headers = ws.row_values(1)
        if not headers:
            ws.append_row(DEFAULT_HEADERS, value_input_option="USER_ENTERED")
            headers = DEFAULT_HEADERS

        row = [analysis_data.get(h, "") for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name', 'Unknown')}'")
    except Exception as e:
        logging.error(f"ERROR writing analysis result: {e}")

def get_all_results(gsheets_client, config: Dict) -> List[Dict]:
    """Fetches all rows from the Results sheet for dashboard export."""
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Results")
        ws = gsheets_client.open_by_key(sheet_id).worksheet(results_tab)
        records = ws.get_all_records()
        logging.info(f"Exported {len(records)} rows from Results sheet.")
        return records
    except Exception as e:
        logging.error(f"ERROR fetching results for dashboard: {e}")
        return []
