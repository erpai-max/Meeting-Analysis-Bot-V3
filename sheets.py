import logging
import gspread
from typing import Dict, List
from google.oauth2 import service_account
import os
import json
import datetime

# ---------- Default Headers (47) ----------
DEFAULT_HEADERS = [
    "Date", "POC Name", "Society Name", "Visit Type", "Meeting Type",
    "Amount Value", "Months", "Deal Status", "Vendor Leads", "Society Leads",
    "Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness", "Negotiation Strength", "Rebuttal Handling",
    "Overall Sentiment", "Total Score", "% Score", "Risks / Unresolved Issues",
    "Improvements Needed", "Owner (Who handled the meeting)", "Email Id", "Kibana ID",
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

def authenticate_google_sheets(config: Dict):
    """Return a gspread Spreadsheet (not Client)."""
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        raise ValueError("Missing GCP_SA_KEY environment variable")

    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(config["google_sheets"]["sheet_id"])
    logging.info("SUCCESS: Authenticated Google Sheets")
    # Ensure tabs exist
    ensure_tabs_exist(sheet, config)
    return sheet

def ensure_tabs_exist(sheet, config: Dict):
    """Ensure 'Analysis Results' and 'Processed Ledger' exist with headers."""
    results_tab = config["google_sheets"]["results_tab_name"]
    ledger_tab = config["google_sheets"]["ledger_tab_name"]

    # Results
    try:
        ws = sheet.worksheet(results_tab)
    except Exception:
        ws = sheet.add_worksheet(title=results_tab, rows="1000", cols=str(len(DEFAULT_HEADERS)))
        ws.append_row(DEFAULT_HEADERS, value_input_option="RAW")

    # If header row missing/empty, write it
    try:
        headers = ws.row_values(1)
        if not headers:
            ws.update(f"A1:{chr(64+len(DEFAULT_HEADERS))}1", [DEFAULT_HEADERS])
    except Exception:
        pass

    # Ledger
    try:
        lw = sheet.worksheet(ledger_tab)
    except Exception:
        lw = sheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
        lw.append_row(LEDGER_HEADERS, value_input_option="RAW")

    try:
        lheaders = lw.row_values(1)
        if not lheaders:
            lw.update("A1:E1", [LEDGER_HEADERS])
    except Exception:
        pass

def write_analysis_result(sheet, analysis_data: Dict, config: Dict):
    """Append a normalized analysis row into the Results sheet."""
    try:
        ws = sheet.worksheet(config["google_sheets"]["results_tab_name"])
        row = [analysis_data.get(h, "N/A") or "N/A" for h in DEFAULT_HEADERS]
        ws.append_row(row, value_input_option="RAW")
        logging.info(f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name','')}'")
    except Exception as e:
        logging.error(f"ERROR writing analysis result: {e}")
        raise

def update_ledger(sheet, file_id: str, status: str, error_msg: str, config: Dict, file_name: str):
    """Update the ledger with processing status for each file."""
    try:
        ws = sheet.worksheet(config["google_sheets"]["ledger_tab_name"])
        records = ws.get_all_records()
        row_index = None
        for i, r in enumerate(records, start=2):  # row 1 = headers
            if str(r.get("File ID")) == str(file_id):
                row_index = i
                break

        if row_index:
            ws.update_cell(row_index, 3, status)  # Status
            ws.update_cell(row_index, 4, (error_msg or "")[:500])  # Error
            ws.update_cell(
                row_index, 5,
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            ws.append_row(
                [file_id, file_name, status, (error_msg or "")[:500],
                 datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                value_input_option="RAW"
            )
        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for file {file_name}: {e}")

def get_processed_file_ids(sheet, config) -> List[str]:
    try:
        ws = sheet.worksheet(config["google_sheets"]["ledger_tab_name"])
        records = ws.get_all_records()
        return [str(r.get("File ID")) for r in records if str(r.get("Status")).lower() == "processed"]
    except Exception as e:
        logging.warning(f"Ledger read failed; defaulting to empty processed list: {e}")
        return []

def get_all_results(sheet, config) -> List[Dict]:
    try:
        ws = sheet.worksheet(config["google_sheets"]["results_tab_name"])
        return ws.get_all_records()
    except Exception as e:
        logging.error(f"ERROR fetching results for dashboard export: {e}")
        return []
