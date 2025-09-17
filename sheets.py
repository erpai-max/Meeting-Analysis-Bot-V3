# sheets.py
import logging
import gspread
from typing import Dict, List
from google.oauth2 import service_account
import os
import json
import datetime

# Default headers expected in Google Sheet (must match config.yaml order)
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

# Authenticate Sheets (returns gspread.Client)
def authenticate_google_sheets(config: Dict):
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY environment variable")

        creds_info = json.loads(gcp_key_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate with Google Sheets: {e}")
        raise

# Get processed file ids from ledger
def get_processed_file_ids(gsheets_client, config) -> List[str]:
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Processed Ledger")
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        try:
            ledger_ws = spreadsheet.worksheet(ledger_tab)
        except Exception:
            logging.info("Ledger sheet not found, returning empty list.")
            return []
        records = ledger_ws.get_all_records()
        file_ids = [r.get("File ID") for r in records if r.get("File ID")]
        logging.info(f"Found {len(file_ids)} file IDs in the ledger.")
        return file_ids
    except Exception as e:
        logging.warning(f"Ledger not found or unreadable, returning empty list: {e}")
        return []

# Append/update ledger row
def update_ledger(gsheets_client, file_id: str, status: str, error: str, config: Dict, file_name: str = "Unknown"):
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Processed Ledger")
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        try:
            ledger_ws = spreadsheet.worksheet(ledger_tab)
        except Exception:
            ledger_ws = spreadsheet.add_worksheet(title=ledger_tab, rows="1000", cols="5")
            ledger_ws.append_row(LEDGER_HEADERS, value_input_option="USER_ENTERED")

        records = ledger_ws.get_all_records()
        row_index = None
        for i, r in enumerate(records, start=2):  # header at row 1
            if str(r.get("File ID")) == str(file_id):
                row_index = i
                break

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if row_index:
            ledger_ws.update_cell(row_index, 3, status)  # Status col
            ledger_ws.update_cell(row_index, 4, error[:500])  # Error col
            ledger_ws.update_cell(row_index, 5, timestamp)
        else:
            ledger_ws.append_row([file_id, file_name, status, error[:500], timestamp], value_input_option="USER_ENTERED")

        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for file {file_name}: {e}")

# Write analysis result row to Results tab
def write_analysis_result(gsheets_client, analysis_data: Dict[str, str], config: Dict):
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Analysis Results")
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        try:
            ws = spreadsheet.worksheet(results_tab)
        except Exception:
            ws = spreadsheet.add_worksheet(title=results_tab, rows="2000", cols=str(len(DEFAULT_HEADERS)))
            ws.append_row(DEFAULT_HEADERS, value_input_option="USER_ENTERED")

        # build row in the order of headers
        headers = ws.row_values(1)
        if not headers:
            headers = DEFAULT_HEADERS

        row = [analysis_data.get(h, "N/A") if analysis_data.get(h, "") != "" else "N/A" for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name','Unknown')}'")
    except Exception as e:
        logging.error(f"ERROR: Failed to write analysis result: {e}")
        raise

# Fetch all results (for dashboard export)
def get_all_results(gsheets_client, config: Dict) -> List[Dict]:
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Analysis Results")
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        try:
            ws = spreadsheet.worksheet(results_tab)
        except Exception:
            logging.info("Results worksheet not found.")
            return []
        records = ws.get_all_records()
        logging.info(f"Exported {len(records)} rows from Results sheet.")
        return records
    except Exception as e:
        logging.error(f"ERROR: Could not fetch results for dashboard export: {e}")
        return []
