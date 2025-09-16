import logging
import gspread
from typing import Dict
from google.oauth2 import service_account
import os
import json

# =======================
# Default Headers (47 cols)
# =======================
DEFAULT_HEADERS = [
    "Date",
    "POC Name",
    "Society Name",
    "Visit Type",
    "Meeting Type",
    "Amount Value",
    "Months",
    "Deal Status",
    "Vendor Leads",
    "Society Leads",
    "Opening Pitch Score",
    "Product Pitch Score",
    "Cross-Sell / Opportunity Handling",
    "Closing Effectiveness",
    "Negotiation Strength",
    "Rebuttal Handling",
    "Overall Sentiment",
    "Total Score",
    "% Score",
    "Risks / Unresolved Issues",
    "Improvements Needed",
    "Owner (Who handled the meeting)",
    "Email Id",
    "Kibana ID",
    "Manager",
    "Product Pitch",
    "Team",
    "Media Link",
    "Doc Link",
    "Suggestions & Missed Topics",
    "Pre-meeting brief",
    "Meeting duration (min)",
    "Rapport Building",
    "Improvement Areas",
    "Product Knowledge Displayed",
    "Call Effectiveness and Control",
    "Next Step Clarity and Commitment",
    "Missed Opportunities",
    "Key Discussion Points",
    "Key Questions",
    "Competition Discussion",
    "Action items",
    "Positive Factors",
    "Negative Factors",
    "Customer Needs",
    "Overall Client Sentiment",
    "Feature Checklist Coverage",
    "Manager Email"
]

# =======================
# Authenticate Sheets
# =======================
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets API and return *sheet object* (not client)."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY environment variable")

        creds_info = json.loads(gcp_key_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)

        sheet_id = config["google_sheets"]["sheet_id"]
        sheet = client.open_by_key(sheet_id)
        logging.info("SUCCESS: Authenticated Google Sheets")
        return sheet
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate with Google Sheets: {e}")
        raise

# =======================
# Write Analysis Results
# =======================
def write_analysis_result(gsheets_sheet, analysis_data: Dict, config: Dict):
    """Append a normalized analysis row into the Results sheet."""
    try:
        ws = gsheets_sheet.worksheet(config["google_sheets"]["results_tab_name"])

        # Guarantee all headers exist in correct order
        row = []
        for h in DEFAULT_HEADERS:
            row.append(analysis_data.get(h, "N/A") or "N/A")

        ws.append_row(row, value_input_option="RAW")

        target = analysis_data.get("Society Name") or analysis_data.get("File Name") or "Unknown"
        logging.info(f"SUCCESS: Wrote analysis result for '{target}'")
    except Exception as e:
        logging.error(f"ERROR writing analysis result: {e}")
        raise

# =======================
# Ledger Update
# =======================
def update_ledger(gsheets_sheet, file_id: str, status: str, error_msg: str, config: Dict, file_name: str):
    """Update the ledger tab with processing status for each file."""
    try:
        ws = gsheets_sheet.worksheet(config["google_sheets"]["ledger_tab_name"])
        records = ws.get_all_records()

        row_index = None
        for i, r in enumerate(records, start=2):  # row 1 = headers
            if str(r.get("File ID")) == str(file_id):
                row_index = i
                break

        if row_index:
            ws.update_cell(row_index, 3, status)       # Status column
            ws.update_cell(row_index, 4, error_msg[:500])  # Error column
        else:
            ws.append_row(
                [file_id, file_name, status, error_msg[:500]],
                value_input_option="RAW"
            )

        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for file {file_name}: {e}")
