import logging
import time
from typing import Dict, Set

import gspread
from tenacity import retry, stop_after_attempt, wait_exponential

# =======================
# Constants
# =======================
DEFAULT_HEADERS = [
    "Date", "POC Name", "Society Name", "Visit Type", "Meeting Type", "Amount Value",
    "Months", "Deal Status", "Vendor Leads", "Society Leads", "Opening Pitch Score",
    "Product Pitch Score", "Cross-Sell / Opportunity Handling", "Closing Effectiveness",
    "Negotiation Strength", "Overall Sentiment", "Total Score", "% Score",
    "Risks / Unresolved Issues", "Improvements Needed", "Owner", "Email Id",
    "Kibana ID", "Manager", "Manager Email", "Product Pitch", "Team", "Media Link", "Doc Link",
    "Suggestions & Missed Topics", "Pre-meeting brief", "Meeting duration (min)",
    "Rebuttal Handling", "Rapport Building", "Improvement Areas",
    "Product Knowledge Displayed", "Call Effectiveness and Control",
    "Next Step Clarity and Commitment", "Missed Opportunities", "Key Discussion Points",
    "Key Questions", "Competition Discussion", "Action items", "Positive Factors",
    "Negative Factors", "Customer Needs", "Overall Client Sentiment", "Feature Checklist Coverage"
]

RETRY_CONFIG = {
    'wait': wait_exponential(multiplier=2, min=5, max=60),
    'stop': stop_after_attempt(5),
}

# =======================
# Sheets Operations with Retry Logic
# =======================
@retry(**RETRY_CONFIG)
def get_processed_file_ids(gsheets_client: gspread.Client, config: Dict) -> Set[str]:
    """Retrieves the set of already processed file IDs from the ledger tab."""
    try:
        logging.info("Retrieving processed file ledger from Google Sheets...")
        sheet_id = config['google_sheets']['sheet_id']
        ledger_tab_name = config['google_sheets']['ledger_tab_name']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(ledger_tab_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"Ledger tab '{ledger_tab_name}' not found. Creating it.")
            worksheet = spreadsheet.add_worksheet(title=ledger_tab_name, rows="100", cols="4")
            worksheet.append_row(["File ID", "Status", "Timestamp", "Error Message"])
            return set()

        processed_ids = set(worksheet.col_values(1)[1:])
        logging.info(f"Found {len(processed_ids)} file IDs in the ledger.")
        return processed_ids
    except Exception as e:
        logging.error(f"ERROR: Could not retrieve processed file ledger: {e}")
        raise

@retry(**RETRY_CONFIG)
def write_results(gsheets_client: gspread.Client, data: Dict[str, str], config: Dict):
    """Writes the analysis results to the main results tab."""
    try:
        logging.info("Attempting to write data to Google Sheets...")
        sheet_id = config['google_sheets']['sheet_id']
        results_tab_name = config['google_sheets']['results_tab_name']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(results_tab_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"Results tab '{results_tab_name}' not found. Creating it.")
            worksheet = spreadsheet.add_worksheet(title=results_tab_name, rows="1000", cols="50")
            
        headers = worksheet.row_values(1)
        if not headers:
            logging.info("No headers found in results sheet. Writing default headers.")
            worksheet.append_row(DEFAULT_HEADERS, value_input_option="USER_ENTERED")
            headers = DEFAULT_HEADERS

        row_to_insert = [data.get(header, "") for header in headers]
        worksheet.append_row(row_to_insert, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Data for '{data.get('Society Name', '')}' written to Google Sheets.")
    except Exception as e:
        logging.error(f"ERROR: Failed to write to Google Sheets: {e}")
        raise

@retry(**RETRY_CONFIG)
def update_ledger(gsheets_client: gspread.Client, file_id: str, status: str, error_message: str, config: Dict):
    """Adds or updates a file's status in the processed ledger."""
    try:
        logging.info(f"Updating ledger for file {file_id} with status: {status}")
        sheet_id = config['google_sheets']['sheet_id']
        ledger_tab_name = config['google_sheets']['ledger_tab_name']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(ledger_tab_name)
        
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        worksheet.append_row([file_id, status, timestamp, error_message], value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Ledger updated for file {file_id}.")
    except Exception as e:
        logging.error(f"ERROR: Failed to update ledger: {e}")
        raise

