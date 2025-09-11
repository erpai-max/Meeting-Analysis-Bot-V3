import logging
from typing import Dict, Set, Optional

import gspread
from tenacity import retry, stop_after_attempt, wait_exponential

# =======================
# Retry Configuration
# =======================
RETRY_CONFIG = {
    'wait': wait_exponential(multiplier=1, min=4, max=60),
    'stop': stop_after_attempt(5),
}

# =======================
# Ledger Operations
# =======================
@retry(**RETRY_CONFIG)
def get_processed_file_ids(gsheets_client, config: Dict) -> Set[str]:
    """Retrieves the set of all file IDs from the 'Processed Ledger' tab."""
    logging.info("Retrieving processed file ledger from Google Sheets...")
    try:
        sheet_id = config['google_sheets']['sheet_id']
        ledger_tab_name = config['google_sheets']['ledger_tab_name']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        try:
            worksheet = spreadsheet.worksheet(ledger_tab_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"Ledger tab '{ledger_tab_name}' not found. Creating it.")
            worksheet = spreadsheet.add_worksheet(title=ledger_tab_name, rows=100, cols=10)
            worksheet.append_row(["File ID", "Status", "Timestamp", "Error Message"])
            return set()

        # Get all values from the first column (File ID), skipping the header
        processed_ids = set(worksheet.col_values(1)[1:])
        logging.info(f"Found {len(processed_ids)} file IDs in the ledger.")
        return processed_ids
    except Exception as e:
        logging.error(f"ERROR: Could not retrieve processed file ledger: {e}")
        # In case of failure, return an empty set to be safe, but re-raise for retry
        raise

@retry(**RETRY_CONFIG)
def update_ledger(gsheets_client, file_id: str, status: str, error_message: str, config: Dict):
    """Adds or updates a record in the 'Processed Ledger' tab."""
    logging.info(f"Updating ledger for file {file_id} with status: {status}")
    try:
        sheet_id = config['google_sheets']['sheet_id']
        ledger_tab_name = config['google_sheets']['ledger_tab_name']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(ledger_tab_name)
        
        timestamp = logging.Formatter().formatTime(logging.makeLogRecord({}))
        
        # Simple append, as we are already de-duplicating at the start
        worksheet.append_row([file_id, status, timestamp, error_message])
        logging.info(f"SUCCESS: Ledger updated for file {file_id}.")
    except Exception as e:
        logging.error(f"ERROR: Could not update ledger for file {file_id}: {e}")
        raise

# =======================
# Results Writing
# =======================
def _coerce_int(val, default=0):
    """Safely converts a value to an integer."""
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return default

def _backfill_scores(d: Dict) -> Dict:
    """Calculates Total Score and % Score if they are missing."""
    score_keys = [
        "Opening Pitch Score", "Product Pitch Score", "Cross-Sell / Opportunity Handling",
        "Closing Effectiveness", "Negotiation Strength"
    ]
    
    # Check if scores are missing or are non-numeric strings
    total_score_present = 'Total Score' in d and isinstance(d['Total Score'], (int, float))
    percent_score_present = '% Score' in d and d['% Score']

    if not total_score_present or not percent_score_present:
        total = sum(_coerce_int(d.get(k)) for k in score_keys)
        d['Total Score'] = str(total)
        d['% Score'] = f"{round((total / 50) * 100)}%" if total > 0 else "0%"
    return d

@retry(**RETRY_CONFIG)
def write_results(gsheets_client, data: Dict[str, str], config: Dict):
    """Writes a row of analysis data to the 'Analysis Results' tab."""
    logging.info("Attempting to write data to Google Sheets...")
    try:
        sheet_id = config['google_sheets']['sheet_id']
        results_tab_name = config['google_sheets']['results_tab_name']
        default_headers = config['google_sheets']['default_headers']
        
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(results_tab_name)

        headers = worksheet.row_values(1)
        if not headers:
            logging.warning(f"No headers found in '{results_tab_name}'. Creating them.")
            worksheet.append_row(default_headers, value_input_option="USER_ENTERED")
            headers = default_headers

        # Backfill calculated fields like total score
        data = _backfill_scores(data)

        # Create the row of values in the correct order based on the sheet's headers
        # Use an empty string "" instead of "N/A" for a cleaner sheet
        row_to_insert = [data.get(header, "") for header in headers]
        
        worksheet.append_row(row_to_insert, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Data for '{data.get('Society Name', '')}' written to Google Sheets.")
    except Exception as e:
        logging.error(f"ERROR: Failed to write to Google Sheets: {e}")
        raise


