import logging
import time
from typing import Dict, List, Any

import gspread
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tenacity import retry, stop_after_attempt, wait_exponential

# =======================
# Constants
# =======================
RETRY_CONFIG = {
    "wait": wait_exponential(multiplier=2, min=5, max=60),
    "stop": stop_after_attempt(5),
    "reraise": True,
}

# =======================
# Sheets Operations with Retry Logic
# =======================
@retry(**RETRY_CONFIG)
def get_processed_file_ids(gsheets_client: gspread.Client, config: Dict) -> Dict[str, Dict[str, str]]:
    """
    Retrieves processed file IDs from the ledger tab.
    Returns a dict: {file_id: {"status": str, "timestamp": str, "error": str, "original_folder": str}}
    """
    try:
        logging.info("Retrieving processed file ledger from Google Sheets...")
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab_name = config["google_sheets"]["ledger_tab_name"]

        spreadsheet = gsheets_client.open_by_key(sheet_id)

        try:
            worksheet = spreadsheet.worksheet(ledger_tab_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"Ledger tab '{ledger_tab_name}' not found. Creating it.")
            worksheet = spreadsheet.add_worksheet(title=ledger_tab_name, rows="1000", cols="5")
            worksheet.append_row(["File ID", "Status", "Timestamp", "Error Message", "Original Folder"])
            return {}

        records = worksheet.get_all_records()
        ledger = {
            str(row["File ID"]): {
                "status": row.get("Status", ""),
                "timestamp": row.get("Timestamp", ""),
                "error": row.get("Error Message", ""),
                "original_folder": row.get("Original Folder", ""),
            }
            for row in records if row.get("File ID")
        }

        logging.info(f"Found {len(ledger)} file IDs in the ledger.")
        return ledger

    except Exception as e:
        logging.error(f"ERROR: Could not retrieve processed file ledger: {e}")
        raise


@retry(**RETRY_CONFIG)
def write_results(gsheets_client: gspread.Client, data: Dict[str, Any], config: Dict):
    """Writes the analysis results to the main results tab."""
    try:
        logging.info("Attempting to write data to Google Sheets...")
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab_name = config["google_sheets"]["results_tab_name"]

        spreadsheet = gsheets_client.open_by_key(sheet_id)

        try:
            worksheet = spreadsheet.worksheet(results_tab_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"Results tab '{results_tab_name}' not found. Creating it.")
            worksheet = spreadsheet.add_worksheet(title=results_tab_name, rows="1000", cols="50")

        headers = worksheet.row_values(1)
        if not headers:
            logging.info("No headers found in results sheet. Writing default headers.")
            headers = config.get("sheets_headers", [])
            if headers:
                worksheet.append_row(headers, value_input_option="USER_ENTERED")

        # Only write values matching the expected headers
        flat_data = {
            k: v for k, v in data.items() if isinstance(v, (str, int, float, bool)) or v is None
        }
        row_to_insert = [str(flat_data.get(header, "")) for header in headers]

        worksheet.append_row(row_to_insert, value_input_option="USER_ENTERED")
        logging.info(f"SUCCESS: Data for '{data.get('Society Name', '')}' written to Google Sheets.")

    except Exception as e:
        logging.error(f"ERROR: Failed to write to Google Sheets: {e}")
        raise


@retry(**RETRY_CONFIG)
def update_ledger(
    gsheets_client: gspread.Client,
    file_id: str,
    status: str,
    error_message: str,
    config: Dict,
    original_folder: str = "",
):
    """Adds or updates a file's status in the processed ledger."""
    try:
        logging.info(f"Updating ledger for file {file_id} with status: {status}")
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab_name = config["google_sheets"]["ledger_tab_name"]

        spreadsheet = gsheets_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(ledger_tab_name)

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        # Try to find existing row
        cell = worksheet.find(file_id)
        if cell:
            row_num = cell.row
            worksheet.update(
                f"B{row_num}:E{row_num}",
                [[status, timestamp, error_message, original_folder]],
                value_input_option="USER_ENTERED",
            )
            logging.info(f"SUCCESS: Ledger updated in-place for file {file_id}.")
        else:
            worksheet.append_row(
                [file_id, status, timestamp, error_message, original_folder],
                value_input_option="USER_ENTERED",
            )
            logging.info(f"SUCCESS: Ledger appended new row for file {file_id}.")

    except Exception as e:
        logging.error(f"ERROR: Failed to update ledger: {e}")
        raise

# =======================
# BigQuery Operations
# =======================
@retry(**RETRY_CONFIG)
def stream_to_bigquery(bq_client: bigquery.Client, data: Dict[str, Any], config: Dict):
    """Streams a single record to the BigQuery table."""
    try:
        project_id = config["google_bigquery"]["project_id"]
        dataset_id = config["google_bigquery"]["dataset_id"]
        table_id = config["google_bigquery"]["table_id"]
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        headers = config.get("sheets_headers", [])
        bq_record = {
            header.replace(" ", "_").replace("/", "_").replace("%", "Percent"): str(val)
            for header, val in data.items()
            if header in headers
        }

        errors = bq_client.insert_rows_json(table_ref, [bq_record])
        if not errors:
            logging.info(f"Successfully streamed record for '{data.get('Society Name', '')}' to BigQuery.")
        else:
            logging.error(f"Encountered errors while streaming to BigQuery: {errors}")

    except Exception as e:
        logging.error(f"ERROR: Failed to stream data to BigQuery: {e}")


def get_all_results(gsheets_client: gspread.Client, config: Dict) -> List[Dict]:
    """Fetches all records from the results tab for the dashboard export."""
    logging.info("Fetching all records from Google Sheet for dashboard export...")
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab_name = config["google_sheets"]["results_tab_name"]
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(results_tab_name)
        return worksheet.get_all_records()
    except Exception as e:
        logging.error(f"Could not fetch all results from sheet: {e}")
        return []
