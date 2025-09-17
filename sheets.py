# sheets.py
import logging
import datetime
from typing import Dict, List, Any

# -----------------------
# Default Headers (47 cols)
# -----------------------
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
    "Manager Email",
]

LEDGER_HEADERS = ["File ID", "File Name", "Status", "Error", "Timestamp"]

# Optional header -> bigquery-safe name mapping
HEADER_MAP = {
    "Date": "date",
    "POC Name": "poc_name",
    "Society Name": "society_name",
    "Visit Type": "visit_type",
    "Meeting Type": "meeting_type",
    "Amount Value": "amount_value",
    "Months": "months",
    "Deal Status": "deal_status",
    "Vendor Leads": "vendor_leads",
    "Society Leads": "society_leads",
    "Opening Pitch Score": "opening_pitch_score",
    "Product Pitch Score": "product_pitch_score",
    "Cross-Sell / Opportunity Handling": "cross_sell_opportunity_handling",
    "Closing Effectiveness": "closing_effectiveness",
    "Negotiation Strength": "negotiation_strength",
    "Rebuttal Handling": "rebuttal_handling",
    "Overall Sentiment": "overall_sentiment",
    "Total Score": "total_score",
    "% Score": "percent_score",
    "Risks / Unresolved Issues": "risks_unresolved_issues",
    "Improvements Needed": "improvements_needed",
    "Owner (Who handled the meeting)": "owner",
    "Email Id": "email_id",
    "Kibana ID": "kibana_id",
    "Manager": "manager",
    "Product Pitch": "product_pitch",
    "Team": "team",
    "Media Link": "media_link",
    "Doc Link": "doc_link",
    "Suggestions & Missed Topics": "suggestions_missed_topics",
    "Pre-meeting brief": "pre_meeting_brief",
    "Meeting duration (min)": "meeting_duration_min",
    "Rapport Building": "rapport_building",
    "Improvement Areas": "improvement_areas",
    "Product Knowledge Displayed": "product_knowledge_displayed",
    "Call Effectiveness and Control": "call_effectiveness_control",
    "Next Step Clarity and Commitment": "next_step_clarity_commitment",
    "Missed Opportunities": "missed_opportunities",
    "Key Discussion Points": "key_discussion_points",
    "Key Questions": "key_questions",
    "Competition Discussion": "competition_discussion",
    "Action items": "action_items",
    "Positive Factors": "positive_factors",
    "Negative Factors": "negative_factors",
    "Customer Needs": "customer_needs",
    "Overall Client Sentiment": "overall_client_sentiment",
    "Feature Checklist Coverage": "feature_checklist_coverage",
    "Manager Email": "manager_email",
}


# -----------------------
# Helpers to handle either gspread.Client or Spreadsheet object
# -----------------------
def _open_spreadsheet(gsheets_client, sheet_id: str):
    """
    Given a gspread client or a Spreadsheet object, return a Spreadsheet object.
    This handles both:
      - gsheets_client.open_by_key(sheet_id) (when gsheets_client is a Client)
      - gsheets_client (when it's already a Spreadsheet)
    """
    try:
        # gspread Client has open_by_key
        if hasattr(gsheets_client, "open_by_key"):
            return gsheets_client.open_by_key(sheet_id)
        # maybe already a Spreadsheet object
        if hasattr(gsheets_client, "worksheet"):
            return gsheets_client
        raise ValueError("Invalid gsheets_client provided (neither Client nor Spreadsheet).")
    except Exception as e:
        logging.error(f"CRITICAL: Could not open spreadsheet {sheet_id}: {e}")
        raise


def _get_or_create_worksheet(spreadsheet, title: str, headers: List[str] = None):
    """
    Get worksheet by title. If not present, create it and write headers (if provided).
    Returns a gspread Worksheet object.
    """
    try:
        ws = spreadsheet.worksheet(title)
        return ws
    except Exception:
        # create if not exists
        rows = max(100, len(headers or []) + 5)
        cols = max(10, len(headers or DEFAULT_HEADERS))
        try:
            ws = spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
            # write headers if provided, else DEFAULT_HEADERS
            header_row = headers if headers else DEFAULT_HEADERS
            ws.append_row(header_row, value_input_option="RAW")
            logging.info(f"Created worksheet '{title}' and wrote headers.")
            return ws
        except Exception as e:
            logging.error(f"Failed to create worksheet '{title}': {e}")
            raise


# -----------------------
# Public API
# -----------------------
def get_processed_file_ids(gsheets_client, config) -> List[str]:
    """
    Reads the ledger sheet and returns all previously processed file IDs.
    Returns empty list if ledger missing or unreadable.
    """
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Ledger")
        spreadsheet = _open_spreadsheet(gsheets_client, sheet_id)
        ledger_ws = _get_or_create_worksheet(spreadsheet, ledger_tab, headers=LEDGER_HEADERS)

        records = ledger_ws.get_all_records()
        file_ids = [str(r.get("File ID")).strip() for r in records if r.get("File ID")]
        logging.info(f"Found {len(file_ids)} file IDs in the ledger.")
        return file_ids
    except Exception as e:
        logging.warning(f"Ledger not found or unreadable, returning empty list: {e}")
        return []


def update_ledger(gsheets_client, file_id: str, status: str, error: str, config: Dict, file_name: str = "Unknown"):
    """
    Appends or updates a row in the ledger for tracking.
    - If an entry with the same File ID exists, updates status & error & timestamp.
    - Otherwise appends a new row: [File ID, File Name, Status, Error, Timestamp]
    """
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        ledger_tab = config["google_sheets"].get("ledger_tab_name", "Ledger")
        spreadsheet = _open_spreadsheet(gsheets_client, sheet_id)
        ledger_ws = _get_or_create_worksheet(spreadsheet, ledger_tab, headers=LEDGER_HEADERS)

        records = ledger_ws.get_all_records()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # find existing row index
        row_index = None
        for idx, rec in enumerate(records, start=2):
            if str(rec.get("File ID")) == str(file_id):
                row_index = idx
                break

        if row_index:
            # Status is column 3, Error is column 4, Timestamp column 5
            try:
                ledger_ws.update_cell(row_index, 3, status)
                ledger_ws.update_cell(row_index, 4, str(error)[:500])
                ledger_ws.update_cell(row_index, 5, timestamp)
            except Exception:
                # fallback: overwrite the whole row
                updated_row = [file_id, file_name, status, str(error)[:500], timestamp]
                ledger_ws.delete_rows(row_index)
                ledger_ws.append_row(updated_row, value_input_option="RAW")
        else:
            ledger_ws.append_row([file_id, file_name, status, str(error)[:500], timestamp], value_input_option="RAW")

        logging.info(f"SUCCESS: Ledger updated → {file_name} ({status})")
    except Exception as e:
        logging.error(f"ERROR updating ledger for file {file_name}: {e}")


def write_analysis_result(gsheets_client, analysis_data: Dict[str, Any], config: Dict):
    """
    Appends structured analysis results to the main Google Sheet (Results tab).
    Guarantees header row exists and writes values in the same order.
    Missing values are written as "N/A".
    """
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Results")
        # headers preference: config["sheets_headers"] -> module DEFAULT_HEADERS
        headers = config.get("sheets_headers") or DEFAULT_HEADERS

        spreadsheet = _open_spreadsheet(gsheets_client, sheet_id)
        ws = None
        try:
            ws = spreadsheet.worksheet(results_tab)
        except Exception:
            # create and write headers
            ws = spreadsheet.add_worksheet(title=results_tab, rows="1000", cols=str(max(10, len(headers))))
            ws.append_row(headers, value_input_option="RAW")
            logging.info(f"Created '{results_tab}' sheet and wrote headers.")

        # Ensure first row has headers; if missing, write them
        existing_headers = ws.row_values(1)
        if not existing_headers or len(existing_headers) < len(headers):
            # replace header row
            # delete row 1 then insert headers (some environments may not allow delete; fallback to update)
            try:
                ws.delete_rows(1)
            except Exception:
                pass
            ws.insert_row(headers, index=1, value_input_option="RAW")

        # Build row according to header order
        row = []
        for h in headers:
            # Accept multiple key forms: exact header, short form, lower-case
            val = None
            # direct
            if h in analysis_data:
                val = analysis_data.get(h)
            else:
                # try lower-cased keys
                for k in analysis_data.keys():
                    if str(k).strip().lower() == h.strip().lower():
                        val = analysis_data.get(k)
                        break

            if val is None or (isinstance(val, str) and val.strip() == ""):
                row.append("N/A")
            else:
                row.append(str(val))

        ws.append_row(row, value_input_option="RAW")
        logging.info(f"SUCCESS: Wrote analysis result for '{analysis_data.get('Society Name', 'Unknown')}'")
    except Exception as e:
        logging.error(f"ERROR: Failed to write analysis result: {e}")
        raise


def get_all_results(gsheets_client, config: Dict) -> List[Dict]:
    """
    Fetches all rows (as list of dicts) from the Results sheet for dashboard export.
    Returns empty list on failure.
    """
    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Results")
        spreadsheet = _open_spreadsheet(gsheets_client, sheet_id)
        ws = _get_or_create_worksheet(spreadsheet, results_tab, headers=config.get("sheets_headers", DEFAULT_HEADERS))
        records = ws.get_all_records()
        logging.info(f"Exported {len(records)} rows from {results_tab} sheet.")
        return records
    except Exception as e:
        logging.error(f"ERROR: Could not fetch results for dashboard export: {e}")
        return []


def normalize_for_bigquery(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert sheet-style headers into BigQuery-safe snake_case names.
    This is a helper used if you integrate with BigQuery streaming inserts.
    """
    normalized = {}
    for old_key, value in record.items():
        new_key = HEADER_MAP.get(old_key, None)
        if not new_key:
            # fallback: lower + replace non-alnum with underscore
            new_key = "".join(ch if ch.isalnum() else "_" for ch in old_key.strip().lower())
            # collapse multiple underscores
            while "__" in new_key:
                new_key = new_key.replace("__", "_")
            new_key = new_key.strip("_")
            if new_key == "":
                new_key = old_key.strip().replace(" ", "_").lower()
        normalized[new_key] = value
    return normalized
