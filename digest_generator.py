import os
import yaml
import logging
import json
import time  # Import the time library
from typing import Dict, List

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import requests

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =======================
# BigQuery Setup Function (Upgraded)
# =======================
def ensure_bigquery_schema(client: bigquery.Client, project_id: str, dataset_id: str, table_id: str, schema_fields: List[str]):
    """Checks if the dataset and table exist and have a schema, creating or updating them if necessary."""
    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref_str = f"{dataset_ref}.{table_id}"
    table_ref = bigquery.TableReference.from_string(table_ref_str)

    try:
        client.get_dataset(dataset_ref)
        logging.info(f"BigQuery dataset '{dataset_ref}' already exists.")
    except NotFound:
        logging.warning(f"BigQuery dataset '{dataset_ref}' not found. Creating it...")
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, timeout=30)
        logging.info(f"Successfully created dataset '{dataset_ref}'.")

    try:
        table = client.get_table(table_ref)
        logging.info(f"BigQuery table '{table_ref_str}' already exists.")
        if not table.schema:
            logging.warning(f"Table '{table_ref_str}' exists but has no schema. Updating it.")
            table.schema = [bigquery.SchemaField(name.replace(" ", "_"), "STRING") for name in schema_fields]
            client.update_table(table, ["schema"])
            logging.info(f"Successfully added schema to table '{table_ref_str}'.")
            # --- THIS IS THE FIX ---
            logging.info("Pausing for 5 seconds to allow schema to propagate...")
            time.sleep(5) # Add a 5-second pause

    except NotFound:
        logging.warning(f"BigQuery table '{table_ref_str}' not found. Creating it with schema...")
        schema = [bigquery.SchemaField(name.replace(" ", "_"), "STRING") for name in schema_fields]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, timeout=30)
        logging.info(f"Successfully created table '{table_ref_str}'.")
        # --- THIS IS THE FIX ---
        logging.info("Pausing for 5 seconds to allow new table to be ready for queries...")
        time.sleep(5) # Add a 5-second pause


# =======================
# Query Functions
# =======================
def get_weekly_summary(bq_client: bigquery.Client, table_ref: str) -> str:
    """Generates a high-level summary of the week's meetings."""
    query = f"""
        SELECT
            Team,
            COUNT(DISTINCT Owner) AS active_reps,
            COUNT(*) AS total_meetings,
            AVG(CAST(NULLIF(TRIM(Percent_Score), '') AS NUMERIC)) AS avg_score,
            SUM(CAST(NULLIF(TRIM(Amount_Value), '') AS NUMERIC)) AS total_deal_value
        FROM `{table_ref}`
        WHERE SAFE.PARSE_DATETIME('%Y/%m/%d', Date) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        GROUP BY Team
        ORDER BY total_meetings DESC;
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        
        if results.total_rows == 0:
            return "No new meetings were analyzed this week."

        summary = "*🏆 Weekly Performance Summary 🏆*\n\n"
        for row in results:
            summary += (
                f"*{row.Team or 'Uncategorized'} Team:*\n"
                f"  - Meetings: `{row.total_meetings}` by `{row.active_reps}` reps\n"
                f"  - Avg Score: `{row.avg_score or 0:.1f}%`\n"
                f"  - Potential Value Discussed: `₹{row.total_deal_value or 0:,.0f}`\n\n"
            )
        return summary
    except Exception as e:
        logging.error(f"Could not generate weekly summary: {e}")
        return "Could not generate weekly summary due to an error."

def get_coaching_opportunities(bq_client: bigquery.Client, table_ref: str) -> str:
    """Identifies reps and skills that need attention."""
    query = f"""
        SELECT
            Owner,
            Team,
            AVG(CAST(NULLIF(TRIM(Opening_Pitch_Score), '') AS NUMERIC)) as avg_opening,
            AVG(CAST(NULLIF(TRIM(Product_Pitch_Score), '') AS NUMERIC)) as avg_product,
            AVG(CAST(NULLIF(TRIM(Closing_Effectiveness), '') AS NUMERIC)) as avg_closing
        FROM `{table_ref}`
        WHERE SAFE.PARSE_DATETIME('%Y/%m/%d', Date) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        GROUP BY Owner, Team
        HAVING avg_opening < 7 OR avg_product < 7 OR avg_closing < 7
        ORDER BY Team, Owner;
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        if results.total_rows == 0:
            return "" 

        coaching = "\n\n*💡 Coaching Opportunities 💡*\n_Reps with average scores below 7 this week._\n\n"
        for row in results:
            coaching += f"*{row.Owner}* ({row.Team or 'N/A'}):\n"
            if row.avg_opening and row.avg_opening < 7: coaching += f"  - Opening Pitch: `{row.avg_opening:.1f}`\n"
            if row.avg_product and row.avg_product < 7: coaching += f"  - Product Pitch: `{row.avg_product:.1f}`\n"
            if row.avg_closing and row.avg_closing < 7: coaching += f"  - Closing: `{row.avg_closing:.1f}`\n"
        return coaching
    except Exception as e:
        logging.error(f"Could not generate coaching report: {e}")
        return "\nCould not generate coaching report due to an error."

# =======================
# Notification Functions
# =======================
def send_slack_notification(message: str, config: Dict):
    """Sends a formatted message to a Slack channel using a webhook."""
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not slack_webhook_url:
        logging.warning("SLACK_WEBHOOK_URL not set. Skipping Slack notification.")
        print("Final Message (would be sent to Slack):\n", message)
        return

    try:
        response = requests.post(
            slack_webhook_url,
            json={"text": message},
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        logging.info("Successfully sent weekly digest to Slack.")
    except requests.exceptions.RequestException as e:
        logging.error(f"ERROR: Failed to send Slack message: {e}")

# =======================
# Main
# =======================
def main():
    """Generates and sends the weekly digest."""
    logging.info("--- Starting Weekly Digest Generator ---")
    
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if not config.get('weekly_digest', {}).get('enabled'):
        logging.info("Weekly digest is disabled in config.yaml. Exiting.")
        return

    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        logging.error("CRITICAL: GCP_SA_KEY environment variable not found. Cannot authenticate.")
        return
        
    try:
        creds_info = json.loads(gcp_key_str)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        project_id = config['google_bigquery']['project_id']
        client = bigquery.Client(credentials=creds, project=project_id)
        logging.info(f"SUCCESS: Authenticated with Google BigQuery for project '{project_id}'.")
    except Exception as e:
        logging.error(f"CRITICAL: BigQuery client authentication failed: {e}")
        return

    dataset_id = config['google_bigquery']['dataset_id']
    table_id = config['google_bigquery']['table_id']
    
    # Ensure schema exists before querying
    schema_fields = config.get('sheets_headers', [])
    ensure_bigquery_schema(client, project_id, dataset_id, table_id, schema_fields)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    summary_report = get_weekly_summary(client, table_ref)
    coaching_report = get_coaching_opportunities(client, table_ref)
    
    final_message = f"{summary_report}{coaching_report}"
    
    send_slack_notification(final_message, config)
    
    logging.info("--- Weekly Digest Generator Finished ---")

if __name__ == "__main__":
    main()

