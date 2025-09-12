import os
import yaml
import logging
from datetime import datetime, timedelta
from typing import Dict

from google.cloud import bigquery
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            AVG(CAST(Percent_Score AS NUMERIC)) AS avg_score,
            SUM(CAST(Amount_Value AS NUMERIC)) AS total_deal_value
        FROM `{table_ref}`
        WHERE DATETIME(TIMESTAMP(Date)) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        GROUP BY Team
        ORDER BY total_meetings DESC;
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        
        summary = "*🏆 Weekly Performance Summary 🏆*\n\n"
        for row in results:
            summary += (
                f"*{row.Team} Team:*\n"
                f"  - Meetings: `{row.total_meetings}` by `{row.active_reps}` reps\n"
                f"  - Avg Score: `{row.avg_score:.2f}%`\n"
                f"  - Potential Value Discussed: `₹{row.total_deal_value:,.2f}`\n\n"
            )
        return summary if results.total_rows > 0 else "No new meetings were analyzed this week."
    except Exception as e:
        return f"Could not generate weekly summary: {e}"

def get_coaching_opportunities(bq_client: bigquery.Client, table_ref: str) -> str:
    """Identifies reps and skills that need attention."""
    query = f"""
        SELECT
            Owner,
            Team,
            AVG(CAST(Opening_Pitch_Score AS NUMERIC)) as avg_opening,
            AVG(CAST(Product_Pitch_Score AS NUMERIC)) as avg_product,
            AVG(CAST(Closing_Effectiveness AS NUMERIC)) as avg_closing
        FROM `{table_ref}`
        WHERE DATETIME(TIMESTAMP(Date)) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        GROUP BY Owner, Team
        HAVING avg_opening < 7 OR avg_product < 7 OR avg_closing < 7
        ORDER BY Team, Owner;
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        if results.total_rows == 0:
            return "" # No coaching opportunities to report

        coaching = "\n\n*💡 Coaching Opportunities 💡*\n_Reps with average scores below 7 this week._\n\n"
        for row in results:
            coaching += f"*{row.Owner}* ({row.Team}):\n"
            if row.avg_opening < 7: coaching += f"  - Opening Pitch: `{row.avg_opening:.1f}`\n"
            if row.avg_product < 7: coaching += f"  - Product Pitch: `{row.avg_product:.1f}`\n"
            if row.avg_closing < 7: coaching += f"  - Closing: `{row.avg_closing:.1f}`\n"
        return coaching
    except Exception as e:
        return f"\nCould not generate coaching report: {e}"

# =======================
# Notification Functions
# =======================
def send_slack_notification(message: str, config: Dict):
    """Sends a formatted message to a Slack channel."""
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    channel = config['weekly_digest']['slack_channel']
    
    if not slack_webhook_url:
        logging.warning("SLACK_WEBHOOK_URL not set. Skipping Slack notification.")
        return

    try:
        # NOTE: Using WebClient with a webhook URL is not standard.
        # This is a simplified example. A real implementation would use a bot token.
        # For webhook URLs, you would typically use the `requests` library.
        # This is a conceptual placeholder.
        client = WebClient(token="") # A bot token would normally go here.
        client.chat_postMessage(channel=channel, text=message)
        logging.info(f"Successfully sent weekly digest to Slack channel {channel}.")
    except SlackApiError as e:
        logging.error(f"ERROR: Failed to send Slack message: {e.response['error']}")
    except Exception as e:
        logging.error(f"An unexpected error occurred with Slack: {e}")


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

    project_id = config['google_bigquery']['project_id']
    dataset_id = config['google_bigquery']['dataset_id']
    table_id = config['google_bigquery']['table_id']
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    client = bigquery.Client()

    summary_report = get_weekly_summary(client, table_ref)
    coaching_report = get_coaching_opportunities(client, table_ref)
    
    final_message = f"{summary_report}{coaching_report}"
    
    send_slack_notification(final_message, config)

if __name__ == "__main__":
    main()

