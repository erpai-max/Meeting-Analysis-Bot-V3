import os
import yaml
import logging
import json
import time
from typing import Dict, List, Any, Tuple
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.oauth2 import service_account
from google.cloud import bigquery
import google.generativeai as genai

# Import email formatter (with charts)
import email_formatter

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =======================
# Authentication
# =======================
def get_bigquery_client(config: Dict, gcp_key_str: str) -> bigquery.Client:
    """Authenticates and returns a BigQuery client."""
    try:
        creds_info = json.loads(gcp_key_str)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        project_id = config["google_bigquery"]["project_id"]
        client = bigquery.Client(credentials=creds, project=project_id)
        logging.info(f"SUCCESS: Authenticated with Google BigQuery for project '{project_id}'.")
        return client
    except Exception as e:
        logging.error(f"CRITICAL: BigQuery client authentication failed: {e}")
        raise

# =======================
# Data Fetching
# =======================
def fetch_manager_data(bq_client: bigquery.Client, table_ref: str, manager_name: str) -> List[Dict]:
    """Fetches performance data for a manager's team (last 7 days + previous week)."""
    query = f"""
        WITH weekly_data AS (
            SELECT
                Owner, Team,
                SAFE_CAST(NULLIF(TRIM(Percent_Score), '') AS NUMERIC) AS Percent_Score,
                SAFE_CAST(NULLIF(TRIM(Amount_Value), '') AS NUMERIC) AS Amount_Value,
                SAFE.PARSE_DATETIME('%Y/%m/%d', Date) AS meeting_date
            FROM `{table_ref}`
            WHERE Manager = '{manager_name}'
        ),
        current_week AS (
            SELECT * FROM weekly_data
            WHERE meeting_date >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
        ),
        previous_week AS (
            SELECT Owner, AVG(Percent_Score) AS prev_week_avg_score
            FROM weekly_data
            WHERE meeting_date >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 14 DAY)
              AND meeting_date < DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
            GROUP BY Owner
        )
        SELECT
            cw.Owner,
            cw.Team,
            cw.Percent_Score,
            cw.Amount_Value,
            pw.prev_week_avg_score
        FROM current_week cw
        LEFT JOIN previous_week pw ON cw.Owner = pw.Owner
    """
    try:
        query_job = bq_client.query(query)
        results = [dict(row) for row in query_job.result()]
        logging.info(f"Found {len(results)} records for {manager_name}.")
        return results
    except Exception as e:
        logging.error(f"Could not fetch data for manager {manager_name}: {e}")
        return []

# =======================
# Data Processing
# =======================
def process_team_data(team_records: List[Dict]) -> Tuple[Dict, List[Dict], List[Dict]]:
    """Aggregates raw data into KPIs, team performance list, and coaching notes."""
    total_meetings = len(team_records)
    total_pipeline = sum(r["Amount_Value"] for r in team_records if r.get("Amount_Value"))
    avg_score = (
        sum(r["Percent_Score"] for r in team_records if r.get("Percent_Score")) / total_meetings
        if total_meetings > 0
        else 0
    )

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    team_performance = []
    reps = sorted(set(r["Owner"] for r in team_records))
    for rep in reps:
        rep_meetings = [r for r in team_records if r["Owner"] == rep]
        rep_avg_score = (
            sum(r["Percent_Score"] for r in rep_meetings if r.get("Percent_Score")) / len(rep_meetings)
            if rep_meetings
            else 0
        )
        rep_pipeline = sum(r["Amount_Value"] for r in rep_meetings if r.get("Amount_Value"))
        prev_scores = [r["prev_week_avg_score"] for r in rep_meetings if r.get("prev_week_avg_score")]
        avg_prev_score = sum(prev_scores) / len(prev_scores) if prev_scores else rep_avg_score
        score_change = rep_avg_score - avg_prev_score

        team_performance.append(
            {
                "owner": rep,
                "meetings": len(rep_meetings),
                "avg_score": rep_avg_score,
                "pipeline": rep_pipeline,
                "score_change": score_change,
            }
        )

    coaching_notes = []  # Can add advanced logic later
    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), coaching_notes

# =======================
# AI Summary
# =======================
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Uses Gemini to generate a 2–3 sentence executive summary."""
    logging.info(f"Generating AI summary for {manager_name}...")
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logging.error("GEMINI_API_KEY not set. Cannot generate AI summary.")
        return "AI summary could not be generated."

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel(config["analysis"]["gemini_model"])

    prompt = f"""
    You are a senior sales analyst providing a weekly summary to {manager_name}.
    Based on the following data for their team's performance over the last 7 days, write a concise, 2–3 sentence executive summary.
    Focus on the most important trend, biggest success, or critical area for improvement.

    KPIs:
    - Total Meetings: {kpis['total_meetings']}
    - Team Avg Score: {kpis['avg_score']:.1f}%
    - Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

    Team Performance:
    {json.dumps(team_data, indent=2)}

    Summary:
    """
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logging.error(f"AI summary failed: {e}")
        return "AI summary not available."

# =======================
# Email Sending
# =======================
def send_email(subject: str, html_content: str, recipient: str):
    """Sends the digest email."""
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Skipping email send.")
        print("\n--- EMAIL CONTENT PREVIEW ---\n")
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:500]}...\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        logging.info(f"SUCCESS: Email sent to {recipient}")
    except Exception as e:
        logging.error(f"ERROR: Failed to send email: {e}")

# =======================
# Main
# =======================
def main():
    logging.info("--- Starting Weekly Digest Generator ---")

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled in config.yaml. Exiting.")
        return

    gcp_key_str = os.environ.get("GCP_SA_KEY")
    bq_client = get_bigquery_client(config, gcp_key_str)

    table_ref = f"{bq_client.project}.{config['google_bigquery']['dataset_id']}.{config['google_bigquery']['table_id']}"

    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined. Exiting.")
        return

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        team_records = fetch_manager_data(bq_client, table_ref, manager)

        if not team_records:
            logging.info(f"No data for {manager} this week.")
            continue

        kpis, team_data, coaching_notes = process_team_data(team_records)
        ai_summary = generate_ai_summary(manager, kpis, team_data, config)

        html_email = email_formatter.create_manager_digest_email(
            manager, kpis, team_data, coaching_notes, ai_summary
        )

        subject = f"Weekly Meeting Digest | {manager} | {time.strftime('%b %d, %Y')}"
        send_email(subject, html_email, email)

    logging.info("--- Weekly Digest Generator Finished ---")


if __name__ == "__main__":
    main()
