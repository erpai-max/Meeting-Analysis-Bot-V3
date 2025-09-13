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

# Import the new email formatter
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
# Data Fetching from BigQuery
# =======================
def fetch_manager_data(bq_client: bigquery.Client, table_ref: str, manager_name: str) -> List[Dict]:
    """Fetches all performance data for a specific manager's team from the last 7 days."""
    query = f"""
        WITH weekly_data AS (
            SELECT
                Owner, Team,
                SAFE_CAST(NULLIF(TRIM(Percent_Score), '') AS NUMERIC) AS Percent_Score,
                SAFE_CAST(NULLIF(TRIM(Amount_Value), '') AS NUMERIC) AS Amount_Value,
                -- Adjust the parsing format if your Date column isn't '%Y/%m/%d'
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
        logging.info(f"Fetching data for manager: {manager_name}")
        query_job = bq_client.query(query)
        results = [dict(row) for row in query_job.result()]
        logging.info(f"Found {len(results)} records for {manager_name}.")
        return results
    except Exception as e:
        logging.error(f"Could not fetch data for manager {manager_name}: {e}")
        return []

# =======================
# Data Processing & Aggregation
# =======================
def process_team_data(team_records: List[Dict]) -> Tuple[Dict, List, List]:
    """Aggregates raw data into KPIs, a team performance list, and coaching notes."""
    total_meetings = len(team_records)
    total_pipeline = sum(r.get("Amount_Value") or 0 for r in team_records)
    avg_score = (
        sum(r.get("Percent_Score") or 0 for r in team_records) / total_meetings
        if total_meetings > 0
        else 0
    )

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    team_performance = []
    reps = sorted({r["Owner"] for r in team_records if r.get("Owner")})
    for rep in reps:
        rep_meetings = [r for r in team_records if r["Owner"] == rep]
        rep_avg_score = (
            sum(r.get("Percent_Score") or 0 for r in rep_meetings) / len(rep_meetings)
            if rep_meetings
            else 0
        )
        rep_pipeline = sum(r.get("Amount_Value") or 0 for r in rep_meetings)

        prev_scores = [r.get("prev_week_avg_score") for r in rep_meetings if r.get("prev_week_avg_score") is not None]
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

    # Placeholder for more advanced coaching note generation
    coaching_notes = []

    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), coaching_notes

# =======================
# AI Summary Generation
# =======================
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Uses Gemini to generate a high-level executive summary."""
    logging.info(f"Generating AI summary for {manager_name}...")
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logging.error("GEMINI_API_KEY not set. Cannot generate AI summary.")
        return "AI summary could not be generated due to a configuration error."

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel(config["analysis"]["gemini_model"])

    prompt = f"""
    You are a senior sales analyst providing a weekly summary to a manager named {manager_name}.
    Based on the following data for their team's performance over the last 7 days, write a concise, 2-3 sentence executive summary.
    Focus on the most important trend, biggest success, or most critical area for improvement. Be direct and data-driven.

    **Key KPIs:**
    - Total Meetings: {kpis['total_meetings']}
    - Team Average Score: {kpis['avg_score']:.1f}%
    - Total Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

    **Individual Performance:**
    {json.dumps(team_data, indent=2)}

    **Your Summary:**
    """
    try:
        response = model.generate_content(prompt)
        logging.info("Successfully generated AI summary.")

        # Safer response parsing
        if hasattr(response, "text") and response.text:
            return response.text.strip()
        elif hasattr(response, "candidates") and response.candidates:
            parts = response.candidates[0].content.parts
            return parts[0].text.strip() if parts else "Summary unavailable."
        else:
            return "Summary unavailable."
    except Exception as e:
        logging.error(f"Failed to generate AI summary: {e}")
        return "An AI-powered summary could not be generated for this week's data."

# =======================
# Email Sending
# =======================
def send_email(subject: str, html_content: str, recipient: str):
    """Sends the HTML report via email using secrets."""
    sender_email = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")

    if not sender_email or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Skipping actual email sending.")
        print("\n--- EMAIL PREVIEW ---")
        print(f"TO: {recipient}")
        print(f"SUBJECT: {subject}")
        print(f"BODY (truncated): {html_content[:500]}...\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    try:
        logging.info(f"Connecting to SMTP server to send email to {recipient}...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient, msg.as_string())
        logging.info("SUCCESS: Email sent.")
    except Exception as e:
        logging.error(f"ERROR: Failed to send email: {e}")

# =======================
# Main
# =======================
def main():
    """Generates and sends the weekly digest reports to each manager."""
    logging.info("--- Starting Weekly Digest Generator ---")

    config_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled"):
        logging.info("Weekly digest is disabled in config. Exiting.")
        return

    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        logging.error("GCP_SA_KEY not set. Exiting.")
        return

    bq_client = get_bigquery_client(config, gcp_key_str)

    table_ref = f"{bq_client.project}.{config['google_bigquery']['dataset_id']}.{config['google_bigquery']['table_id']}"

    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined in config. Nothing to do.")
        return

    for manager_name, manager_email in manager_emails.items():
        logging.info(f"--- Generating report for {manager_name} ---")

        team_records = fetch_manager_data(bq_client, table_ref, manager_name)

        if not team_records:
            logging.info(f"No new meeting data for {manager_name}'s team this week. Skipping report.")
            continue

        kpis, team_data, coaching_notes = process_team_data(team_records)
        ai_summary = generate_ai_summary(manager_name, kpis, team_data, config)

        email_html = email_formatter.create_manager_digest_email(
            manager_name, kpis, team_data, coaching_notes, ai_summary
        )

        subject = f"Your Team's Weekly Meeting Analysis Summary ({time.strftime('%b %d')})"
        send_email(subject, email_html, manager_email)

    logging.info("--- Weekly Digest Generator Finished ---")


if __name__ == "__main__":
    main()
