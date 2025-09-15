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

import sheets
import email_formatter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -----------------------
# BigQuery Authentication
# -----------------------
def get_bigquery_client(config: Dict, gcp_key_str: str):
    """Authenticates BigQuery client. Returns None if not usable (free tier)."""
    try:
        creds_info = json.loads(gcp_key_str)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        project_id = config["google_bigquery"]["project_id"]
        client = bigquery.Client(credentials=creds, project=project_id)
        logging.info(f"SUCCESS: Authenticated with Google BigQuery for project '{project_id}'.")
        return client
    except Exception as e:
        logging.warning(f"⚠️ BigQuery not available: {e}")
        return None

# -----------------------
# BigQuery Fetch
# -----------------------
def fetch_manager_data_bigquery(bq_client, table_ref: str, manager_name: str) -> List[Dict]:
    query = f"""
        WITH weekly_data AS (
            SELECT
                Owner, Team,
                SAFE_CAST(NULLIF(TRIM(percent_score), '') AS NUMERIC) AS Percent_Score,
                SAFE_CAST(NULLIF(TRIM(amount_value), '') AS NUMERIC) AS Amount_Value,
                SAFE.PARSE_DATETIME('%Y/%m/%d', date) AS meeting_date
            FROM `{table_ref}`
            WHERE manager = '{manager_name}'
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
        return [dict(row) for row in query_job.result()]
    except Exception as e:
        logging.warning(f"⚠️ BigQuery query failed for {manager_name}: {e}")
        return []

# -----------------------
# Sheets Fallback
# -----------------------
def fetch_manager_data_sheets(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    """Fallback: Read data from Google Sheets instead of BigQuery."""
    try:
        records = sheets.get_all_results(gsheets_client, config)
        filtered = [r for r in records if str(r.get("Manager", "")).lower() == manager_name.lower()]
        logging.info(f"Fetched {len(filtered)} records for {manager_name} from Google Sheets.")
        return filtered
    except Exception as e:
        logging.error(f"Sheets fallback failed for {manager_name}: {e}")
        return []

# -----------------------
# Data Processing
# -----------------------
def process_team_data(team_records: List[Dict]) -> Tuple[Dict, List[Dict], List[Dict]]:
    total_meetings = len(team_records)
    total_pipeline = sum(float(r.get("Amount Value") or r.get("Amount_Value") or 0) for r in team_records)
    avg_score = (
        sum(float(r.get("Percent_Score") or r.get("% Score") or 0) for r in team_records) / total_meetings
        if total_meetings > 0 else 0
    )

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    team_performance = []
    reps = sorted(set(r.get("Owner") or r.get("Owner (Who handled the meeting)") for r in team_records))
    for rep in reps:
        rep_meetings = [r for r in team_records if (r.get("Owner") or r.get("Owner (Who handled the meeting)")) == rep]
        rep_avg_score = (
            sum(float(r.get("Percent_Score") or r.get("% Score") or 0) for r in rep_meetings) / len(rep_meetings)
            if rep_meetings else 0
        )
        rep_pipeline = sum(float(r.get("Amount Value") or r.get("Amount_Value") or 0) for r in rep_meetings)
        score_change = 0  # Simplified for Sheets fallback

        team_performance.append({
            "owner": rep,
            "meetings": len(rep_meetings),
            "avg_score": rep_avg_score,
            "pipeline": rep_pipeline,
            "score_change": score_change,
        })

    coaching_notes = []  # optional
    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), coaching_notes

# -----------------------
# AI Summary
# -----------------------
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    logging.info(f"Generating AI summary for {manager_name}...")
    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"]["gemini_model"])
        prompt = f"""
        Provide a 2–3 sentence executive summary for {manager_name}'s team:
        - Total Meetings: {kpis['total_meetings']}
        - Team Avg Score: {kpis['avg_score']:.1f}%
        - Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}
        """
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logging.warning(f"AI summary failed: {e}")
        return "AI summary not available."

# -----------------------
# Email Sending
# -----------------------
def send_email(subject: str, html_content: str, recipient: str):
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Preview only.")
        print("\n--- EMAIL PREVIEW ---\n", html_content[:500])
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    try:
        import smtplib
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        logging.info(f"SUCCESS: Email sent to {recipient}")
    except Exception as e:
        logging.error(f"ERROR sending email: {e}")

# -----------------------
# Main
# -----------------------
def main():
    logging.info("--- Starting Weekly Digest Generator ---")
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Digest disabled. Exiting.")
        return

    gcp_key_str = os.environ.get("GCP_SA_KEY")
    bq_client = get_bigquery_client(config, gcp_key_str)
    table_ref = f"{config['google_bigquery']['project_id']}.{config['google_bigquery']['dataset_id']}.{config['google_bigquery']['table_id']}"

    # Sheets auth (for fallback)
    from main import authenticate_google_services
    _, gsheets_client = authenticate_google_services()

    for manager, email in config.get("manager_emails", {}).items():
        logging.info(f"--- Generating digest for {manager} ---")

        if bq_client:
            team_records = fetch_manager_data_bigquery(bq_client, table_ref, manager)
        else:
            team_records = fetch_manager_data_sheets(gsheets_client, config, manager)

        if not team_records:
            logging.info(f"No data for {manager} this week.")
            continue

        kpis, team_data, coaching_notes = process_team_data(team_records)
        ai_summary = generate_ai_summary(manager, kpis, team_data, config)
        html_email = email_formatter.create_manager_digest_email(manager, kpis, team_data, coaching_notes, ai_summary)

        subject = f"Weekly Meeting Digest | {manager} | {time.strftime('%b %d, %Y')}"
        send_email(subject, html_email, email)

    logging.info("--- Weekly Digest Generator Finished ---")

if __name__ == "__main__":
    main()
