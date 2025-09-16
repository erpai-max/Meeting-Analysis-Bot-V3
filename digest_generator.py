import os
import yaml
import logging
import time
import json
from typing import Dict, List, Any
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
from google.oauth2 import service_account
import google.generativeai as genai

import sheets
import email_formatter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -----------------------
# Authenticate Google Sheets
# -----------------------
def get_sheets_client() -> gspread.Client:
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

# -----------------------
# Data Fetching from Sheets
# -----------------------
def fetch_manager_data(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    """Fetches all analysis rows for a given manager from Google Sheets."""
    all_records = sheets.get_all_results(gsheets_client, config)
    return [r for r in all_records if str(r.get("Manager", "")).strip() == manager_name]

# -----------------------
# Data Processing
# -----------------------
def process_team_data(team_records: List[Dict]) -> Dict:
    """Aggregates raw data into KPIs and team performance list."""
    total_meetings = len(team_records)
    total_pipeline = sum(float(r.get("Amount Value") or 0) for r in team_records)
    avg_score = (
        sum(float(r.get("% Score") or 0) for r in team_records) / total_meetings
        if total_meetings > 0 else 0
    )

    team_performance = []
    reps = sorted(set(r.get("Owner (Who handled the meeting)", "Unknown") for r in team_records))
    for rep in reps:
        rep_meetings = [r for r in team_records if r.get("Owner (Who handled the meeting)") == rep]
        rep_avg_score = sum(float(r.get("% Score") or 0) for r in rep_meetings) / len(rep_meetings)
        rep_pipeline = sum(float(r.get("Amount Value") or 0) for r in rep_meetings)

        team_performance.append({
            "owner": rep,
            "meetings": len(rep_meetings),
            "avg_score": rep_avg_score,
            "pipeline": rep_pipeline,
            "score_change": 0.0,  # WoW change skipped in Sheets-only mode
        })

    return {
        "kpis": {"total_meetings": total_meetings, "avg_score": avg_score, "total_pipeline": total_pipeline},
        "team_performance": sorted(team_performance, key=lambda x: x["avg_score"], reverse=True),
        "coaching_notes": []
    }

# -----------------------
# AI Summary
# -----------------------
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Generates executive summary with Gemini (Sheets-only)."""
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        return "AI summary not available."

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel(config["analysis"]["gemini_model"])

    prompt = f"""
    You are a senior sales analyst writing a weekly digest for {manager_name}.
    KPIs: {kpis}
    Team Performance: {json.dumps(team_data, indent=2)}
    Write a concise, 2–3 sentence executive summary.
    """
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logging.error(f"AI summary failed: {e}")
        return "AI summary not available."

# -----------------------
# Email Sending
# -----------------------
def send_email(subject: str, html_content: str, recipient: str):
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Skipping email send.")
        print(f"\n--- EMAIL PREVIEW ---\nTO: {recipient}\nSUBJECT: {subject}\n{html_content[:500]}...\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.sendmail(sender, recipient, msg.as_string())
    logging.info(f"SUCCESS: Email sent to {recipient}")

# -----------------------
# Main
# -----------------------
def main():
    logging.info("--- Starting Weekly Digest Generator ---")

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled. Exiting.")
        return

    gsheets_client = get_sheets_client()
    manager_emails = config.get("manager_emails", {})

    for manager, email in manager_emails.items():
        team_records = fetch_manager_data(gsheets_client, config, manager)
        if not team_records:
            logging.info(f"No data for {manager} this week.")
            continue

        processed = process_team_data(team_records)
        ai_summary = generate_ai_summary(manager, processed["kpis"], processed["team_performance"], config)

        html_email = email_formatter.create_manager_digest_email(
            manager, processed["kpis"], processed["team_performance"], processed["coaching_notes"], ai_summary
        )
        subject = f"Weekly Meeting Digest | {manager} | {time.strftime('%b %d, %Y')}"
        send_email(subject, html_email, email)

    logging.info("--- Weekly Digest Generator Finished ---")


if __name__ == "__main__":
    main()
