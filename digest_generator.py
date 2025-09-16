import os
import yaml
import logging
import json
import time
from typing import Dict, List, Any
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
from google.oauth2 import service_account
import google.generativeai as genai

# Import helpers
import email_formatter
import sheets

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =======================
# Authentication
# =======================
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets and return client + worksheet."""
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY environment variable")

        creds_info = json.loads(gcp_key_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)

        sheet_id = config["google_sheets"]["sheet_id"]
        return client.open_by_key(sheet_id)
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate with Google Sheets: {e}")
        raise

# =======================
# Data Fetching
# =======================
def fetch_manager_data(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    """Fetches meeting data for a manager's team from Google Sheets (last 7 days)."""
    try:
        ws = gsheets_client.worksheet(config["google_sheets"]["results_tab_name"])
        records = ws.get_all_records()

        # Convert dates and filter last 7 days
        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(days=7)

        manager_records = []
        for r in records:
            if str(r.get("Manager", "")).strip() != manager_name:
                continue
            try:
                meeting_date = datetime.strptime(str(r.get("Date", "")), "%Y-%m-%d")
            except Exception:
                continue
            if meeting_date >= cutoff:
                manager_records.append(r)

        logging.info(f"Found {len(manager_records)} records for {manager_name}.")
        return manager_records
    except Exception as e:
        logging.error(f"Could not fetch data for manager {manager_name}: {e}")
        return []

# =======================
# Data Processing
# =======================
def process_team_data(team_records: List[Dict]) -> Dict[str, Any]:
    """Aggregates raw data into KPIs + team performance list."""
    total_meetings = len(team_records)
    total_pipeline = sum(float(r.get("Amount Value") or 0) for r in team_records)
    avg_score = (
        sum(float(r.get("% Score") or 0) for r in team_records) / total_meetings
        if total_meetings > 0
        else 0
    )

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    team_performance = []
    reps = sorted(set(r.get("Owner (Who handled the meeting)", "") for r in team_records))
    for rep in reps:
        rep_meetings = [r for r in team_records if r.get("Owner (Who handled the meeting)", "") == rep]
        rep_avg_score = (
            sum(float(r.get("% Score") or 0) for r in rep_meetings) / len(rep_meetings)
            if rep_meetings else 0
        )
        rep_pipeline = sum(float(r.get("Amount Value") or 0) for r in rep_meetings)

        team_performance.append({
            "owner": rep,
            "meetings": len(rep_meetings),
            "avg_score": rep_avg_score,
            "pipeline": rep_pipeline,
            "score_change": 0  # (Optional WoW tracking if needed later)
        })

    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), []

# =======================
# AI Summary
# =======================
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Uses Gemini to generate a 2–3 sentence executive summary (fallback to OpenRouter if quota issue)."""
    logging.info(f"Generating AI summary for {manager_name}...")

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
    """

    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"]["gemini_model"])
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logging.error(f"Gemini failed, trying OpenRouter: {e}")
        try:
            import openai
            openai.api_key = os.environ.get("OPENROUTER_API_KEY")
            response = openai.ChatCompletion.create(
                model=config["analysis"]["openrouter_model_name"],
                messages=[{"role": "user", "content": prompt}],
            )
            return response["choices"][0]["message"]["content"].strip()
        except Exception as e2:
            logging.error(f"OpenRouter also failed: {e2}")
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

    # Auth Sheets
    gsheets_client = authenticate_google_sheets(config)

    # Loop managers
    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined. Exiting.")
        return

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        team_records = fetch_manager_data(gsheets_client, config, manager)

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
