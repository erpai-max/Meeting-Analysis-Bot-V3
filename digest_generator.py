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
import gspread
import google.generativeai as genai

# Import email formatter (with charts)
import email_formatter
import sheets  # ✅ reuse our sheets.py functions

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =======================
# Data Fetching (Google Sheets instead of BQ)
# =======================
def fetch_manager_data(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    """Fetches performance data for a manager's team (last 7 days + previous week)."""
    try:
        all_records = sheets.get_all_results(gsheets_client, config)
        manager_records = [r for r in all_records if r.get("Manager") == manager_name]

        # Convert numeric fields safely
        for r in manager_records:
            try:
                r["Percent_Score"] = float(r.get("% Score") or 0)
            except:
                r["Percent_Score"] = 0.0
            try:
                r["Amount_Value"] = float(r.get("Amount Value") or 0)
            except:
                r["Amount_Value"] = 0.0

        logging.info(f"Found {len(manager_records)} records for {manager_name}.")
        return manager_records
    except Exception as e:
        logging.error(f"Could not fetch data for {manager_name}: {e}")
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
        if total_meetings > 0 else 0
    )

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    team_performance = []
    reps = sorted(set(r.get("Owner (Who handled the meeting)") for r in team_records))
    for rep in reps:
        rep_meetings = [r for r in team_records if r.get("Owner (Who handled the meeting)") == rep]
        rep_avg_score = (
            sum(r["Percent_Score"] for r in rep_meetings if r.get("Percent_Score")) / len(rep_meetings)
            if rep_meetings else 0
        )
        rep_pipeline = sum(r["Amount_Value"] for r in rep_meetings if r.get("Amount_Value"))

        team_performance.append(
            {
                "owner": rep,
                "meetings": len(rep_meetings),
                "avg_score": rep_avg_score,
                "pipeline": rep_pipeline,
                "score_change": 0.0,  # WoW change removed (need history tracking for that)
            }
        )

    coaching_notes = []  # optional extension
    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), coaching_notes


# =======================
# AI Summary
# =======================
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Uses Gemini/OpenRouter to generate a 2–3 sentence executive summary."""
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

    Summary:
    """

    try:
        gemini_key = os.environ.get("GEMINI_API_KEY")
        if gemini_key:
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel(config["analysis"]["gemini_model"])
            response = model.generate_content(prompt)
            return response.text.strip()

        # Fallback → OpenRouter
        openrouter_key = os.environ.get("OPENROUTER_API_KEY")
        if openrouter_key:
            import httpx
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {openrouter_key}"}
            data = {
                "model": config["analysis"]["openrouter_model_name"],
                "messages": [{"role": "user", "content": prompt}],
            }
            resp = httpx.post(url, headers=headers, json=data, timeout=60)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
            else:
                logging.error(f"OpenRouter API failed: {resp.text}")
                return "AI summary not available."

        return "AI summary could not be generated."
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
        import smtplib
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

    # Authenticate Sheets
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    gsheets_client = gspread.authorize(creds)

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
