# digest_generator.py
import os
import yaml
import logging
import json
import time
from typing import Dict, List, Any
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2 import service_account
import gspread
import google.generativeai as genai
import email_formatter
import sheets

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_google_sheets(config: Dict):
    try:
        gcp_key_str = os.environ.get("GCP_SA_KEY")
        if not gcp_key_str:
            raise ValueError("Missing GCP_SA_KEY")
        creds_info = json.loads(gcp_key_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logging.error(f"CRITICAL: Could not authenticate with Google Sheets: {e}")
        raise

def fetch_manager_data(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    try:
        ss = gsheets_client.open_by_key(config["google_sheets"]["sheet_id"])
        ws = ss.worksheet(config["google_sheets"]["results_tab_name"])
        records = ws.get_all_records()
        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(days=7)
        manager_records = []
        for r in records:
            if str(r.get("Manager", "")).strip() != manager_name:
                continue
            try:
                meeting_date = None
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%y"):
                    try:
                        from datetime import datetime
                        meeting_date = datetime.strptime(str(r.get("Date", "")).strip(), fmt)
                        break
                    except Exception:
                        continue
                if not meeting_date:
                    continue
                if meeting_date >= cutoff:
                    manager_records.append(r)
            except Exception:
                continue
        logging.info(f"Found {len(manager_records)} records for {manager_name}.")
        return manager_records
    except Exception as e:
        logging.error(f"Could not fetch data for manager {manager_name}: {e}")
        return []

def process_team_data(team_records: List[Dict]):
    total_meetings = len(team_records)
    total_pipeline = sum(float(r.get("Amount Value") or 0) for r in team_records)
    avg_score = (sum(float(r.get("% Score") or 0) for r in team_records) / total_meetings) if total_meetings else 0
    kpis = {"total_meetings": total_meetings, "avg_score": avg_score, "total_pipeline": total_pipeline}
    reps = sorted(set(r.get("Owner (Who handled the meeting)", "") for r in team_records))
    team_performance = []
    for rep in reps:
        rep_meetings = [r for r in team_records if r.get("Owner (Who handled the meeting)", "") == rep]
        rep_avg_score = (sum(float(r.get("% Score") or 0) for r in rep_meetings) / len(rep_meetings)) if rep_meetings else 0
        rep_pipeline = sum(float(r.get("Amount Value") or 0) for r in rep_meetings)
        team_performance.append({"owner": rep, "meetings": len(rep_meetings), "avg_score": rep_avg_score, "pipeline": rep_pipeline, "score_change": 0})
    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), []

def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    logging.info(f"Generating AI summary for {manager_name}...")
    try:
        gem_key = os.environ.get("GEMINI_API_KEY")
        if gem_key:
            genai.configure(api_key=gem_key)
            model = genai.GenerativeModel(config["analysis"]["gemini_model"])
            prompt = f"You are a senior sales analyst. Summarize for {manager_name}. KPIs: {kpis}. Team: {json.dumps(team_data)}"
            resp = model.generate_content(prompt)
            return resp.text.strip()
        # fallback to OpenRouter (openai)
        import openai
        openai.api_key = os.environ.get("OPENROUTER_API_KEY")
        if not openai.api_key:
            return "AI summary not available."
        response = openai.ChatCompletion.create(model=config["analysis"]["openrouter_model_name"], messages=[{"role":"user","content":prompt}])
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.error(f"AI summary failed: {e}")
        return "AI summary not available."

def send_email(subject: str, html_content: str, recipient: str):
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")
    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Skipping email send. Showing preview.")
        print("\n--- EMAIL PREVIEW ---\n", subject, recipient, html_content[:500])
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
        logging.error(f"Failed to send email: {e}")

def main():
    logging.info("--- Starting Weekly Digest Generator ---")
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled in config.yaml.")
        return
    gsheets_client = authenticate_google_sheets(config)
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
        html_email = email_formatter.create_manager_digest_email(manager, kpis, team_data, coaching_notes, ai_summary)
        subject = f"Weekly Meeting Digest | {manager} | {time.strftime('%b %d, %Y')}"
        send_email(subject, html_email, email)
    logging.info("--- Weekly Digest Generator Finished ---")

if __name__ == "__main__":
    main()
