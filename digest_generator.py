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

import sheets
import email_formatter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_google_sheets(config: Dict):
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        raise ValueError("Missing GCP_SA_KEY")
    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(config["google_sheets"]["sheet_id"])
    sheets.ensure_tabs_exist(sheet, config)
    return sheet

def fetch_manager_data(sheet, config: Dict, manager_name: str) -> List[Dict]:
    try:
        ws = sheet.worksheet(config["google_sheets"]["results_tab_name"])
        records = ws.get_all_records()
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

def process_team_data(team_records: List[Dict]) -> (Dict[str, Any], List[Dict], List[Dict]):
    total_meetings = len(team_records)
    def fnum(v): 
        try: return float(str(v).replace(",","").replace("%","").strip() or 0)
        except: return 0.0

    total_pipeline = sum(fnum(r.get("Amount Value")) for r in team_records)
    avg_score = (sum(fnum(r.get("% Score")) for r in team_records) / total_meetings) if total_meetings else 0.0

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    reps = sorted(set(r.get("Owner (Who handled the meeting)", "") for r in team_records))
    team_perf = []
    for rep in reps:
        rep_meetings = [r for r in team_records if r.get("Owner (Who handled the meeting)", "") == rep]
        rep_avg = (sum(fnum(r.get("% Score")) for r in rep_meetings) / len(rep_meetings)) if rep_meetings else 0
        rep_pipe = sum(fnum(r.get("Amount Value")) for r in rep_meetings)
        team_perf.append({"owner": rep, "meetings": len(rep_meetings), "avg_score": rep_avg, "pipeline": rep_pipe, "score_change": 0})
    team_perf.sort(key=lambda x: x["avg_score"], reverse=True)

    return kpis, team_perf, []

def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    logging.info(f"Generating AI summary for {manager_name}...")
    prompt = f"""
    You are a sales analyst. Write a concise 2–3 sentence weekly summary for {manager_name}.

    KPIs:
    - Total Meetings: {kpis['total_meetings']}
    - Avg Score: {kpis['avg_score']:.1f}%
    - Pipeline: {email_formatter.format_currency(kpis['total_pipeline'])}

    Team Performance:
    {json.dumps(team_data, indent=2)}
    """
    # Gemini first
    try:
        import google.generativeai as genai
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model_name = config["analysis"]["gemini_model"]
        response = genai.GenerativeModel(model_name).generate_content(prompt)
        return (response.text or "").strip() or "AI summary not available."
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

def send_email(subject: str, html_content: str, recipient: str):
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Printing preview.")
        print("\n--- EMAIL CONTENT PREVIEW ---\n")
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:1200]}...\n")
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
        logging.error(f"ERROR sending email: {e}")

def main():
    logging.info("--- Starting Weekly Digest Generator ---")

    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled. Exiting.")
        return

    sheet = authenticate_google_sheets(config)

    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined. Exiting.")
        return

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        team_records = fetch_manager_data(sheet, config, manager)
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
