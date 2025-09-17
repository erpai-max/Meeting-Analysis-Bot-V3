# digest_generator.py
import os
import yaml
import logging
import json
import time
from typing import Dict, List, Any, Tuple, Optional
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# Google / AI libs
from google.oauth2 import service_account

# We'll import BigQuery/gspread lazily depending on config to avoid unnecessary failures
import google.generativeai as genai

# Try to import email_formatter; if unavailable (e.g., matplotlib missing), provide a simple fallback
try:
    import email_formatter
except Exception as e:
    logging.warning(f"email_formatter import failed ({e}); using simple fallback formatter.")

    # minimal fallback implementations
    def format_currency(value: float) -> str:
        try:
            if value is None:
                return "₹0"
            return f"₹{float(value):,.0f}"
        except Exception:
            return "₹0"

    def create_manager_digest_email(manager_name, kpis, team_data, coaching_notes, ai_summary):
        safe_summary = (str(ai_summary) or "").replace("\n", "<br>")
        rows = ""
        for m in team_data:
            rows += f"<tr><td>{m.get('owner')}</td><td>{m.get('meetings')}</td><td>{m.get('avg_score'):.1f}%</td><td>{format_currency(m.get('pipeline'))}</td></tr>"
        return f"""
        <html><body>
        <h1>Weekly Meeting Digest - {manager_name}</h1>
        <p><b>AI Summary:</b><br>{safe_summary}</p>
        <p><b>KPIs:</b> Meetings: {kpis.get('total_meetings')} | Avg Score: {kpis.get('avg_score'):.1f}% | Pipeline: {format_currency(kpis.get('total_pipeline'))}</p>
        <table border="1"><thead><tr><th>Owner</th><th>Meetings</th><th>Avg Score</th><th>Pipeline</th></tr></thead><tbody>{rows}</tbody></table>
        </body></html>
        """

    # package-like shim so rest of code can call same names
    class email_formatter:
        format_currency = staticmethod(format_currency)
        create_manager_digest_email = staticmethod(create_manager_digest_email)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------------------------------------------
# Helpers: Auth
# ----------------------------------------------------------
def get_service_account_credentials() -> Optional[service_account.Credentials]:
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        logging.error("GCP_SA_KEY environment variable not set.")
        return None
    try:
        creds_info = json.loads(gcp_key_str)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        return creds
    except Exception as e:
        logging.error(f"Failed to load service account credentials from GCP_SA_KEY: {e}")
        return None

def get_bigquery_client(config: Dict) -> Optional["google.cloud.bigquery.Client"]:
    try:
        from google.cloud import bigquery
    except Exception as e:
        logging.error(f"google-cloud-bigquery not installed or import failed: {e}")
        return None

    creds = get_service_account_credentials()
    if not creds:
        return None
    try:
        project_id = config["google_bigquery"]["project_id"]
        client = bigquery.Client(credentials=creds, project=project_id)
        logging.info(f"SUCCESS: Authenticated with BigQuery for project '{project_id}'.")
        return client
    except Exception as e:
        logging.error(f"BigQuery client creation failed: {e}")
        return None

def get_gsheets_client(config: Dict):
    try:
        import gspread
    except Exception as e:
        logging.error(f"gspread not installed or import failed: {e}")
        return None
    creds = get_service_account_credentials()
    if not creds:
        return None
    # authorize with required scopes
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        delegated = creds.with_scopes(scopes)
        client = gspread.authorize(delegated)
        logging.info("SUCCESS: Authenticated with Google Sheets")
        return client
    except Exception as e:
        logging.error(f"Failed to authorize gspread client: {e}")
        return None

# ----------------------------------------------------------
# Data fetching: BigQuery or Sheets
# ----------------------------------------------------------
def fetch_manager_data_bq(bq_client, table_ref: str, manager_name: str) -> List[Dict]:
    """
    Fetch rows for manager_name from BigQuery. Returns list of dicts with normalized fields:
    Owner, Team, Percent_Score, Amount_Value, meeting_date (as datetime.date)
    """
    if not bq_client:
        logging.error("BigQuery client not available.")
        return []

    # Attempt to parse common numeric columns from your table; adjust names if needed in BigQuery schema
    query = f"""
    SELECT
      COALESCE(NULLIF(TRIM(Owner), ''), 'N/A') AS Owner,
      COALESCE(NULLIF(TRIM(Team), ''), 'N/A') AS Team,
      SAFE_CAST(REPLACE(REPLACE(COALESCE(NULLIF(TRIM(`% Score`),''), '0'), '%',''), ',' , '') AS FLOAT64) AS Percent_Score,
      SAFE_CAST(REPLACE(REPLACE(COALESCE(NULLIF(TRIM(`Amount Value`),''), '0'), '₹',''), ',' , '') AS FLOAT64) AS Amount_Value,
      PARSE_DATE('%Y-%m-%d', Date) AS meeting_date
    FROM `{table_ref}`
    WHERE Manager = @mgr
    """
    try:
        job_config = None
        # use query parameters to avoid injection
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("mgr", "STRING", manager_name)]
        )
        query_job = bq_client.query(query, job_config=job_config)
        rows = []
        for r in query_job.result():
            rows.append({
                "Owner": r["Owner"],
                "Team": r["Team"],
                "Percent_Score": float(r["Percent_Score"]) if r["Percent_Score"] is not None else 0.0,
                "Amount_Value": float(r["Amount_Value"]) if r["Amount_Value"] is not None else 0.0,
                "meeting_date": r["meeting_date"],
                "prev_week_avg_score": None
            })
        logging.info(f"Found {len(rows)} records for {manager_name} (BigQuery).")
        return rows
    except Exception as e:
        logging.error(f"Could not fetch data for manager {manager_name} from BigQuery: {e}")
        return []

def try_parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # try common cases: dd-mm-yy
    try:
        return datetime.strptime(s, "%d-%m-%y")
    except Exception:
        return None

def fetch_manager_data_sheets(gsheets_client, config: Dict, manager_name: str) -> List[Dict]:
    """
    Fetch rows for a manager from Google Sheets (Results tab).
    Returns list of dicts with Owner, Team, Percent_Score, Amount_Value, meeting_date.
    """
    if not gsheets_client:
        logging.error("Sheets client not available.")
        return []

    try:
        sheet_id = config["google_sheets"]["sheet_id"]
        results_tab = config["google_sheets"].get("results_tab_name", "Results")
        spreadsheet = gsheets_client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(results_tab)
        records = ws.get_all_records()
    except Exception as e:
        logging.error(f"Could not open Results sheet: {e}")
        return []

    cutoff = datetime.now() - timedelta(days=7)
    rows = []
    for r in records:
        try:
            if str(r.get("Manager", "")).strip() != manager_name:
                continue
            meeting_date = try_parse_date(r.get("Date", ""))
            if meeting_date is None:
                # If no date, treat as older (skip) — but you could include by removing this filter
                continue
            if meeting_date < cutoff:
                continue
            percent_str = str(r.get("% Score", "")).replace("%", "").replace(",", "").strip()
            amount_str = str(r.get("Amount Value", "")).replace("₹", "").replace(",", "").strip()
            rows.append({
                "Owner": r.get("Owner (Who handled the meeting)", "") or r.get("Owner", ""),
                "Team": r.get("Team", ""),
                "Percent_Score": float(percent_str) if percent_str else 0.0,
                "Amount_Value": float(amount_str) if amount_str else 0.0,
                "meeting_date": meeting_date.date(),
                "prev_week_avg_score": None
            })
        except Exception as e:
            logging.debug(f"Skipping record due to parse error: {e}")
            continue

    logging.info(f"Found {len(rows)} records for {manager_name} (Sheets).")
    return rows

# ----------------------------------------------------------
# Aggregation & AI summary
# ----------------------------------------------------------
def process_team_data(team_records: List[Dict]) -> Tuple[Dict, List[Dict], List[Dict]]:
    """Aggregate KPIs and prepare team rows and coaching notes."""
    total_meetings = len(team_records)
    total_pipeline = sum(r.get("Amount_Value", 0.0) or 0.0 for r in team_records)
    avg_score = (
        sum(r.get("Percent_Score", 0.0) or 0.0 for r in team_records) / total_meetings
        if total_meetings > 0 else 0.0
    )
    kpis = {"total_meetings": total_meetings, "avg_score": avg_score, "total_pipeline": total_pipeline}

    owners = sorted({r.get("Owner") for r in team_records if r.get("Owner")})
    team_performance = []
    for owner in owners:
        rep_meetings = [r for r in team_records if r.get("Owner") == owner]
        rep_avg = (sum(r.get("Percent_Score", 0.0) for r in rep_meetings) / len(rep_meetings)) if rep_meetings else 0.0
        rep_pipeline = sum(r.get("Amount_Value", 0.0) for r in rep_meetings)
        team_performance.append({
            "owner": owner,
            "meetings": len(rep_meetings),
            "avg_score": rep_avg,
            "pipeline": rep_pipeline,
            "score_change": 0.0
        })

    coaching_notes = []  # placeholder for future logic
    return kpis, sorted(team_performance, key=lambda x: x["avg_score"], reverse=True), coaching_notes

def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Use Gemini as primary LLM, fallback to OpenRouter/OpenAI."""
    logging.info(f"Generating AI summary for {manager_name}...")
    prompt = f"""
You are a senior sales analyst providing a short executive summary to {manager_name}.
KPIs:
- Total Meetings: {kpis['total_meetings']}
- Team Avg Score: {kpis['avg_score']:.1f}%
- Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

Team Performance:
{json.dumps(team_data, indent=2)}

Write a concise 2-3 sentence executive summary focusing on the most important trend or recommendation.
"""
    # Try Gemini
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if config.get("analysis", {}).get("use_gemini", True) and gemini_key:
        try:
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))
            response = model.generate_content(prompt)
            text = response.text.strip()
            logging.info("SUCCESS: AI summary (Gemini).")
            return text
        except Exception as e:
            logging.warning(f"Gemini failed: {e}")

    # Fallback to OpenRouter/OpenAI
    try:
        import openai
        openai.api_key = os.environ.get("OPENROUTER_API_KEY")
        model_name = config["analysis"].get("openrouter_model_name")
        if not openai.api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set for fallback")
        # Use ChatCompletion or Completion depending on availability
        try:
            resp = openai.ChatCompletion.create(model=model_name, messages=[{"role":"user","content":prompt}])
            text = resp["choices"][0]["message"]["content"].strip()
        except Exception:
            # older API fallback
            resp = openai.Completion.create(model=model_name, prompt=prompt, max_tokens=200)
            text = resp["choices"][0]["text"].strip()
        logging.info("SUCCESS: AI summary (OpenRouter/OpenAI).")
        return text
    except Exception as e:
        logging.error(f"AI summary fallback failed: {e}")
        return "AI summary not available."

# ----------------------------------------------------------
# Email send
# ----------------------------------------------------------
def send_email(subject: str, html_content: str, recipient: str):
    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")
    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Skipping SMTP send; printing preview.")
        print("\n--- EMAIL PREVIEW ---")
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:800]}...\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        logging.info(f"SUCCESS: Email sent to {recipient}")
    except Exception as e:
        logging.error(f"Failed to send email to {recipient}: {e}")

# ----------------------------------------------------------
# Main flow
# ----------------------------------------------------------
def main():
    logging.info("--- Starting Weekly Digest Generator ---")
    # load config
    cfg_path = "config.yaml"
    if not os.path.exists(cfg_path):
        logging.error("config.yaml not found. Exiting.")
        return
    with open(cfg_path, "r") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled in config. Exiting.")
        return

    use_bq = config.get("use_bigquery", False)

    # Prepare clients
    bq_client = None
    gsheets_client = None
    if use_bq:
        bq_client = get_bigquery_client(config)
        if not bq_client:
            logging.error("BigQuery enabled but client creation failed. Falling back to Sheets if possible.")
            use_bq = False

    if not use_bq:
        gsheets_client = get_gsheets_client(config)
        if not gsheets_client:
            logging.error("Sheets client creation failed. Exiting.")
            return

    # Table ref if BigQuery
    table_ref = None
    if use_bq and bq_client:
        table_ref = f"{bq_client.project}.{config['google_bigquery']['dataset_id']}.{config['google_bigquery']['table_id']}"

    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No manager_emails defined in config. Exiting.")
        return

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        if use_bq:
            team_records = fetch_manager_data_bq(bq_client, table_ref, manager)
        else:
            team_records = fetch_manager_data_sheets(gsheets_client, config, manager)

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
