# digest_generator.py

import os
import re
import yaml
import json
import time
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta

import gspread
from google.oauth2 import service_account
import google.generativeai as genai

# local modules
import sheets
import email_formatter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -----------------------
# Google Sheets Auth
# -----------------------
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets and return an opened Spreadsheet handle."""
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        raise ValueError("Missing GCP_SA_KEY environment variable")

    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = config["google_sheets"]["sheet_id"]
    logging.info("SUCCESS: Authenticated Google Sheets")
    return client.open_by_key(sheet_id)


# -----------------------
# Parsing helpers
# -----------------------
_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%b %d, %Y",
    "%d %b %Y",
]

def _parse_date(d: Any) -> datetime | None:
    """Parse many common date shapes; also trims time portion if present."""
    if not d:
        return None
    s = str(d).strip()
    if not s or s.upper() in ("N/A", "NA", "NONE"):
        return None

    # ISO-like with time: 2025-09-04T16:33:00Z
    if "T" in s:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _to_float_percent(val: Any) -> float | None:
    """Extract a number from '83.3%' / '83' / 'N/A'. Return float percentage (0-100)."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.upper() in ("N/A", "NA", "NONE"):
        return None
    m = re.search(r"[-+]?\d*\.?\d+", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group())
    except Exception:
        return None


def _to_float_amount_inr(val: Any) -> float:
    """
    Convert '₹53,000', '53,000', '53000.00', '₹23 per flat' into first numeric value.
    Returns 0.0 if not present.
    """
    if val is None:
        return 0.0
    s = str(val)
    m = re.search(r"[-+]?\d[\d,]*\.?\d*", s.replace("₹", ""))
    if not m:
        return 0.0
    try:
        return float(m.group().replace(",", ""))
    except Exception:
        return 0.0


def _clean_name(s: Any) -> str:
    return str(s or "").strip()


# -----------------------
# Data Fetching from Sheets
# -----------------------
def fetch_manager_data(spreadsheet, config: Dict, manager_name: str) -> List[Dict]:
    """
    Fetch rows from Results tab for the given manager within last_n_days.
    Ignores rows without a parseable date. If results_tab_name missing, defaults "Results".
    """
    tab = config["google_sheets"].get("results_tab_name", "Results")
    last_n = int(config.get("weekly_digest", {}).get("last_n_days", 7))

    try:
        ws = spreadsheet.worksheet(tab)
    except Exception as e:
        logging.error(f"Could not open worksheet '{tab}': {e}")
        return []

    try:
        rows = ws.get_all_records()
    except Exception as e:
        logging.error(f"Could not read records from '{tab}': {e}")
        return []

    now = datetime.now()
    cutoff = now - timedelta(days=last_n)
    mgr_norm = manager_name.strip().lower()

    picked: List[Dict] = []
    skipped = 0

    for r in rows:
        mgr = _clean_name(r.get("Manager", ""))
        if mgr.lower() != mgr_norm:
            continue
        dt = _parse_date(r.get("Date"))
        if not dt:
            skipped += 1
            continue
        if dt >= cutoff:
            picked.append(r)

    if skipped:
        logging.info(f"Found {len(picked)} records for {manager_name}. (Skipped {skipped})")
    else:
        logging.info(f"Found {len(picked)} records for {manager_name}.")

    return picked


# -----------------------
# Aggregation / KPIs
# -----------------------
def _safe_owner(r: Dict) -> str:
    return _clean_name(r.get("Owner (Who handled the meeting)", "")) or "Unknown"


def process_team_data(team_records: List[Dict], last_n_days: int) -> Tuple[Dict, List[Dict], List[Dict]]:
    """
    Build KPIs + per-rep metrics + basic coaching notes.
    - avg_score uses "% Score" parsed percent
    - pipeline uses "Amount Value"
    - previous week window = [now-14d, now-7d)
    """
    now = datetime.now()
    w2_start = now - timedelta(days=14)
    w2_end = now - timedelta(days=7)

    total_meetings = len(team_records)
    scores: List[float] = []
    pipeline_total = 0.0

    # For score change, collect prev week averages per owner
    prev_week_by_owner: Dict[str, List[float]] = {}

    for r in team_records:
        # Current-week score
        s = _to_float_percent(r.get("% Score"))
        if s is not None:
            scores.append(s)

        # Amount value
        pipeline_total += _to_float_amount_inr(r.get("Amount Value"))

        # Build prev-week owner samples from *all* records that are in prev week window.
        dt = _parse_date(r.get("Date"))
        if dt and (w2_start <= dt < w2_end):
            owner = _safe_owner(r)
            prev_score = _to_float_percent(r.get("% Score"))
            if prev_score is not None:
                prev_week_by_owner.setdefault(owner, []).append(prev_score)

    avg_score = (sum(scores) / len(scores)) if scores else 0.0

    # Per-rep metrics from *current* team_records set
    team_performance: List[Dict] = []
    owners = sorted({ _safe_owner(r) for r in team_records })
    for owner in owners:
        rep_meetings = [r for r in team_records if _safe_owner(r) == owner]
        rep_scores = [_to_float_percent(r.get("% Score")) for r in rep_meetings if _to_float_percent(r.get("% Score")) is not None]
        rep_avg = (sum(rep_scores) / len(rep_scores)) if rep_scores else 0.0
        rep_pipeline = sum(_to_float_amount_inr(r.get("Amount Value")) for r in rep_meetings)

        prev_list = prev_week_by_owner.get(owner, [])
        prev_avg = (sum(prev_list) / len(prev_list)) if prev_list else rep_avg
        score_change = rep_avg - prev_avg

        team_performance.append({
            "owner": owner,
            "meetings": len(rep_meetings),
            "avg_score": rep_avg,
            "pipeline": rep_pipeline,
            "score_change": score_change,
        })

    # Simple coaching notes: flag reps under 70% average
    coaching_notes: List[Dict] = []
    for rep in team_performance:
        if rep["avg_score"] < 70:
            coaching_notes.append({
                "owner": rep["owner"],
                "lowest_metric": "Overall % Score",
                "lowest_score": rep["avg_score"],
                "note": "Focus on tightening discovery/objection handling; rehearse sharp ERP/ASP value pitches."
            })

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": pipeline_total,
        "window_days": last_n_days,
    }
    team_sorted = sorted(team_performance, key=lambda x: x["avg_score"], reverse=True)
    return kpis, team_sorted, coaching_notes


# -----------------------
# AI Summary
# -----------------------
def _generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Use Gemini, fallback to OpenRouter."""
    prompt = f"""
You are a senior sales analyst writing a 2–3 sentence executive summary for {manager_name}.
Base your summary ONLY on the data below. Mention one key trend, a bright spot, and one improvement area.

KPIs:
- Total Meetings: {kpis['total_meetings']}
- Team Avg Score: {kpis['avg_score']:.1f}%
- Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

Team Performance (owner, avg_score, meetings, pipeline, WoW change):
{json.dumps([
    {"owner": t["owner"], "avg_score": round(t["avg_score"], 1), "meetings": t["meetings"], "pipeline": t["pipeline"], "score_change": round(t["score_change"],1)}
    for t in team_data
], indent=2)}

Summary:
"""
    # Try Gemini
    try:
        if os.environ.get("GEMINI_API_KEY"):
            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            model = genai.GenerativeModel(config["analysis"].get("gemini_model", "gemini-1.5-flash"))
            resp = model.generate_content(prompt)
            text = getattr(resp, "text", "") or ""
            if text.strip():
                return text.strip()
    except Exception as e:
        logging.error(f"Gemini summary failed: {e}")

    # Fallback OpenRouter
    try:
        from openai import OpenAI
        api_key = os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not set")

        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        completion = client.chat.completions.create(
            model=config["analysis"].get("openrouter_model_name", "meta-llama/llama-3.1-8b-instruct"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return completion.choices[0].message.content.strip()
    except Exception as e2:
        logging.error(f"OpenRouter summary failed: {e2}")
        return "Summary not available this week."


# -----------------------
# Email sending (robust to blank envs)
# -----------------------
def send_email(subject: str, html_content: str, recipient: str):
    """Sends the digest email via SMTP (SSL 465 default or STARTTLS 587 with MAIL_USE_TLS=true)."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    sender = (os.environ.get("MAIL_USERNAME") or "").strip()
    password = (os.environ.get("MAIL_PASSWORD") or "").strip()

    host = (os.environ.get("MAIL_SMTP_HOST") or "smtp.gmail.com").strip()
    port_str = (os.environ.get("MAIL_SMTP_PORT") or "").strip()
    use_tls_str = (os.environ.get("MAIL_USE_TLS") or "").strip().lower()

    port = int(port_str) if port_str.isdigit() else 465
    use_tls = use_tls_str in ("1", "true", "yes")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD missing. Printing preview instead of sending.")
        print("\n--- EMAIL CONTENT PREVIEW ---\n")
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:1000]}...\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    try:
        if use_tls:
            server = smtplib.SMTP(host, port)
            server.ehlo()
            server.starttls()
            server.login(sender, password)
        else:
            server = smtplib.SMTP_SSL(host, port)
            server.login(sender, password)

        server.sendmail(sender, [recipient], msg.as_string())
        server.quit()
        logging.info(f"SUCCESS: Email sent to {recipient}")

    except smtplib.SMTPAuthenticationError as e:
        code = getattr(e, "smtp_code", None)
        emsg = getattr(e, "smtp_error", b"").decode(errors="ignore")
        logging.error(
            f"SMTP auth failed ({code}): {emsg}. "
            "Tip: For Gmail, enable 2-Step Verification and use an App Password. "
            "MAIL_USERNAME must match the account that owns the App Password."
        )
    except Exception as e:
        logging.error(f"ERROR sending email: {e}")


# -----------------------
# Main
# -----------------------
def main():
    logging.info("--- Starting Weekly Digest Generator ---")

    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", False):
        logging.info("Weekly digest disabled in config.yaml. Exiting.")
        return

    spreadsheet = authenticate_google_sheets(config)
    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined under 'manager_emails'. Exiting.")
        return

    last_n_days = int(config.get("weekly_digest", {}).get("last_n_days", 7))

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        records = fetch_manager_data(spreadsheet, config, manager)

        if not records:
            logging.info(f"No data for {manager} this week.")
            continue

        kpis, team_data, coaching_notes = process_team_data(records, last_n_days)
        ai_summary = _generate_ai_summary(manager, kpis, team_data, config)

        html_email = email_formatter.create_manager_digest_email(
            manager_name=manager,
            kpis=kpis,
            team_data=team_data,
            coaching_notes=coaching_notes,
            ai_summary=ai_summary,
        )

        subject = f"Weekly Meeting Digest | {manager} | {time.strftime('%b %d, %Y')}"
        send_email(subject, html_email, email)

    logging.info("--- Weekly Digest Generator Finished ---")


if __name__ == "__main__":
    main()
