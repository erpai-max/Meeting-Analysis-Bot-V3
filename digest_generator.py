import os
import yaml
import logging
import json
import re
import time
from typing import Dict, List, Any, Optional
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
from google.oauth2 import service_account
import google.generativeai as genai

# Local modules
import email_formatter
import sheets

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -----------------------
# Auth
# -----------------------
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets and return spreadsheet handle."""
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        raise RuntimeError("Missing GCP_SA_KEY env var")
    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet_id = config["google_sheets"]["sheet_id"]
    return client.open_by_key(sheet_id)


# -----------------------
# Utilities
# -----------------------
DATE_PATTERNS = [
    "%Y-%m-%d",  # 2025-09-04
    "%d-%m-%Y",  # 04-09-2025
    "%m-%d-%Y",  # 09-04-2025
    "%Y/%m/%d",  # 2025/09/04
    "%d/%m/%Y",  # 04/09/2025
    "%m/%d/%Y",  # 09/04/2025
    "%d.%m.%Y",  # 04.09.2025
    "%b %d, %Y", # Sep 04, 2025
    "%d %b %Y",  # 04 Sep 2025
]

def parse_date_flexible(value: str):
    """Return datetime.date or None."""
    from datetime import datetime
    if not value or str(value).strip().upper() in ("", "N/A", "NA", "NONE", "NULL"):
        return None
    s = str(value).strip()
    # Some sheets return like 9/4/2025 00:00:00
    s = re.sub(r"\s+\d{2}:\d{2}:\d{2}$", "", s)
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # Last resort: try to pull yyyy-mm-dd pieces out
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        except Exception:
            pass
    return None

def norm(s: Any) -> str:
    return str(s or "").strip()

def icase(s: Any) -> str:
    return norm(s).casefold()

def num_from_text(s: Any) -> float:
    """
    Extract a reasonable float from '₹53,000', '83.3%', 'N/A'.
    Returns 0.0 if cannot parse.
    """
    t = str(s or "")
    if not t or t.upper() in ("N/A", "NA", "NONE", "NULL"):
        return 0.0
    # Keep digits, dot, minus
    cleaned = re.sub(r"[^\d\.\-]", "", t.replace(",", ""))
    try:
        return float(cleaned) if cleaned not in ("", ".", "-", "-.") else 0.0
    except Exception:
        return 0.0


# -----------------------
# Data fetching
# -----------------------
def build_manager_to_owners(config: Dict) -> Dict[str, List[str]]:
    """
    Invert config.manager_map (owner -> {Manager, Team, Email})
    -> manager_to_owners[manager_name] = [owner1, owner2, ...]
    """
    mgr_to_owners: Dict[str, List[str]] = {}
    for owner, meta in (config.get("manager_map") or {}).items():
        mgr = meta.get("Manager", "")
        if not mgr:
            continue
        mgr_to_owners.setdefault(mgr, []).append(owner)
    return mgr_to_owners

def fetch_manager_data(gsheets, config: Dict, manager_name: str) -> List[Dict]:
    """
    Pull rows for a Manager from Google Sheets:
    - If row Manager matches (case-insensitive) manager_name OR
    - If row Owner belongs to that manager via config.manager_map
    Then apply lookback days filter on Date. If date missing and include_undated: include.
    """
    results_tab = config["google_sheets"]["results_tab_name"]
    ws = gsheets.worksheet(results_tab)
    records = ws.get_all_records()  # list of dicts

    from datetime import datetime, timedelta
    lookback_days = int(config.get("weekly_digest", {}).get("lookback_days", 7))
    include_undated = bool(config.get("weekly_digest", {}).get("include_undated", True))
    cutoff = (datetime.now() - timedelta(days=lookback_days)).date()

    mgr_to_owners = build_manager_to_owners(config)
    owners_for_manager = set(mgr_to_owners.get(manager_name, []))
    mgr_key = manager_name.casefold()

    picked: List[Dict] = []
    skipped = 0

    for r in records:
        row_mgr = icase(r.get("Manager"))
        row_owner = norm(r.get("Owner (Who handled the meeting)"))
        row_owner_ic = icase(row_owner)

        belongs = False
        if row_mgr == mgr_key:
            belongs = True
        elif row_owner in owners_for_manager or row_owner_ic in {icase(x) for x in owners_for_manager}:
            belongs = True

        if not belongs:
            skipped += 1
            continue

        # Date filter
        row_date = parse_date_flexible(r.get("Date"))
        if row_date is None:
            if include_undated:
                picked.append(r)
            else:
                skipped += 1
            continue

        if row_date >= cutoff:
            picked.append(r)
        else:
            skipped += 1

    logging.info(f"Found {len(picked)} records for {manager_name}. (Skipped {skipped})")
    return picked


# -----------------------
# Aggregation
# -----------------------
def process_team_data(team_records: List[Dict]) -> (Dict[str, Any], List[Dict], List[Dict]):
    """Compute KPIs + per-rep aggregates."""
    total_meetings = len(team_records)
    total_pipeline = sum(num_from_text(r.get("Amount Value")) for r in team_records)

    scores = [num_from_text(r.get("% Score")) for r in team_records if norm(r.get("% Score"))]
    avg_score = (sum(scores) / len(scores)) if scores else 0.0

    kpis = {
        "total_meetings": total_meetings,
        "avg_score": avg_score,
        "total_pipeline": total_pipeline,
    }

    # per-owner
    by_owner: Dict[str, List[Dict]] = {}
    for r in team_records:
        owner = norm(r.get("Owner (Who handled the meeting)"))
        by_owner.setdefault(owner, []).append(r)

    team_perf: List[Dict] = []
    for owner, rows in by_owner.items():
        mcount = len(rows)
        avg_s = (sum(num_from_text(x.get("% Score")) for x in rows) / mcount) if mcount else 0.0
        pipe = sum(num_from_text(x.get("Amount Value")) for x in rows)
        team_perf.append({
            "owner": owner,
            "meetings": mcount,
            "avg_score": avg_s,
            "pipeline": pipe,
            "score_change": 0.0,  # WoW could be computed later if needed
        })

    team_perf.sort(key=lambda x: x["avg_score"], reverse=True)
    coaching_notes: List[Dict] = []  # add rules later if desired
    return kpis, team_perf, coaching_notes


# -----------------------
# AI Summary
# -----------------------
def generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Gemini first, optional OpenRouter fallback."""
    prompt = f"""
You are a senior sales analyst. Write a concise 2–3 sentence executive summary for manager {manager_name} based on:

KPIs:
- Total Meetings: {kpis['total_meetings']}
- Team Avg Score: {kpis['avg_score']:.1f}%
- Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

Team Performance (owner, avg_score, pipeline):
{json.dumps([{k: (round(v,1) if isinstance(v,(int,float)) else v) for k,v in m.items()} for m in team_data], indent=2)}
"""

    # Try Gemini
    try:
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel(config["analysis"]["gemini_model"])
        resp = model.generate_content(prompt)
        text = getattr(resp, "text", None)
        if not text and getattr(resp, "candidates", None):
            parts = getattr(resp.candidates[0], "content", None)
            if parts and getattr(parts, "parts", None):
                text = "".join(getattr(p, "text", "") for p in parts.parts)
        if text:
            return text.strip()
    except Exception as e:
        logging.error(f"Gemini failed, trying OpenRouter: {e}")

    # Fallback OpenRouter (optional)
    try:
        from openai import OpenAI
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.environ.get("OPENROUTER_API_KEY"))
        completion = client.chat.completions.create(
            model=config["analysis"]["openrouter_model_name"],
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return completion.choices[0].message.content.strip()
    except Exception as e2:
        logging.error(f"OpenRouter also failed: {e2}")
        return "AI summary not available."


# -----------------------
# Email
# -----------------------
def send_email(subject: str, html_content: str, recipient: str):
    """Sends the digest email via SMTP (supports SSL 465 or STARTTLS 587)."""
    import smtplib, os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    sender = os.environ.get("MAIL_USERNAME")
    password = os.environ.get("MAIL_PASSWORD")
    host = os.environ.get("MAIL_SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("MAIL_SMTP_PORT", "465"))
    use_tls = os.environ.get("MAIL_USE_TLS", "false").strip().lower() in ("1", "true", "yes")

    if not sender or not password:
        logging.warning("MAIL_USERNAME or MAIL_PASSWORD not set. Printing preview instead.")
        print("\n--- EMAIL CONTENT PREVIEW ---\n")
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:1000]}...\n")
        return

    # Gmail requires the "From" to match the authenticated account unless you've set up 'Send mail as'
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_content, "html"))

    try:
        if use_tls:
            # STARTTLS (e.g., host=smtp.gmail.com, port=587)
            server = smtplib.SMTP(host, port)
            server.ehlo()
            server.starttls()
            server.login(sender, password)
        else:
            # SSL (e.g., host=smtp.gmail.com, port=465)
            server = smtplib.SMTP_SSL(host, port)
            server.login(sender, password)

        server.sendmail(sender, [recipient], msg.as_string())
        server.quit()
        logging.info(f"SUCCESS: Email sent to {recipient}")

    except smtplib.SMTPAuthenticationError as e:
        code = getattr(e, "smtp_code", None)
        msg = getattr(e, "smtp_error", b"").decode(errors="ignore")
        logging.error(
            f"SMTP auth failed ({code}): {msg}. "
            "If using Gmail: enable 2-Step Verification and use an App Password, "
            "make sure MAIL_USERNAME matches the account you created the App Password for, "
            "and paste the 16-char code without spaces."
        )
    except Exception as e:
        logging.error(f"ERROR sending email via SMTP: {e}")



# -----------------------
# Main
# -----------------------
def main():
    logging.info("--- Starting Weekly Digest Generator ---")

    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config.get("weekly_digest", {}).get("enabled", True):
        logging.info("Weekly digest disabled in config.yaml. Exiting.")
        return

    gsheets = authenticate_google_sheets(config)

    manager_emails = config.get("manager_emails", {})
    if not manager_emails:
        logging.warning("No managers defined in config.manager_emails. Exiting.")
        return

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")
        team_records = fetch_manager_data(gsheets, config, manager)

        if not team_records:
            logging.info(f"No data for {manager} this period.")
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
