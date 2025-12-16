# digest_generator.py — Updated (Dec 2025)
# Weekly Digest Generator for Meeting Analysis Bot (Gemini optional)

import os
import re
import yaml
import json
import time
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta

import gspread
from google.oauth2 import service_account

# Try to import Gemini; runs fine without it
try:
    import google.generativeai as genai
except Exception:
    genai = None

# local modules
import sheets
import email_formatter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

# -----------------------
# Google Sheets Auth
# -----------------------
def authenticate_google_sheets(config: Dict):
    """Authenticate with Google Sheets and return an opened Spreadsheet handle."""
    gcp_key_str = os.environ.get("GCP_SA_KEY")
    if not gcp_key_str:
        raise ValueError("Missing GCP_SA_KEY environment variable")

    creds_info = json.loads(gcp_key_str)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)

    client = gspread.authorize(creds)
    sheet_id = config["google_sheets"]["sheet_id"]
    logging.info("SUCCESS: Authenticated Google Sheets")
    return client.open_by_key(sheet_id)

# -----------------------
# Parsing helpers
# -----------------------
_DATE_FORMATS = [
    "%d/%m/%y",    # 13/12/25
    "%d/%m/%Y",    # 13/12/2025
    "%Y-%m-%d",    # 2025-12-13
    "%d-%m-%Y",    # 13-12-2025
    "%m/%d/%Y",    # 12/13/2025
    "%Y/%m/%d",    # 2025/12/13
    "%b %d, %Y",   # Dec 13, 2025
    "%d %b %Y",    # 13 Dec 2025

    # Date+time shapes (common from sheets / manual entry)
    "%d/%m/%y %H:%M",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y, %H:%M",
    "%d/%m/%y, %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d-%m-%Y %H:%M",
]

def _normalize_text(s: Any) -> str:
    """Lowercase + trim + collapse whitespace + remove most punctuation for matching."""
    x = str(s or "").strip().lower()
    x = re.sub(r"\s+", " ", x)
    x = re.sub(r"[^\w\s]", "", x)  # remove punctuation
    x = x.strip()
    return x

def _parse_date(d: Any) -> Optional[datetime]:
    """
    Parse many common date shapes.
    - Handles ISO with T/Z
    - Handles date strings that contain time (splits / tries full parse)
    Returns naive datetime or None.
    """
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.replace(tzinfo=None)

    s = str(d).strip()
    if not s or s.upper() in ("N/A", "NA", "NONE"):
        return None

    # If it looks like ISO timestamp with T
    if "T" in s:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass

    # Try direct formats
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue

    # If there is extra content (like "13/12/25 11:20 AM" or "13/12/25 - something")
    # try to extract the first plausible date-ish token substring and parse again.
    # Common: take first 10 chars, or first chunk until space/comma
    candidates = []
    # split on newline
    s1 = s.splitlines()[0].strip()
    candidates.append(s1)

    # take substring before comma
    if "," in s1:
        candidates.append(s1.split(",")[0].strip())

    # take substring before ' - '
    if " - " in s1:
        candidates.append(s1.split(" - ")[0].strip())

    # take first token + second token (date + time)
    parts = s1.split()
    if len(parts) >= 2:
        candidates.append(parts[0].strip())
        candidates.append((parts[0] + " " + parts[1]).strip())
    elif len(parts) == 1:
        candidates.append(parts[0].strip())

    # take first 10 / 8 chars (dd/mm/yy or dd/mm/yyyy)
    candidates.append(s1[:10].strip())
    candidates.append(s1[:8].strip())

    seen = set()
    for c in candidates:
        c = c.strip()
        if not c or c in seen:
            continue
        seen.add(c)
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(c, fmt)
            except Exception:
                continue

    return None

def _to_float_percent(val: Any) -> Optional[float]:
    """Extract a number from '83.3%' / '83' / 'N/A'. Return float percentage (0-100) or None."""
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
    Convert '₹53,000', '53,000', '53000.00' into numeric value.
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
def fetch_manager_data(spreadsheet, config: Dict, manager_name: str, last_n_days: int) -> List[Dict]:
    """
    Fetch rows from Results tab for the given manager within last_n_days.
    Uses config.google_sheets.results_tab_name (your config: "Analysis Results").
    """
    tab = config.get("google_sheets", {}).get("results_tab_name", "Analysis Results")

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
    cutoff = now - timedelta(days=last_n_days)

    target_mgr = _normalize_text(manager_name)
    picked: List[Dict] = []
    skipped_bad_dates = 0
    matched_manager_count = 0

    # For debug
    unique_mgrs = set()
    dates_seen = []

    for r in rows:
        mgr_raw = _clean_name(r.get("Manager", ""))  # column must be "Manager"
        mgr_norm = _normalize_text(mgr_raw)
        if mgr_norm:
            unique_mgrs.add(mgr_raw.strip())

        # Manager match (normalized)
        if mgr_norm != target_mgr:
            continue

        matched_manager_count += 1

        dval = r.get("Date")
        d = _parse_date(dval)
        if not d:
            skipped_bad_dates += 1
            continue

        dates_seen.append(d)

        # Keep if within lookback window (tolerate slight future entries)
        if cutoff <= d <= (now + timedelta(days=1)):
            picked.append(r)

    if not picked:
        logging.info(f"Found 0 records for {manager_name}.")
        logging.info(f"DEBUG: Rows matching Manager='{manager_name}' before date filter: {matched_manager_count}")
        logging.info(f"DEBUG: Rows skipped due to bad/NA dates: {skipped_bad_dates}")

        # show manager values available (helps detect spelling mismatch)
        if unique_mgrs:
            sample = sorted(unique_mgrs)[:30]
            logging.info(f"DEBUG: Sample manager values in sheet (first 30): {sample}")

        # show date range for matched manager rows
        if dates_seen:
            logging.info(f"DEBUG: Date range (matched manager rows): {min(dates_seen).date()} → {max(dates_seen).date()}")
        else:
            logging.info("DEBUG: No parseable dates found for matched manager rows (check Date format / column).")

    else:
        if skipped_bad_dates:
            logging.info(f"Found {len(picked)} records for {manager_name}. (Skipped {skipped_bad_dates} bad/NA dates)")
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

    prev_week_by_owner: Dict[str, List[float]] = {}

    for r in team_records:
        s = _to_float_percent(r.get("% Score"))
        if s is not None:
            scores.append(s)

        pipeline_total += _to_float_amount_inr(r.get("Amount Value"))

        d = _parse_date(r.get("Date"))
        if d and (w2_start <= d < w2_end):
            owner = _safe_owner(r)
            prev_s = _to_float_percent(r.get("% Score"))
            if prev_s is not None:
                prev_week_by_owner.setdefault(owner, []).append(prev_s)

    avg_score = (sum(scores) / len(scores)) if scores else 0.0

    team_performance: List[Dict] = []
    owners = sorted({_safe_owner(r) for r in team_records})
    for owner in owners:
        rep_meetings = [r for r in team_records if _safe_owner(r) == owner]

        rep_scores = []
        for rr in rep_meetings:
            ps = _to_float_percent(rr.get("% Score"))
            if ps is not None:
                rep_scores.append(ps)

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

    coaching_notes: List[Dict] = []
    for rep in team_performance:
        if rep["avg_score"] < 70:
            coaching_notes.append({
                "owner": rep["owner"],
                "lowest_metric": "Overall % Score",
                "lowest_score": round(rep["avg_score"], 1),
                "note": "Focus on tighter discovery, sharper ERP/ASP mapping, and stronger objection handling."
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
# AI Summary (Gemini; optional)
# -----------------------
def _generate_ai_summary(manager_name: str, kpis: Dict, team_data: List[Dict], config: Dict) -> str:
    """Use Gemini to write a short executive summary (2–3 sentences)."""
    if genai is None or not os.environ.get("GEMINI_API_KEY"):
        return "Summary not available this week."

    model_name = (config.get("google_llm") or {}).get("model", "gemini-2.5-flash")

    prompt = f"""
You are a senior sales analyst writing a 2–3 sentence executive summary for {manager_name}.
Base your summary ONLY on the data below. Mention:
1) one key trend,
2) one bright spot,
3) one improvement area.

KPIs:
- Total Meetings: {kpis['total_meetings']}
- Team Avg Score: {kpis['avg_score']:.1f}%
- Pipeline Value: {email_formatter.format_currency(kpis['total_pipeline'])}

Team Performance (owner, avg_score, meetings, pipeline, WoW change):
{json.dumps([
    {"owner": t["owner"], "avg_score": round(t["avg_score"], 1), "meetings": t["meetings"],
     "pipeline": t["pipeline"], "score_change": round(t["score_change"], 1)}
    for t in team_data
], indent=2)}

Return ONLY the summary text, nothing else.
"""

    try:
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(prompt)
        text = getattr(resp, "text", "") or ""
        return text.strip() if text.strip() else "Summary not available this week."
    except Exception as e:
        logging.error(f"Gemini summary failed: {e}")
        return "Summary not available this week."

# -----------------------
# Email sending (robust)
# -----------------------
def send_email(subject: str, html_content: str, recipient: str):
    """Sends digest email via SMTP (SSL 465 default or STARTTLS 587 with MAIL_USE_TLS=true)."""
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
        print(f"TO: {recipient}\nSUBJECT: {subject}\n{html_content[:1200]}...\n")
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
            "Tip: For Gmail, enable 2-Step Verification and use an App Password."
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

    wd = config.get("weekly_digest", {})
    last_n_days = int(wd.get("last_n_days", wd.get("lookback_days", 7)))

    for manager, email in manager_emails.items():
        logging.info(f"--- Generating digest for {manager} ---")

        records = fetch_manager_data(spreadsheet, config, manager, last_n_days)
        if not records:
            logging.info(f"No data for {manager} in the last {last_n_days} day(s).")
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
