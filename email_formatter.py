# email_formatter.py
"""
Email HTML generator for weekly digest.

Produces:
- format_currency(value)
- create_manager_digest_email(manager_name, kpis, team_data, coaching_notes, ai_summary)

Generates charts with matplotlib if available; otherwise omits images gracefully.
"""
from typing import Dict, List, Any
import html
import io
import base64
import logging

# attempt to import matplotlib; if unavailable, we'll skip charts
try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except Exception:
    MATPLOTLIB_AVAILABLE = False
    logging.warning("matplotlib not available — charts will be omitted from emails.")

def format_currency(value: float) -> str:
    """Formats a number as Indian Rupees with no decimals (safe)."""
    try:
        if value is None:
            return "₹0"
        return "₹{:,.0f}".format(float(value))
    except (ValueError, TypeError):
        return "₹0"

def _generate_bar_chart_base64(team_data: List[Dict[str, Any]]) -> str:
    """Generates a bar chart (avg score by owner) and returns base64 PNG or ''."""
    if not MATPLOTLIB_AVAILABLE:
        return ""
    try:
        owners = [m.get("owner", "") for m in team_data]
        scores = [float(m.get("avg_score") or 0) for m in team_data]
        if not owners:
            return ""

        plt.figure(figsize=(6, 3))
        plt.bar(owners, scores)
        plt.title("Average Score by Owner (Last 7 Days)")
        plt.ylabel("Score (%)")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")
    except Exception as e:
        logging.warning(f"Chart generation failed: {e}")
        return ""

def _generate_pipeline_pie_base64(team_data: List[Dict[str, Any]]) -> str:
    """Generates a pie chart of pipeline share and returns base64 PNG or ''."""
    if not MATPLOTLIB_AVAILABLE:
        return ""
    try:
        owners = []
        pipelines = []
        for m in team_data:
            p = float(m.get("pipeline") or 0)
            if p > 0:
                owners.append(m.get("owner", ""))
                pipelines.append(p)
        if not pipelines:
            return ""

        plt.figure(figsize=(5, 5))
        plt.pie(pipelines, labels=owners, autopct="%1.1f%%", startangle=140)
        plt.title("Pipeline Value Share")
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")
    except Exception as e:
        logging.warning(f"Pipeline chart generation failed: {e}")
        return ""

def create_manager_digest_email(
    manager_name: str,
    kpis: Dict[str, Any],
    team_data: List[Dict[str, Any]],
    coaching_notes: List[Dict[str, Any]],
    ai_summary: str,
) -> str:
    """
    Build complete HTML email for manager digest.
    - manager_name: name of manager (string)
    - kpis: dict with keys total_meetings, avg_score, total_pipeline
    - team_data: list of dicts with keys owner, meetings, avg_score, pipeline, score_change
    - coaching_notes: list (unused display if empty)
    - ai_summary: string
    """
    # Safe values
    total_meetings = int(kpis.get("total_meetings") or 0)
    avg_score = float(kpis.get("avg_score") or 0)
    pipeline_display = format_currency(kpis.get("total_pipeline") or 0)

    safe_summary = html.escape(str(ai_summary or "")).replace("\n", "<br>")

    # Team rows HTML
    team_rows_html = []
    for member in team_data:
        owner = html.escape(str(member.get("owner", "")))
        meetings = int(member.get("meetings") or 0)
        avg = float(member.get("avg_score") or 0)
        pipeline_val = format_currency(member.get("pipeline") or 0)
        score_change = float(member.get("score_change") or 0)
        score_color = "#16a34a" if score_change >= 0 else "#dc2626"
        score_icon = "▲" if score_change >= 0 else "▼"
        row = (
            "<tr>"
            f"<td style='padding:12px 16px; font-weight:600; color:#1f2937;'>{owner}</td>"
            f"<td style='padding:12px 16px;'>{meetings}</td>"
            f"<td style='padding:12px 16px;'>{avg:.1f}% "
            f"<span style='color:{score_color}; font-size:12px;'>({score_icon} {abs(score_change):.1f}%)</span></td>"
            f"<td style='padding:12px 16px;'>{pipeline_val}</td>"
            "</tr>"
        )
        team_rows_html.append(row)

    # Coaching notes (if any)
    coaching_html = ""
    if coaching_notes:
        items = []
        for n in coaching_notes:
            owner = html.escape(str(n.get("owner", "")))
            metric = html.escape(str(n.get("lowest_metric", "")))
            lowest_score = float(n.get("lowest_score") or 0)
            items.append(
                "<li style='margin-bottom:12px;'><strong style='color:#111827;'>"
                f"{owner}:</strong> <span style='color:#4b5563;'>Focus on {metric} "
                f"(Avg: {lowest_score:.1f}/10)</span></li>"
            )
        coaching_html = (
            "<div style='background-color:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:24px; margin-top:20px;'>"
            "<h2 style='font-size:18px; font-weight:700; color:#111827;'>💡 Coaching Opportunities</h2>"
            "<ul style='list-style-type:none; padding:0; margin:0; font-size:14px;'>"
            + "".join(items)
            + "</ul></div>"
        )

    # Charts (maybe empty)
    chart1_b64 = _generate_bar_chart_base64(team_data)
    chart2_b64 = _generate_pipeline_pie_base64(team_data)

    chart_html = ""
    if chart1_b64:
        chart_html += (
            "<div style='margin-top:20px; text-align:center;'>"
            "<h2 style='font-size:18px; font-weight:700; color:#111827;'>📊 Average Score by Owner</h2>"
            f"<img src='data:image/png;base64,{chart1_b64}' alt='Avg Score Chart' style='max-width:100%; border-radius:12px;'/>"
            "</div>"
        )
    if chart2_b64:
        chart_html += (
            "<div style='margin-top:20px; text-align:center;'>"
            "<h2 style='font-size:18px; font-weight:700; color:#111827;'>💰 Pipeline Value Share</h2>"
            f"<img src='data:image/png;base64,{chart2_b64}' alt='Pipeline Chart' style='max-width:100%; border-radius:12px;'/>"
            "</div>"
        )

    # Compose final HTML
    html_content = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Weekly Meeting Analysis Summary</title></head>
<body style="font-family: 'Inter', sans-serif; background-color: #f3f4f6; margin:0; padding:20px;">
  <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%">
    <tr><td>
      <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="600" align="center"
             style="margin:0 auto; background-color:#f9fafb; border-radius:16px; padding:32px;">
        <tr><td>
          <h1 style="font-size:24px; font-weight:800; color:#111827; margin:0;">Weekly Meeting Analysis</h1>
          <p style="font-size:16px; color:#6b7280; margin:4px 0 0;">Strategic Summary for {html.escape(manager_name)}</p>
        </td></tr>

        <tr><td style="padding-top:20px;">
          <div>✅ <b>Total Meetings:</b> {total_meetings} |
                       📈 <b>Avg Score:</b> {avg_score:.1f}% |
                       💰 <b>Pipeline:</b> {pipeline_display}</div>
        </td></tr>

        <tr><td style="padding-top:20px;">🚀 <b>AI Summary:</b><br>{safe_summary}</td></tr>

        <tr><td style="padding-top:20px;">🏆 <b>Team Performance:</b>
          <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top:10px; font-size:14px;">
            <thead style="background-color:#f9fafb;">
              <tr>
                <th align="left" style="padding:8px;">Owner</th>
                <th align="left" style="padding:8px;">Meetings</th>
                <th align="left" style="padding:8px;">Avg Score (% WoW)</th>
                <th align="left" style="padding:8px;">Pipeline</th>
              </tr>
            </thead>
            <tbody>
              {"".join(team_rows_html)}
            </tbody>
          </table>
        </td></tr>

        {f"<tr><td>{coaching_html}</td></tr>" if coaching_html else ""}
        {f"<tr><td>{chart_html}</td></tr>" if chart_html else ""}

        <tr><td style="padding-top:32px; text-align:center; font-size:12px; color:#9ca3af;">
          This is an automated report generated by the Meeting Analysis Bot.
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>
"""
    return html_content
