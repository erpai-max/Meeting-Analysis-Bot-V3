from typing import Dict, List, Any
import html
import matplotlib.pyplot as plt
import io
import base64

def format_currency(value: float) -> str:
    """Formats a number as Indian Rupees (no decimals)."""
    try:
        if value is None:
            return "₹0"
        return f"₹{float(value):,.0f}"
    except (ValueError, TypeError):
        return "₹0"

def generate_chart_base64(team_data: List[Dict]) -> str:
    """Generates a bar chart of Avg Score by Owner and returns as base64 image."""
    owners = [m.get("owner", "") for m in team_data]
    scores = [float(m.get("avg_score") or 0) for m in team_data]

    if not owners:
        return ""

    plt.figure(figsize=(6, 3))
    plt.bar(owners, scores, color="#3b82f6", alpha=0.8)
    plt.title("Average Score by Owner (Last 7 Days)")
    plt.ylabel("Score (%)")
    plt.xticks(rotation=30, ha="right")

    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", transparent=True)
    plt.close()
    buf.seek(0)

    return base64.b64encode(buf.read()).decode("utf-8")

def generate_pipeline_chart_base64(team_data: List[Dict]) -> str:
    """Generates a pie chart of Pipeline Value share by Owner and returns as base64 image."""
    owners = [m.get("owner", "") for m in team_data if float(m.get("pipeline") or 0) > 0]
    pipelines = [float(m.get("pipeline") or 0) for m in team_data if float(m.get("pipeline") or 0) > 0]

    if not owners or not pipelines:
        return ""

    plt.figure(figsize=(5, 5))
    plt.pie(pipelines, labels=owners, autopct="%1.1f%%", startangle=140)
    plt.title("Pipeline Value Share")

    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", transparent=True)
    plt.close()
    buf.seek(0)

    return base64.b64encode(buf.read()).decode("utf-8")

def create_manager_digest_email(
    manager_name: str,
    kpis: Dict[str, Any],
    team_data: List[Dict],
    coaching_notes: List[Dict],
    ai_summary: str
) -> str:
    """Generates a complete, attractive HTML email report for a specific manager."""

    # --- KPI Values ---
    kpi_total_meetings = kpis.get("total_meetings", 0)
    kpi_avg_score = float(kpis.get("avg_score") or 0)
    kpi_pipeline = format_currency(kpis.get("total_pipeline", 0))

    # --- AI Executive Summary ---
    safe_summary = html.escape(str(ai_summary or "")).replace("\n", "<br>")

    # --- Team Table ---
    team_rows = []
    for member in team_data:
        score_change = float(member.get("score_change") or 0)
        score_color = "#16a34a" if score_change >= 0 else "#dc2626"
        score_icon = "▲" if score_change >= 0 else "▼"
        row = f"""
            <tr>
                <td style="padding: 12px 16px; font-weight: 600; color: #1f2937;">{html.escape(str(member.get('owner', '')))}</td>
                <td style="padding: 12px 16px;">{int(member.get('meetings') or 0)}</td>
                <td style="padding: 12px 16px;">
                    {float(member.get('avg_score') or 0):.1f}% 
                    <span style="color: {score_color}; font-size: 12px;">
                        ({score_icon} {abs(score_change):.1f}% WoW)
                    </span>
                </td>
                <td style="padding: 12px 16px;">{format_currency(member.get('pipeline'))}</td>
            </tr>
        """
        team_rows.append(row)

    # --- Coaching Notes ---
    coaching_notes_html = ""
    if coaching_notes:
        items = []
        for note in coaching_notes:
            items.append(f"""
                <li style="margin-bottom: 12px;">
                    <strong style="color: #111827;">{html.escape(str(note.get('owner', '')))}:</strong>
                    <span style="color: #4b5563;">Focus on {html.escape(str(note.get('lowest_metric', '')))} 
                    (Avg: {float(note.get('lowest_score') or 0):.1f}/10)</span>
                </li>
            """)
        coaching_notes_html = f"""
            <div style="background-color: #ffffff; border: 1px solid #e5e7eb; 
                        border-radius: 12px; padding: 24px; margin-top: 20px;">
                <h2 style="font-size: 18px; font-weight: 700; color: #111827;">💡 Coaching Opportunities</h2>
                <ul style="list-style-type: none; padding: 0; margin: 0; font-size: 14px;">{"".join(items)}</ul>
            </div>
        """
    else:
        coaching_notes_html = """
            <div style="background-color: #f9fafb; border-radius: 12px; padding: 16px; margin-top: 20px; text-align:center;">
                <span style="font-size: 14px; color: #4b5563;">✅ No coaching notes this week</span>
            </div>
        """

    # --- Charts ---
    chart1 = generate_chart_base64(team_data)
    chart2 = generate_pipeline_chart_base64(team_data)

    chart_html = ""
    if chart1:
        chart_html += f"""
            <div style="margin-top:20px; text-align:center;">
                <h2 style="font-size:18px; font-weight:700; color:#111827;">📊 Average Score by Owner</h2>
                <img src="data:image/png;base64,{chart1}" alt="Avg Score Chart" style="max-width:100%; border-radius:12px;"/>
            </div>
        """
    if chart2:
        chart_html += f"""
            <div style="margin-top:20px; text-align:center;">
                <h2 style="font-size:18px; font-weight:700; color:#111827;">💰 Pipeline Value Share</h2>
                <img src="data:image/png;base64,{chart2}" alt="Pipeline Chart" style="max-width:100%; border-radius:12px;"/>
            </div>
        """

    # --- Final HTML ---
    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"><title>Weekly Meeting Analysis Summary</title></head>
    <body style="font-family: 'Inter', sans-serif; background-color: #f3f4f6; margin: 0; padding: 20px;">
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr><td>
                <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="600" align="center"
                       style="margin:0 auto; background-color:#ffffff; border-radius:16px; padding:32px; box-shadow:0 2px 6px rgba(0,0,0,0.05);">
                    <tr><td>
                        <h1 style="font-size:24px; font-weight:800; color:#111827; margin:0;">Weekly Meeting Analysis</h1>
                        <p style="font-size:16px; color:#6b7280; margin:4px 0 0;">Strategic Summary for {html.escape(manager_name)}</p>
                    </td></tr>
                    <tr><td>
                        <div style="margin-top:20px;">✅ <b>Total Meetings:</b> {kpi_total_meetings} | 
                        📈 <b>Avg Score:</b> {kpi_avg_score:.1f}% | 
                        💰 <b>Pipeline:</b> {kpi_pipeline}</div>
                    </td></tr>
                    <tr><td style="padding-top:20px;">🚀 <b>AI Summary:</b><br>{safe_summary}</td></tr>
                    <tr><td style="padding-top:20px;">🏆 <b>Team Performance:</b>
                        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="margin-top:10px; font-size:14px; border-collapse:collapse;">
                            <thead style="background-color:#f3f4f6;">
                                <tr>
                                    <th align="left" style="padding:8px; font-weight:600;">Owner</th>
                                    <th align="left" style="padding:8px; font-weight:600;">Meetings</th>
                                    <th align="left" style="padding:8px; font-weight:600;">Avg Score (% WoW)</th>
                                    <th align="left" style="padding:8px; font-weight:600;">Pipeline</th>
                                </tr>
                            </thead>
                            <tbody>{"".join(team_rows)}</tbody>
                        </table>
                    </td></tr>
                    <tr><td>{coaching_notes_html}</td></tr>
                    <tr><td>{chart_html}</td></tr>
                    <tr><td style="padding-top:32px; text-align:center; font-size:12px; color:#9ca3af;">
                        This is an automated report generated by the Meeting Analysis Bot.
                    </td></tr>
                </table>
            </td></tr>
        </table>
    </body>
    </html>
    """
