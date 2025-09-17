from typing import Dict, List, Any
import html

def format_currency(value: float) -> str:
    try:
        if value is None:
            return "₹0"
        return f"₹{float(value):,.0f}"
    except (ValueError, TypeError):
        return "₹0"

def create_manager_digest_email(
    manager_name: str,
    kpis: Dict[str, Any],
    team_data: List[Dict],
    coaching_notes: List[Dict],
    ai_summary: str
) -> str:
    kpi_total_meetings = kpis.get("total_meetings", 0)
    kpi_avg_score = float(kpis.get("avg_score") or 0)
    kpi_pipeline = format_currency(kpis.get("total_pipeline", 0))
    safe_summary = html.escape(str(ai_summary or "")).replace("\n", "<br>")

    team_rows = []
    for member in team_data:
        score_change = float(member.get("score_change") or 0)
        score_color = "#16a34a" if score_change >= 0 else "#dc2626"
        score_icon = "▲" if score_change >= 0 else "▼"
        row = f"""
            <tr>
                <td style="padding: 10px 12px; font-weight: 600;">{html.escape(str(member.get('owner', '')))}</td>
                <td style="padding: 10px 12px;">{int(member.get('meetings') or 0)}</td>
                <td style="padding: 10px 12px;">
                    {float(member.get('avg_score') or 0):.1f}% 
                    <span style="color: {score_color}; font-size: 12px;">({score_icon} {abs(score_change):.1f}%)</span>
                </td>
                <td style="padding: 10px 12px;">{format_currency(member.get('pipeline'))}</td>
            </tr>
        """
        team_rows.append(row)

    coaching_notes_html = ""
    if coaching_notes:
        items = []
        for note in coaching_notes:
            items.append(f"""
                <li style="margin-bottom: 10px;">
                    <strong>{html.escape(str(note.get('owner', '')))}:</strong>
                    <span>Focus on {html.escape(str(note.get('lowest_metric', '')))} 
                    (Avg: {float(note.get('lowest_score') or 0):.1f}/10)</span>
                </li>
            """)
        coaching_notes_html = f"""
            <div style="background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:20px; margin-top:20px;">
                <h2 style="font-size:18px; margin:0 0 10px;">💡 Coaching Opportunities</h2>
                <ul style="list-style:none; padding:0; margin:0;">{"".join(items)}</ul>
            </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"><title>Weekly Meeting Analysis Summary</title></head>
    <body style="font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, 'Helvetica Neue', Arial; background:#f3f4f6; margin:0; padding:20px;">
        <table role="presentation" width="100%">
            <tr><td>
                <table role="presentation" width="600" align="center" style="margin:0 auto; background:#fff; border-radius:14px; padding:24px; border:1px solid #e5e7eb;">
                    <tr><td>
                        <h1 style="font-size:22px; margin:0;">Weekly Meeting Analysis</h1>
                        <p style="color:#6b7280; margin:6px 0 0;">Strategic Summary for {html.escape(manager_name)}</p>
                    </td></tr>

                    <tr><td style="padding-top:16px;">
                        <div>✅ <b>Total Meetings:</b> {kpi_total_meetings} &nbsp; | &nbsp;
                        📈 <b>Avg Score:</b> {kpi_avg_score:.1f}% &nbsp; | &nbsp;
                        💰 <b>Pipeline:</b> {kpi_pipeline}</div>
                    </td></tr>

                    <tr><td style="padding-top:16px;">
                        <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px;">
                            <b>🚀 AI Executive Summary</b>
                            <div style="margin-top:6px; line-height:1.5;">{safe_summary}</div>
                        </div>
                    </td></tr>

                    <tr><td style="padding-top:16px;">
                        <b>🏆 Team Performance (Last 7 Days)</b>
                        <table width="100%" style="margin-top:8px; font-size:14px; border-collapse:collapse;">
                            <thead>
                                <tr style="background:#f9fafb;">
                                    <th align="left" style="padding:8px 12px; border-bottom:1px solid #e5e7eb;">Owner</th>
                                    <th align="left" style="padding:8px 12px; border-bottom:1px solid #e5e7eb;">Meetings</th>
                                    <th align="left" style="padding:8px 12px; border-bottom:1px solid #e5e7eb;">Avg Score (% WoW)</th>
                                    <th align="left" style="padding:8px 12px; border-bottom:1px solid #e5e7eb;">Pipeline</th>
                                </tr>
                            </thead>
                            <tbody>{"".join(team_rows)}</tbody>
                        </table>
                    </td></tr>

                    {f"<tr><td>{coaching_notes_html}</td></tr>" if coaching_notes_html else ""}

                    <tr><td style="padding-top:18px; text-align:center; color:#9ca3af; font-size:12px;">
                        Automated report generated by Meeting Analysis Bot.
                    </td></tr>
                </table>
            </td></tr>
        </table>
    </body>
    </html>
    """
