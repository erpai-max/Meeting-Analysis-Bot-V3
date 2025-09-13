from typing import Dict, List, Any
import html

def format_currency(value: float) -> str:
    """Formats a number as Indian Rupees (no decimals)."""
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
    """
    Generates a complete, attractive HTML email report for a specific manager.
    """

    # --- KPI Cards ---
    kpi_cards_html = f"""
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr>
                <td style="padding: 10px 0;">
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%">
                        <tr>
                            <!-- Total Meetings -->
                            <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; 
                                            border: 1px solid #e5e7eb; padding: 20px; 
                                            text-align: center; width: 100%;">
                                    <p style="font-size: 14px; font-weight: 500; 
                                              color: #6b7280; margin: 0;">Total Meetings</p>
                                    <p style="font-size: 30px; font-weight: 700; 
                                              color: #4f46e5; margin: 4px 0 0;">
                                        {kpis.get('total_meetings', 0)}
                                    </p>
                                </div>
                            </td>
                            <!-- Avg Score -->
                            <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; 
                                            border: 1px solid #e5e7eb; padding: 20px; 
                                            text-align: center; width: 100%;">
                                    <p style="font-size: 14px; font-weight: 500; 
                                              color: #6b7280; margin: 0;">Avg Score</p>
                                    <p style="font-size: 30px; font-weight: 700; 
                                              color: #1d4ed8; margin: 4px 0 0;">
                                        {float(kpis.get('avg_score') or 0):.1f}%
                                    </p>
                                </div>
                            </td>
                            <!-- Pipeline Value -->
                            <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; 
                                            border: 1px solid #e5e7eb; padding: 20px; 
                                            text-align: center; width: 100%;">
                                    <p style="font-size: 14px; font-weight: 500; 
                                              color: #6b7280; margin: 0;">Pipeline Value</p>
                                    <p style="font-size: 30px; font-weight: 700; 
                                              color: #059669; margin: 4px 0 0;">
                                        {format_currency(kpis.get('total_pipeline', 0))}
                                    </p>
                                </div>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    """

    # --- AI Executive Summary ---
    ai_summary_html = f"""
        <div style="background-color: #ffffff; border: 1px solid #e5e7eb; 
                    border-radius: 12px; padding: 24px; margin-top: 20px;">
            <h2 style="font-size: 18px; font-weight: 700; color: #111827; 
                       margin: 0 0 16px;">🚀 AI Executive Summary</h2>
            <p style="font-size: 16px; color: #374151; line-height: 1.6;">
                {html.escape(str(ai_summary or "")).replace("\n", "<br>")}
            </p>
        </div>
    """

    # --- Team Performance Table ---
    team_rows_html = ""
    for member in team_data:
        score_change = float(member.get("score_change") or 0)
        score_color = "#16a34a" if score_change >= 0 else "#dc2626"
        score_icon = "▲" if score_change >= 0 else "▼"

        team_rows_html += f"""
            <tr>
                <td style="padding: 12px 16px; font-weight: 600; color: #1f2937;">
                    {html.escape(str(member.get('owner', '')))}
                </td>
                <td style="padding: 12px 16px;">{int(member.get('meetings') or 0)}</td>
                <td style="padding: 12px 16px;">
                    {float(member.get('avg_score') or 0):.1f}% 
                    <span style="color: {score_color}; font-size: 12px;">
                        ({score_icon} {abs(score_change):.1f}%)
                    </span>
                </td>
                <td style="padding: 12px 16px;">{format_currency(member.get('pipeline'))}</td>
            </tr>
        """

    team_table_html = f"""
        <div style="background-color: #ffffff; border: 1px solid #e5e7eb; 
                    border-radius: 12px; padding: 24px; margin-top: 20px;">
            <h2 style="font-size: 18px; font-weight: 700; color: #111827; 
                       margin: 0 0 16px;">🏆 Team Performance (Last 7 Days)</h2>
            <table role="presentation" border="0" cellpadding="0" cellspacing="0" 
                   width="100%" style="font-size: 14px; color: #374151;">
                <thead style="background-color: #f9fafb;">
                    <tr>
                        <th style="padding: 12px 16px; text-align: left;">Owner</th>
                        <th style="padding: 12px 16px; text-align: left;">Meetings</th>
                        <th style="padding: 12px 16px; text-align: left;">Avg Score (% WoW)</th>
                        <th style="padding: 12px 16px; text-align: left;">Pipeline Value</th>
                    </tr>
                </thead>
                <tbody style="border-top: 1px solid #e5e7eb;">
                    {team_rows_html}
                </tbody>
            </table>
        </div>
    """

    # --- Coaching Opportunities ---
    coaching_notes_html = ""
    if coaching_notes:
        items = ""
        for note in coaching_notes:
            items += f"""
                <li style="margin-bottom: 12px;">
                    <strong style="color: #111827;">
                        {html.escape(str(note.get('owner', '')))}:
                    </strong>
                    <span style="color: #4b5563;">
                        Focus on {html.escape(str(note.get('lowest_metric', '')))} 
                        (Avg: {float(note.get('lowest_score') or 0):.1f}/10)
                    </span>
                </li>
            """
        coaching_notes_html = f"""
            <div style="background-color: #ffffff; border: 1px solid #e5e7eb; 
                        border-radius: 12px; padding: 24px; margin-top: 20px;">
                <h2 style="font-size: 18px; font-weight: 700; color: #111827; 
                           margin: 0 0 16px;">💡 Coaching Opportunities</h2>
                <ul style="list-style-type: none; padding: 0; margin: 0; font-size: 14px;">
                    {items}
                </ul>
            </div>
        """

    # --- Final HTML Assembly ---
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Weekly Meeting Analysis Summary</title>
    </head>
    <body style="font-family: 'Inter', sans-serif; background-color: #f3f4f6; 
                 margin: 0; padding: 20px;">
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" 
               width="100%">
            <tr>
                <td>
                    <table role="presentation" border="0" cellpadding="0" 
                           cellspacing="0" width="600" align="center" 
                           style="margin: 0 auto; background-color: #f9fafb; 
                                  border-radius: 16px; padding: 32px;">
                        <tr>
                            <td>
                                <h1 style="font-size: 24px; font-weight: 800; 
                                           color: #111827; margin: 0;">
                                    Weekly Meeting Analysis
                                </h1>
                                <p style="font-size: 16px; color: #6b7280; margin: 4px 0 0;">
                                    Strategic Summary for {html.escape(manager_name)}
                                </p>
                            </td>
                        </tr>
                        <tr><td>{kpi_cards_html}</td></tr>
                        <tr><td>{ai_summary_html}</td></tr>
                        <tr><td>{team_table_html}</td></tr>
                        {f"<tr><td>{coaching_notes_html}</td></tr>" if coaching_notes_html else ""}
                        <tr>
                            <td style="padding-top: 32px; text-align: center; 
                                       font-size: 12px; color: #9ca3af;">
                                This is an automated report generated by the Meeting Analysis Bot.
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
