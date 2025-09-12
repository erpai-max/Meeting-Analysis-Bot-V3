from typing import Dict, List, Any

def format_currency(value: float) -> str:
    """Formats a number as Indian Rupees."""
    if value is None:
        return "₹0"
    return f"₹{value:,.0f}"

def create_manager_digest_email(manager_name: str, kpis: Dict[str, Any], team_data: List[Dict], coaching_notes: List[Dict], ai_summary: str) -> str:
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
                            <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; border: 1px solid #e5e7eb; padding: 20px; text-align: center; width: 100%; box-sizing: border-box;">
                                    <p style="font-size: 14px; font-weight: 500; color: #6b7280; margin: 0;">Total Meetings</p>
                                    <p style="font-size: 30px; font-weight: 700; color: #4f46e5; margin: 4px 0 0;">{kpis.get('total_meetings', 0)}</p>
                                </div>
                            </td>
                            <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; border: 1px solid #e5e7eb; padding: 20px; text-align: center; width: 100%; box-sizing: border-box;">
                                    <p style="font-size: 14px; font-weight: 500; color: #6b7280; margin: 0;">Avg Score</p>
                                    <p style="font-size: 30px; font-weight: 700; color: #1d4ed8; margin: 4px 0 0;">{kpis.get('avg_score', 0):.1f}%</p>
                                </div>
                            </td>
                             <td align="center" style="padding: 0 5px;">
                                <div style="border-radius: 12px; background-color: #ffffff; border: 1px solid #e5e7eb; padding: 20px; text-align: center; width: 100%; box-sizing: border-box;">
                                    <p style="font-size: 14px; font-weight: 500; color: #6b7280; margin: 0;">Pipeline Value</p>
                                    <p style="font-size: 30px; font-weight: 700; color: #059669; margin: 4px 0 0;">{format_currency(kpis.get('total_pipeline', 0))}</p>
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
        <div style="background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px; margin-top: 20px;">
            <h2 style="font-size: 18px; font-weight: 700; color: #111827; margin: 0 0 16px;">🚀 AI Executive Summary</h2>
            <p style="font-size: 16px; color: #374151; line-height: 1.6;">{ai_summary.replace('\n', '<br>')}</p>
        </div>
    """

    # --- Team Performance Table ---
    team_rows_html = ""
    for team_member in team_data:
        score_color = "#16a34a" if team_member.get('score_change', 0) >= 0 else "#dc2626"
        score_icon = "▲" if team_member.get('score_change', 0) >= 0 else "▼"
        team_rows_html += f"""
            <tr>
                <td style="padding: 12px 16px; font-weight: 600; color: #1f2937;">{team_member['owner']}</td>
                <td style="padding: 12px 16px;">{team_member['meetings']}</td>
                <td style="padding: 12px 16px;">
                    {team_member['avg_score']:.1f}% 
                    <span style="color: {score_color}; font-size: 12px;">({score_icon} {abs(team_member.get('score_change', 0)):.1f}%)</span>
                </td>
                <td style="padding: 12px 16px;">{format_currency(team_member['pipeline'])}</td>
            </tr>
        """
    team_table_html = f"""
        <div style="background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px; margin-top: 20px;">
            <h2 style="font-size: 18px; font-weight: 700; color: #111827; margin: 0 0 16px;">🏆 Team Performance (Last 7 Days)</h2>
            <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="font-size: 14px; color: #374151;">
                <thead style="background-color: #f9fafb;">
                    <tr>
                        <th style="padding: 12px 16px; text-align: left; font-weight: 600;">Owner</th>
                        <th style="padding: 12px 16px; text-align: left; font-weight: 600;">Meetings</th>
                        <th style="padding: 12px 16px; text-align: left; font-weight: 600;">Avg Score (% Change WoW)</th>
                        <th style="padding: 12px 16px; text-align: left; font-weight: 600;">Pipeline Value</th>
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
        coaching_items = ""
        for note in coaching_notes:
            coaching_items += f"""
                <li style="margin-bottom: 12px;">
                    <strong style="color: #111827;">{note['owner']}:</strong>
                    <span style="color: #4b5563;">Focus on {note['lowest_metric']} (Avg: {note['lowest_score']:.1f}/10)</span>
                </li>
            """
        coaching_notes_html = f"""
            <div style="background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px; margin-top: 20px;">
                <h2 style="font-size: 18px; font-weight: 700; color: #111827; margin: 0 0 16px;">💡 Coaching Opportunities</h2>
                <ul style="list-style-type: none; padding: 0; margin: 0; font-size: 14px;">
                    {coaching_items}
                </ul>
            </div>
        """

    # --- Final HTML Assembly ---
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Weekly Meeting Analysis Summary</title>
    </head>
    <body style="font-family: Inter, sans-serif; background-color: #f3f4f6; margin: 0; padding: 20px;">
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr>
                <td>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="600" align="center" style="margin: 0 auto; background-color: #f9fafb; border-radius: 16px; padding: 32px;">
                        <!-- Header -->
                        <tr>
                            <td>
                                <h1 style="font-size: 24px; font-weight: 800; color: #111827; margin: 0;">Weekly Meeting Analysis</h1>
                                <p style="font-size: 16px; color: #6b7280; margin: 4px 0 0;">Strategic Summary for {manager_name}</p>
                            </td>
                        </tr>
                        <!-- KPIs -->
                        <tr>
                            <td>{kpi_cards_html}</td>
                        </tr>
                        <!-- AI Summary -->
                        <tr>
                            <td>{ai_summary_html}</td>
                        </tr>
                        <!-- Team Table -->
                        <tr>
                            <td>{team_table_html}</td>
                        </tr>
                        <!-- Coaching Section -->
                        { '<tr><td>' + coaching_notes_html + '</td></tr>' if coaching_notes_html else '' }
                        <!-- Footer -->
                        <tr>
                            <td style="padding: 32px 0 0; text-align: center; font-size: 12px; color: #9ca3af;">
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

