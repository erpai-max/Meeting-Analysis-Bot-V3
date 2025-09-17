# email_formatter.py
from typing import Dict, List, Any
import html
import io
import base64
import math
import logging

# Try to import matplotlib; if it fails we gracefully degrade to no-chart mode
try:
    import matplotlib.pyplot as plt  # type: ignore
    MATPLOTLIB_AVAILABLE = True
except Exception as e:
    logging.warning(f"matplotlib not available ({e}); charts will be omitted.")
    MATPLOTLIB_AVAILABLE = False


def format_currency(value: Any) -> str:
    """
    Formats a number as Indian Rupees (no decimals). Returns string like "₹12,345".
    Accepts numeric input or strings that can be converted to float.
    """
    try:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return "₹0"
        # allow strings containing commas or currency symbol
        if isinstance(value, str):
            cleaned = value.replace("₹", "").replace(",", "").strip()
            num = float(cleaned) if cleaned != "" else 0.0
        else:
            num = float(value)
        # remove decimals, round to nearest rupee
        num_rounded = math.floor(num + 0.5)
        return f"₹{num_rounded:,}"
    except Exception:
        return "₹0"


def _fig_to_base64(fig) -> str:
    """
    Helper: save matplotlib Figure to PNG base64 string.
    """
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_chart_base64(team_data: List[Dict]) -> str:
    """
    Generates a simple bar chart of avg_score by owner and returns base64 PNG.
    If matplotlib is not available or data is empty, returns empty string.
    """
    if not MATPLOTLIB_AVAILABLE:
        return ""

    owners = [str(m.get("owner", "") or "") for m in team_data]
    scores = []
    for m in team_data:
        try:
            scores.append(float(m.get("avg_score") or 0.0))
        except Exception:
            scores.append(0.0)

    if not owners or not scores:
        return ""

    try:
        fig = plt.figure(figsize=(6, 3.5))
        ax = fig.add_subplot(1, 1, 1)
        ax.bar(owners, scores)
        ax.set_title("Average Score by Owner (Last 7 Days)")
        ax.set_ylabel("Score (%)")
        ax.set_xlabel("Owner")
        ax.set_ylim(0, max(max(scores) * 1.15, 10))
        plt.xticks(rotation=30, ha="right")
        return _fig_to_base64(fig)
    except Exception as e:
        logging.warning(f"Failed to render avg score chart: {e}")
        return ""


def generate_pipeline_chart_base64(team_data: List[Dict]) -> str:
    """
    Generates a pie chart showing pipeline share by owner and returns base64 PNG.
    If matplotlib not available or no pipeline data, returns empty string.
    """
    if not MATPLOTLIB_AVAILABLE:
        return ""

    owners = []
    pipelines = []
    for m in team_data:
        try:
            val = float(m.get("pipeline") or 0.0)
        except Exception:
            val = 0.0
        if val > 0:
            owners.append(str(m.get("owner") or ""))
            pipelines.append(val)

    if not owners or not pipelines:
        return ""

    try:
        fig = plt.figure(figsize=(5, 5))
        ax = fig.add_subplot(1, 1, 1)
        ax.pie(pipelines, labels=owners, autopct="%1.1f%%", startangle=140)
        ax.set_title("Pipeline Value Share")
        return _fig_to_base64(fig)
    except Exception as e:
        logging.warning(f"Failed to render pipeline chart: {e}")
        return ""


def create_manager_digest_email(
    manager_name: str,
    kpis: Dict[str, Any],
    team_data: List[Dict],
    coaching_notes: List[Dict],
    ai_summary: str,
) -> str:
    """
    Generates the HTML email body for the manager digest.
    - Escapes all dynamic content.
    - Embeds charts as base64 if matplotlib is available.
    """

    # Safe values
    safe_manager = html.escape(str(manager_name or ""))
    total_meetings = int(kpis.get("total_meetings") or 0)
    avg_score = float(kpis.get("avg_score") or 0.0)
    pipeline_str = format_currency(kpis.get("total_pipeline") or 0)

    safe_summary = html.escape(str(ai_summary or "")).replace("\n", "<br>")

    # Build team rows
    team_rows_html = ""
    for member in team_data:
        owner = html.escape(str(member.get("owner") or ""))
        meetings = int(member.get("meetings") or 0)
        try:
            avg = float(member.get("avg_score") or 0.0)
        except Exception:
            avg = 0.0
        try:
            pipeline_val = format_currency(member.get("pipeline") or 0)
        except Exception:
            pipeline_val = "₹0"
        score_change = float(member.get("score_change") or 0.0)
        score_color = "#16a34a" if score_change >= 0 else "#dc2626"
        score_icon = "▲" if score_change >= 0 else "▼"

        team_rows_html += f"""
        <tr>
          <td style="padding:12px 16px; font-weight:600; color:#111827;">{owner}</td>
          <td style="padding:12px 16px;">{meetings}</td>
          <td style="padding:12px 16px;">{avg:.1f}% <span style="color:{score_color}; font-size:12px;">({score_icon} {abs(score_change):.1f}%)</span></td>
          <td style="padding:12px 16px;">{pipeline_val}</td>
        </tr>
        """

    # Coaching notes
    coaching_html = ""
    if coaching_notes:
        items_html = ""
        for note in coaching_notes:
            owner = html.escape(str(note.get("owner", "")))
            metric = html.escape(str(note.get("lowest_metric", "")))
            lowest_score = float(note.get("lowest_score") or 0.0)
            items_html += f"<li style='margin-bottom:10px;'><strong>{owner}:</strong> Focus on {metric} (Avg: {lowest_score:.1f}/10)</li>"
        coaching_html = f"""
        <div style="background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:18px; margin-top:20px;">
          <h3 style="margin:0 0 10px 0; font-size:16px;">💡 Coaching Opportunities</h3>
          <ul style="margin:0; padding-left:16px; font-size:14px;">{items_html}</ul>
        </div>
        """

    # Charts
    chart1 = generate_chart_base64(team_data)
    chart2 = generate_pipeline_chart_base64(team_data)

    charts_html = ""
    if chart1:
        charts_html += f"""
        <div style="margin-top:20px; text-align:center;">
          <h3 style="font-size:16px; margin-bottom:8px;">📊 Average Score by Owner</h3>
          <img src="data:image/png;base64,{chart1}" alt="Avg Score Chart" style="max-width:100%; border-radius:8px;"/>
        </div>
        """
    if chart2:
        charts_html += f"""
        <div style="margin-top:20px; text-align:center;">
          <h3 style="font-size:16px; margin-bottom:8px;">💰 Pipeline Value Share</h3>
          <img src="data:image/png;base64,{chart2}" alt="Pipeline Chart" style="max-width:100%; border-radius:8px;"/>
        </div>
        """

    # Put it all together
    html_body = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Weekly Meeting Digest</title>
      </head>
      <body style="font-family:Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; background:#f3f4f6; padding:20px; margin:0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td align="center">
              <table role="presentation" width="700" cellpadding="0" cellspacing="0" style="background:#f9fafb; border-radius:14px; padding:28px; border:1px solid #e6edf3;">
                <tr>
                  <td>
                    <h1 style="font-size:22px; margin:0 0 8px 0; color:#0f172a;">Weekly Meeting Analysis</h1>
                    <p style="margin:0; color:#6b7280;">Strategic summary for <strong>{safe_manager}</strong></p>
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:18px;">
                    <div style="background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:18px;">
                      <div style="display:flex; gap:16px; align-items:center; justify-content:space-between;">
                        <div>
                          <div style="font-size:12px; color:#6b7280;">Total Meetings</div>
                          <div style="font-size:20px; font-weight:700; color:#111827;">{total_meetings}</div>
                        </div>
                        <div>
                          <div style="font-size:12px; color:#6b7280;">Avg Score</div>
                          <div style="font-size:20px; font-weight:700; color:#0b63d6;">{avg_score:.1f}%</div>
                        </div>
                        <div>
                          <div style="font-size:12px; color:#6b7280;">Pipeline Value</div>
                          <div style="font-size:20px; font-weight:700; color:#047857;">{pipeline_str}</div>
                        </div>
                      </div>
                    </div>
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:18px;">
                    <div style="background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:18px;">
                      <h3 style="margin:0 0 8px 0; font-size:16px;">🚀 AI Executive Summary</h3>
                      <p style="margin:0; color:#374151; line-height:1.5;">{safe_summary}</p>
                    </div>
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:18px;">
                    <div style="background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:18px;">
                      <h3 style="margin:0 0 10px 0; font-size:16px;">🏆 Team Performance (Last 7 Days)</h3>
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px; color:#374151;">
                        <thead style="background:#f8fafc;">
                          <tr>
                            <th align="left" style="padding:8px 12px;">Owner</th>
                            <th align="left" style="padding:8px 12px;">Meetings</th>
                            <th align="left" style="padding:8px 12px;">Avg Score (% WoW)</th>
                            <th align="left" style="padding:8px 12px;">Pipeline Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {team_rows_html}
                        </tbody>
                      </table>
                    </div>
                  </td>
                </tr>

                <tr>
                  <td>
                    {coaching_html}
                  </td>
                </tr>

                <tr>
                  <td>
                    {charts_html}
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:20px; text-align:center; color:#9ca3af; font-size:12px;">
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

    return html_body
