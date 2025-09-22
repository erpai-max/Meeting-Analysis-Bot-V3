# export_dashboard.py
import os
import json
import yaml
import logging
import shutil

import sheets  # uses your existing sheets.py

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    # Load config
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Authenticate Google Sheets (re-uses your sheets.py helper)
    gs = sheets.authenticate_google_sheets(config)

    # Grab all rows from the Results tab
    rows = sheets.get_all_results(gs, config)

    # Where to write
    dash_cfg = config.get("dashboard", {}) or {}
    out_dir = dash_cfg.get("output_dir", "docs")
    out_name = dash_cfg.get("filename", "dashboard_data.json")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, out_name)

    # (Optional) strip sensitive fields before publishing
    strip_cols = set(dash_cfg.get("strip_columns", []))
    if strip_cols:
        def clean_row(r):
            return {k: v for k, v in r.items() if k not in strip_cols}
        rows = [clean_row(r) for r in rows]

    # Write pretty JSON for the dashboard
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    logging.info(f"Exported {len(rows)} records → {out_path}")

    # (Optional) copy dashboard.html at repo root → docs/index.html
    if dash_cfg.get("copy_html_from_root", True):
        src_html = "dashboard.html"
        dest_html = os.path.join(out_dir, "index.html")
        if os.path.exists(src_html):
            shutil.copyfile(src_html, dest_html)
            logging.info(f"Copied {src_html} → {dest_html}")
        else:
            logging.info("dashboard.html not found at repo root; skipping HTML copy.")

if __name__ == "__main__":
    # Requires env var: GCP_SA_KEY (service account JSON content)
    if not os.environ.get("GCP_SA_KEY"):
        raise SystemExit("GCP_SA_KEY env var missing. Set it to your service account JSON string.")
    main()
