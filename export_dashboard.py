# export_dashboard.py
import os
import json
import yaml
import logging
import shutil

# This script relies on your custom 'sheets.py' module to handle Google Sheets communication.
import sheets

# Configure logging for clear output during GitHub Actions runs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """
    Main function to fetch data from Google Sheets and export it for the web dashboard.
    """
    # Load the central configuration file.
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Authenticate with Google Sheets using the service account key.
    gs = sheets.authenticate_google_sheets(config)

    # Fetch all records from the "Analysis Results" tab.
    rows = sheets.get_all_results(gs, config)

    # Read dashboard-specific settings from the config file.
    dash_cfg = config.get("dashboard", {}) or {}
    out_dir = dash_cfg.get("output_dir", "docs")
    out_name = dash_cfg.get("filename", "dashboard_data.json")
    
    # Ensure the output directory (e.g., 'docs/') exists.
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, out_name)

    # As defined in config.yaml, strip any sensitive columns before publishing the data.
    strip_cols = set(dash_cfg.get("strip_columns", []))
    if strip_cols:
        def clean_row(r):
            # Creates a new dictionary excluding any keys found in the strip_columns list.
            return {k: v for k, v in r.items() if k not in strip_cols}
        cleaned_rows = [clean_row(r) for r in rows]
    else:
        cleaned_rows = rows

    # Write the cleaned data to the final JSON file for the dashboard.
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_rows, f, ensure_ascii=False, indent=2)
    logging.info(f"Successfully exported {len(cleaned_rows)} records to {out_path}")

    # Your config also specifies to copy the master HTML file into the output directory.
    # This prepares the 'docs' folder for deployment to GitHub Pages.
    if dash_cfg.get("copy_html_from_root", True):
        # NOTE: You have two HTML files. This script uses 'dashboard.html' from the root
        # and copies it to 'docs/index.html'. Your GitHub Action then publishes the 'docs' folder.
        # This is a good setup.
        src_html = "dashboard.html"
        dest_html = os.path.join(out_dir, "index.html")
        if os.path.exists(src_html):
            shutil.copyfile(src_html, dest_html)
            logging.info(f"Successfully copied '{src_html}' to '{dest_html}' for deployment.")
        else:
            logging.warning(f"'{src_html}' not found at project root; skipping HTML copy.")

if __name__ == "__main__":
    # This script requires the GCP_SA_KEY environment variable to be set.
    # Your GitHub Actions workflow ('export-dashboard.yml') correctly provides this.
    if not os.environ.get("GCP_SA_KEY"):
        raise SystemExit("CRITICAL: GCP_SA_KEY environment variable is not set.")
    main()
