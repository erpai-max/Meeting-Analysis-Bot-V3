# export_dashboard.py
import os, json, yaml, gspread
from google.oauth2 import service_account

def main():
    # Load config
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sheet_id = config["google_sheets"]["sheet_id"]
    results_tab = config["google_sheets"]["results_tab_name"]

    # Auth with service account (same key you already use)
    gcp_key_str = os.environ["GCP_SA_KEY"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(gcp_key_str),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    client = gspread.authorize(creds)

    ws = client.open_by_key(sheet_id).worksheet(results_tab)
    rows = ws.get_all_records()

    # Ensure site/ exists and write JSON there
    os.makedirs("site", exist_ok=True)
    with open("site/dashboard_data.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # Also copy dashboard.html (if stored at repo root) to site/
    src = "dashboard.html"
    if os.path.exists(src):
        with open(src, "r", encoding="utf-8") as fr, open("site/dashboard.html", "w", encoding="utf-8") as fw:
            fw.write(fr.read())

    print(f"Exported {len(rows)} rows to site/dashboard_data.json")

if __name__ == "__main__":
    main()
