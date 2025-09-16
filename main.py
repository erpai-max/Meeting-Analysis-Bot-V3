import os
import json
import logging
import yaml

from gdrive import authenticate_gdrive
import sheets
import analysis

# =======================
# Logging
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =======================
# Auth with Google
# =======================
def authenticate_google(config):
    """Authenticate with Drive + Sheets."""
    try:
        drive_service = authenticate_gdrive(config)
        gsheets_client = sheets.authenticate_google_sheets(config)  # FIXED
        logging.info("SUCCESS: Authentication with Google services complete.")
        return drive_service, gsheets_client
    except Exception as e:
        logging.error(f"CRITICAL: Authentication failed: {e}")
        raise

# =======================
# Main
# =======================
def main():
    logging.info("--- Starting Meeting Analysis Bot v5 ---")

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    logging.info("Loaded configuration from config.yaml")

    # Authenticate
    drive_service, gsheets_client = authenticate_google(config)

    from gdrive import scan_and_process_all
    scan_and_process_all(drive_service, gsheets_client, config)

if __name__ == "__main__":
    main()
