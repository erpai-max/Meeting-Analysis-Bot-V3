Meeting Analysis Bot - V4
This repository contains the code for a sophisticated, fully automated meeting analysis platform. The system leverages Google Drive, GitHub Actions, Google's Gemini AI, and Google BigQuery to create a robust pipeline that processes audio/video meeting recordings and transforms them into actionable business intelligence.

Key Features
Automated Processing: Automatically discovers new meeting recordings in a structured Google Drive folder system (Parent Folder -> City -> Team Member).

AI-Powered Transcription & Analysis: Uses faster-whisper for accurate transcription and a powerful, customizable Gemini AI prompt to perform a deep analysis of the conversation, extracting 47 distinct data points and performance scores.

Intelligent Data Enrichment: Automatically populates key data fields like Owner, Manager, Team, and Email Id based on the folder structure and a central configuration file. It can also parse filenames for Date, Society Name, and Meeting Type.

Robust Error Handling:

Quarantine Flow: Automatically moves files that cause an unrecoverable error to a "Quarantine" folder with an error note for easy triage.

Retry Logic: Implements exponential backoff for all API calls, making the system resilient to temporary network or service issues.

Idempotency: Maintains a "Processed Ledger" in a Google Sheet to ensure files are never processed more than once, saving time and resources.

Long-Term Data Warehousing: Streams all analysis results to a Google BigQuery table, creating a scalable, long-term data warehouse for advanced analytics.

Automated AI-Powered Weekly Digests: A separate, scheduled workflow runs every Friday to generate and email a professional HTML summary report to managers. The report includes an AI-generated executive summary, team performance KPIs, and week-over-week performance tracking.

Centralized Configuration: All system settings, prompts, and mappings are managed in a single, easy-to-edit config.yaml file.

Project Architecture
The system is composed of several modules that work together:

main.py: The primary orchestrator. It runs frequently to discover and process new meeting files.

digest_generator.py: The reporting engine. It runs weekly to query BigQuery, generate insights, and email the digest.

config.yaml: The "control panel" for the entire project. All settings are managed here.

Utility Modules (gdrive.py, analysis.py, sheets.py, email_formatter.py): These files contain the specific logic for interacting with Google services, performing analysis, and formatting reports.

GitHub Workflows (run_analysis.yml, run_digest.yml): These files define the schedule and environment for the automated scripts.

Setup and Configuration Guide
Follow these steps to set up the project in a new repository.

Phase 1: Google Cloud & Drive Setup
Google Drive Folder Structure:

Create a main parent folder (e.g., Meeting Tracker 2025).

Inside, create subfolders for each city (e.g., Bangalore, Hyderabad).

Inside each city folder, create subfolders for each team member (e.g., Sharath, Hemanth).

Create two separate, top-level folders: Processed Meetings and Quarantined Meetings.

Google Sheet Setup:

Create a new, blank Google Sheet.

Rename the first tab to Analysis Results.

Create a second tab and name it Processed Ledger.

Google Cloud Project:

Go to the Google Cloud Console.

Create a new project or use an existing one.

Enable the following APIs:

Google Drive API

Google Sheets API

Google BigQuery API

Vertex AI API (for Gemini)

Create a Service Account. Go to IAM & Admin -> Service Accounts, click Create Service Account, give it a name, and grant it the Editor role.

Create a JSON key for this service account and download it.

Set Permissions:

Open the downloaded JSON key and find the client_email.

Share the following with that client_email, giving it Editor permissions:

The main Meeting Tracker 2025 folder.

The Processed Meetings folder.

The Quarantined Meetings folder.

The Google Sheet.

Phase 2: GitHub Repository Setup
Create a new private repository on GitHub.

Create the project files: Add the following files to your repository:

config.yaml

requirements.txt

main.py

gdrive.py

analysis.py

sheets.py

digest_generator.py

email_formatter.py

Create the workflow files:

In your repository, create the .github/workflows/ directory.

Inside, create run_analysis.yml and run_digest.yml.

Copy the code from this project into the corresponding files.

Phase 3: Final Configuration
Edit config.yaml: This is your most important step. Open config.yaml and fill in all the placeholder values:

parent_folder_id, processed_folder_id, quarantine_folder_id.

project_id (from your Google Cloud project info).

sheet_id (from your Google Sheet URL).

Review the manager_map and manager_emails sections and ensure they are correct.

Add GitHub Secrets: Go to your repository's Settings > Secrets and variables > Actions and add the following secrets:

GCP_SA_KEY: Paste the entire content of your downloaded service account JSON file.

GEMINI_API_KEY: Paste your API key from Google AI Studio.

MAIL_USERNAME: The Gmail address you will use to send the weekly digest.

MAIL_PASSWORD: The 16-character Google App Password for that email account.

Phase 4: How to Use
Automated Runs: The system is now fully automated. The run_analysis.yml workflow will run every hour to process new files, and the run_digest.yml workflow will run every Friday morning to send the report.

Manual Runs: You can trigger either workflow manually at any time by going to the Actions tab, selecting the workflow, and clicking Run workflow.

Pausing: To pause the automation, go to the Actions tab, select a workflow, click the ... menu, and choose Disable workflow.
