Meeting Analysis Bot (V4)
An automated Business Intelligence platform that analyzes sales and service meeting recordings from Google Drive, extracts deep insights using AI, and persists the data for long-term analysis and coaching.

This system is designed to be robust, resilient, and easy to manage, transforming raw meeting files into actionable business intelligence.

Core Features
This is not just a simple script; it's an enterprise-grade platform with the following key features:

🤖 Automated Transcription & AI Analysis: Uses faster-whisper for accurate transcription and a powerful, evidence-based Gemini prompt for deep conversational analysis.

📂 Dynamic Folder Discovery: Automatically discovers your City -> Team Member folder structure. You can add new team members or cities in Google Drive, and the bot will find them without any code changes.

✨ Data Enrichment Engine: Intelligently enriches AI-generated data with context from folder names (Owner, Manager, Team, Email) and filenames (Kibana ID, Dates, Meeting Type).

🛡️ Robust Error & Quota Handling:

Retry Logic: Automatically retries failed API calls with exponential backoff.

Quarantine Flow: If a file fails for an unrecoverable reason, it's moved to a "Quarantine" folder with an error note for manual review. You never lose a failed file.

Graceful Quota Exit: Detects Gemini API rate limits, stops the current run, and leaves unprocessed files to be picked up by the next scheduled run.

💾 Persistent & Idempotent:

Processed Ledger: Maintains a log in a separate Google Sheet tab of every file it processes, ensuring it never wastes time or resources re-analyzing a completed file.

BigQuery Data Warehousing: Automatically writes every analysis record to a Google BigQuery table for scalable, long-term data storage and advanced analytics.

🔒 PII Redaction: Automatically scrubs emails and phone numbers from transcripts before sending them to the AI to enhance privacy.

** digests_generator.py Ready:** Includes a separate script and workflow to automatically generate and send weekly performance digests to managers via Slack.

How It Works
The system is orchestrated by two main GitHub Action workflows:

run_analysis.yml (Hourly):

Trigger: Runs automatically every hour or can be triggered manually.

Process:

Authenticates with Google services.

Reads the Processed Ledger from Google Sheets to know which files to ignore.

Scans your Meeting Tracker folder structure in Google Drive to find new audio/video files.

For each new file:

Downloads it securely.

Transcribes it to text.

Redacts personal information.

Sends the clean transcript to the Gemini AI for deep analysis using the advanced prompt in config.yaml.

Enriches the AI's response with data from the file's context.

Writes the final, clean record to both Google Sheets (for easy viewing) and Google BigQuery (for long-term storage).

Moves the original file to the Processed Meetings folder.

Updates the Processed Ledger with a "Success" status.

If any step fails, the file is moved to the Quarantined Meetings folder and the ledger is updated with the error.

run_digest.yml (Weekly):

Trigger: Runs automatically every Friday (or can be run manually).

Process:

Queries the BigQuery database for all meetings analyzed in the past 7 days.

Generates a summary of team performance and identifies coaching opportunities.

Sends this summary as a formatted message to a designated Slack channel.

🚀 Setup Guide
Follow these steps to deploy your own instance of the Meeting Analysis Bot.

Phase 1: Google Cloud & Drive Preparation
Google Cloud Project:

Ensure you have a Google Cloud Project.

Enable the following APIs: Google Drive API, Google Sheets API, Google Cloud AI Platform API, and BigQuery API.

Create a Service Account and download its JSON key file.

Google Drive Folder Structure:

Create a main parent folder (e.g., Meeting Tracker 2025).

Inside, create your city subfolders (e.g., Bangalore, Hyderabad).

Inside each city, create your team member subfolders (e.g., Sharath, Hemanth).

Separately, create two empty folders: Processed Meetings and Quarantined Meetings.

Google Sheet Setup:

Create a new, blank Google Sheet.

Rename the first tab at the bottom to Analysis Results.

Create a second tab and name it Processed Ledger.

Set Permissions:

Find the client_email in your downloaded service account JSON key.

Share the following four items with that client_email, giving it Editor permissions:

The main Meeting Tracker 2025 folder.

The Processed Meetings folder.

The Quarantined Meetings folder.

Your Google Sheet.

Phase 2: GitHub Repository Setup
Create Repository: Create a new private GitHub repository (e.g., Meeting-Analysis-Bot-V4).

Add Project Files: Upload all the project files:

.github/workflows/run_analysis.yml

.github/workflows/run_digest.yml

config.yaml

requirements.txt

main.py

gdrive.py

analysis.py

sheets.py

digest_generator.py

Add GitHub Secrets: Go to your repository's Settings > Secrets and variables > Actions and add the following:

GCP_SA_KEY: Paste the entire content of your service account JSON file.

GEMINI_API_KEY: Paste your API key from Google AI Studio.

SLACK_WEBHOOK_URL (Optional): Your incoming webhook URL from Slack for weekly digests.

Phase 3: Final Configuration
Edit config.yaml: This is your main control panel.

Open config.yaml in your repository.

Fill in your unique IDs for parent_folder_id, processed_folder_id, quarantine_folder_id, project_id (for BigQuery), and sheet_id.

Review the manager_map and manager_emails to ensure they are correct.

Commit your changes.

Phase 4: Run the Bot!
You are all set.

Go to the Actions tab of your repository.

Select "Run Meeting Analysis" and click Run workflow.

The bot will start processing any files it finds. The first run may take a long time as it processes your entire backlog. Subsequent runs will be much faster.
