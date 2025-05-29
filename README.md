[![sync_whatsapp_attachments](https://github.com/NooraHealth/sync-turn-whatsapp-attachments/actions/workflows/sync_whatsapp_attachments.yaml/badge.svg)](https://github.com/NooraHealth/sync-turn-whatsapp-attachments/actions/workflows/sync_whatsapp_attachments.yaml)

# Overview

This repository contains GitHub Actions workflows that fetch inbound message attachmnets data from Turn API and sync the data to GCP storage bucket.

The sync runs daily and the code fetches data for the past 2 days.

## Setup

1. Set up uv for managing dependencies.
   1. Install uv according to instructions [here](https://docs.astral.sh/uv/getting-started/installation/).
   2. Install python using `uv python install`.
   3. Sync the project's dependencies using `uv sync`.
   4. Add and remove dependencies using `uv add package_name` and `uv remove package_name`.
   5. Run python scripts using `uv run path_to_script.py`.

2. Configure the repository secrets locally.
   1. Create a file secrets/slack_token.txt that contains the Slack bot token.
   2. Create a file secrets/service_account_key_raw.json that contains the JSON key for the service account that will dump the data to the storage buckets (airbyte-user). Ensure that this has 'Storage Object User' permission on the dev and prod buckets.
   3. Create a file secrets/service_account_key_analytics.json that contains the JSON key for the service account that will connect to BigQuery (metabase-user).
   4. Create a file secrets/turn_auth.json that contains the JSON key for the API headers used by turn for each line (available on passbolt).

3. Configure the repository secrets on GitHub.
   1. Copy and paste the contents of secrets/slack_token.txt into a secret named "SLACK_TOKEN".
   2. Copy and paste the contents of secrets/service_account_key_raw.json into a secret named "SERVICE_ACCOUNT_KEY_RAW".
   3. Copy and paste the contents of secrets/service_account_key_analytics.json into a secret named "SERVICE_ACCOUNT_KEY_ANALYTICS".
   4. Copy and paste the contents of secrets/turn_auth.json into a secret named "TURN_HEADERS".
   5. Create a GH Token with read and write access for Actions and paste it into a secret named "GH_PAT".
