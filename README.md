[![sync-to-bigquery](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync-to-bigquery.yaml/badge.svg)](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync-to-bigquery.yaml)

# Overview

This repository contains GitHub Actions workflows that fetch data from the CCP Andhra Pradesh API and then sync the data to the BigQuery data warehouse.

When syncing to BigQuery, the code fetches data starting with 30 days prior to the latest data existing in BigQuery. This redundancy accounts for the possibility that historical data behind the API might change. The data are deduplicated in dbt.

## Setup

1. Set up uv for managing dependencies.
   1. Install uv according to instructions [here](https://docs.astral.sh/uv/getting-started/installation/).
   2. Install python using `uv python install`.
   3. Sync the project's dependencies using `uv sync`.
   4. Add and remove dependencies using `uv add package_name` and `uv remove package_name`.
   5. Run python scripts using `uv run path_to_script.py`.

2. Configure the repository secrets locally.
   1. Create a file secrets/api.yml as follows, replacing `xxx` as appropriate:

      ```yaml
      url: xxx
      username: xxx
      password: xxx
      ```
      The `url` is currently a proxy URL, because the actual API is inaccessible within GitHub Actions due to an unresolvable misconfiguration on the API server.
   2. Create a file secrets/slack.yml as follows, replacing `xxx` as appropriate:

      ```yaml
      token: xxx
      channel_id: xxx
      ```
      The `token` is for the bot account used to send the data. To get the `channel_id`, right click to select "View channel details" or "View conversation details", then look at the bottom of the About tab.
   3. Create a file secrets/service_account_key.json that contains the JSON key for the service account that will connect to BigQuery. The name of the file should match the value of `service_account_key` in params/bigquery.yml.

3. Configure the repository secrets on GitHub.
   1. Copy and paste the contents of secrets/api.yml into a secret named "API_PARAMS".
   2. Copy and paste the contents of secrets/slack.yml into a secret named "SLACK_PARAMS".
   3. Copy and paste the contents of secrets/bigquery_service_account_key.json into a secret named "SERVICE_ACCOUNT_KEY".
