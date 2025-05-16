import google
import json
import os
import time
import pandas as pd
import oyaml as yaml
import slack_sdk
import mimetypes
from urllib.parse import urlparse
from pathlib import Path
from google.cloud import bigquery, bigquery_storage, storage
from google.oauth2.service_account import Credentials


def get_params(params_path = 'params.yaml', envir = None):
    """
    Load global settings and environment-specific bucket_name from params.yaml.
    Returns a dict with credentials, GCS project IDs, bucket_name, turn headers, and Slack config.
    """
    # Load params file
    with open(params_path) as f:
        params = yaml.safe_load(f)

    # Determine environment (dev vs prod) from GitHub Actions or override
    github_ref_name = os.getenv('GITHUB_REF_NAME')
    if envir is None:
        envir = 'prod' if github_ref_name == 'main' else 'dev'

    # Extract bucket_name for this environment
    env_cfg = params['environments'][envir]
    params['bucket_name'] = env_cfg['bucket_name']
    # Clean up
    del params['environments']

    # Load service account credentials and turn headers
    if github_ref_name is None:
        # Local dev: read files from 'secrets/'
        raw_key_path = Path('secrets', params['service_account_key_raw'])
        analytics_key_path = Path('secrets', params['service_account_key_analytics'])
        params['credentials_raw'] = Credentials.from_service_account_file(raw_key_path)
        params['credentials_analytics'] = Credentials.from_service_account_file(analytics_key_path)

        turn_headers_path = Path('secrets', params['turn_headers'])
        params['turn_headers'] = json.load(open(turn_headers_path))

        # Slack token from local file
        params['slack_token'] = Path('secrets', 'slack_token.txt').read_text().strip()
    else:
        # CI: read JSON strings from environment variables
        params['credentials_raw'] = Credentials.from_service_account_info(
            json.loads(os.getenv('SERVICE_ACCOUNT_KEY_RAW', '{}'))
        )
        params['credentials_analytics'] = Credentials.from_service_account_info(
            json.loads(os.getenv('SERVICE_ACCOUNT_KEY_ANALYTICS', '{}'))
        )
        params['turn_headers'] = json.loads(os.getenv('TURN_HEADERS', '{}'))
        params['slack_token'] = os.getenv('SLACK_TOKEN')

    # Slack channel ID (same for dev/prod)
    params['slack_channel_id'] = params['slack_channel_id']

    return params


def get_storage_client(project, credentials):
    return storage.Client(project=project, credentials=credentials)


def get_bigquery_client(project, credentials):
    return bigquery.Client(project=project, credentials=credentials)

def derive_filename(uri, mime_type):
    """
    Extract the basename from a URI; if missing an extension, guess from the MIME type.
    """
    path = urlparse(uri).path
    filename = os.path.basename(path)
    name, ext = os.path.splitext(filename)
    if not ext and mime_type:
        guessed = mimetypes.guess_extension(mime_type)
        ext = guessed or ''
        filename = f"{name}{ext}"
    return filename

# def upload_bytes_to_gcs(data, bucket, filename, destination):
#     """
#     Upload raw bytes to GCS, setting a content-type.
#     Returns the blob object.
#     """
#     blob = bucket.blob(destination)
#     blob.upload_from_string(
#       data,
#       content_type=mimetypes.guess_type(filename)[0] or "application/octet-stream"
#       )
#     content_type = mimetypes.guess_type(destination)[0] or 'application/octet-stream'
#     return blob


def run_read_bigquery(query, credentials, num_tries = 5, wait_secs = 5):
    """
    Execute a BigQuery query, retrying on concurrent-update errors.
    """
    bq_client = bigquery.Client(credentials = credentials)
    for attempt in range(1, num_tries + 1):
      try:
          df = (bq_client.query(query)  # executes the query
             .result()                  # waits for completion
             .to_dataframe())
          return df
      except google.api_core.exceptions.BadRequest as e:
          if 'due to concurrent update' in str(e) and attempt < num_tries:
              time.sleep(wait_secs)
          else:
              raise e

def get_slack_message_text(error: Exception) -> str:
    text = (
        f":warning: Sync failed with the following error:"
        f"\n\n`{str(error)}`"
    )
    run_url = os.getenv('RUN_URL')
    if run_url:
        text += f"\n\nSee the GitHub Actions <{run_url}|workflow log>."
    return text


def get_slack_message_text(e, source_name):
  text = (
    f':warning: Sync for *{source_name}* failed with the following error:'
    f'\n\n`{str(e)}`'
  )
  run_url = os.getenv('RUN_URL')
  if run_url is not None:
    text += f'\n\nPlease see the GitHub Actions <{run_url}|workflow run log>.'
  return text


def send_message_to_slack(text, channel_id, token):
  client = slack_sdk.WebClient(token = token)
  try:
    client.chat_postMessage(channel = channel_id, text = text)
  except slack_sdk.SlackApiError as e:
    assert e.response['error']
