import google
import io
import json
import os
import oyaml as yaml
import polars as pl
import slack_sdk
import time
import uuid
import warnings
import mimetypes
from google.cloud import bigquery, storage
from google.oauth2.service_account import Credentials
from pathlib import Path


def get_params(source_name, params_path = 'params.yaml', envir = None):
  with open(params_path) as f:
    params = yaml.safe_load(f)

  github_ref_name = os.getenv('GITHUB_REF_NAME')
  params.update({'github_ref_name': github_ref_name})
  if envir is None:
    envir = 'prod' if github_ref_name == 'main' else 'dev'

  y = [x for x in params['sources'] if x['name'] == source_name][0]
  params['source_name'] = y['name']

  z = [x for x in y['environments'] if x['name'] == envir][0]
  params['environment'] = z.pop('name')
  params.update(z)
  del params['sources']

  key_raw = 'service_account_key_raw'
  key_analytics = 'service_account_key_analytics'
  if github_ref_name is None:
    with open(Path('secrets', params[key_raw])) as f:
      params[key_raw] = json.load(f)
    with open(Path('secrets', params[key_analytics])) as f:
      params[key_analytics] = json.load(f)
    with open(Path('secrets', 'slack_token.txt')) as f:
      params['slack_token'] = f.read().strip('\n')
    with open(Path('secrets', f'{source_name}.yaml')) as f:
      params['source_params'] = yaml.safe_load(f)
  else:
    params[key_raw] = json.loads(os.getenv('SERVICE_ACCOUNT_KEY_RAW'))
    params[key_analytics] = json.loads(os.getenv('SERVICE_ACCOUNT_KEY_ANALYTICS'))
    params['slack_token'] = os.getenv('SLACK_TOKEN')
    params['source_params'] = yaml.safe_load(os.getenv('SOURCE_PARAMS'))

  params['credentials_raw'] = Credentials.from_service_account_info(params[key_raw])
  params['credentials_analytics'] = Credentials.from_service_account_info(params[key_analytics])
  del params[key_raw]
  del params[key_analytics]
  return params


def json_dumps_list(x):
  return json.dumps(x.to_list())


def run_bigquery(query, credentials, num_tries = 5, wait_secs = 5):
  client = bigquery.Client(credentials = credentials)
  gtg = False
  idx_try = 1
  while not gtg and idx_try <= num_tries:
    try:
      query_job = client.query(query)
      result = query_job.result()
      gtg = True
    except google.api_core.exceptions.BadRequest as e:
      if 'due to concurrent update' in str(e) and (idx_try < num_tries):
        time.sleep(wait_secs)
        idx_try += 1
      else:
        raise e
  return result


def read_bigquery(query, credentials):
  with warnings.catch_warnings(action = 'ignore'):
    rows = run_bigquery(query, credentials).to_arrow()
    df = pl.from_arrow(rows)
  return df


def read_bigquery_exists(table_name, params):
  q = f"select * from `{params['dataset']}.__TABLES__` where table_id = '{table_name}'"
  df = read_bigquery(q, params['credentials'])
  return df.shape[0] > 0

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
