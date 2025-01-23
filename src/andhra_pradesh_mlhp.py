import argparse
import concurrent.futures
import datetime as dt
import functools
import github
import os
import polars as pl
import polars.selectors as cs
import re
import requests
import tqdm
from pathlib import Path
from . import utils


USERS_TABLE_NAME = 'users'
DEFAULT_EXTRACTED_AT = dt.datetime(2023, 4, 1).replace(tzinfo = dt.timezone.utc)


def get_chunk_dates(fromdate, todate, chunk_days):
  chunk_dates = pl.date_range(
    fromdate, todate, f'{chunk_days}d', closed = 'left', eager = True).to_list()
  chunk_dates.append(todate + dt.timedelta(days = 1))

  fromdates = [x.strftime('%Y-%m-%d') for x in chunk_dates[:-1]]
  todates = [(x - dt.timedelta(days = 1)).strftime('%Y-%m-%d') for x in chunk_dates[1:]]
  return zip(fromdates, todates)


def get_sessions_from_api(fromdate, todate, username, api_key, api_url):
  try_chunk_days = [180, 60, 21, 7]
  base_headers = {'username': username, 'ApiKey': api_key}
  no_data = {'status': 'Failed', 'msg': 'No Data Found'}

  for chunk_days in try_chunk_days:
    try:
      if chunk_days != try_chunk_days[0]:
        print(f'Downshifting username {username} to chunk_days={chunk_days}')
      chunk_dates = get_chunk_dates(fromdate, todate, chunk_days)
      data = []

      for chunk_fromdate, chunk_todate in chunk_dates:
        chunk_headers = {'fromdate': chunk_fromdate, 'todate': chunk_todate}
        response = requests.get(api_url, headers = chunk_headers | base_headers)
        response.raise_for_status()
        if response.json() != no_data and 'data' in response.json().keys():
          data.extend(response.json()['data'])
      break

    except Exception as e:
      if chunk_days == try_chunk_days[-1]:
        raise e

  if len(data) == 0:
    return pl.DataFrame()

  # TODO: decide whether to cast types
  int_cols = ['id']  # , 'patients_trained', 'member_trained']
  df = (
    pl.from_dicts(data)
    .with_columns(cs.by_name(int_cols, require_all = False).cast(pl.Int64))
    .with_columns(pl.col('session_date').str.to_date('%d-%m-%Y'))
    .with_columns(
      cs.by_name('subdata', require_all = False)
      .map_elements(utils.json_dumps_list, return_dtype = pl.String))
  )
  return df


def sync_sessions_by_user(user_dict, params, extracted_at, overlap_days = 30):
  # TODO: how far back can sessions be created, edited, or deleted? affects overlap_days
  q_pre = f"update `{params['dataset']}.{USERS_TABLE_NAME}` set"
  q_suf = f"where username = '{user_dict['username']}'"

  query = f'{q_pre} is_extracting = true {q_suf}'
  utils.run_bigquery(query, params['credentials'])

  fromdate = max(DEFAULT_EXTRACTED_AT.date(), user_dict['max_todate'])
  todate = extracted_at.date() - dt.timedelta(days = 1)

  try:
    df = get_sessions_from_api(
      fromdate, todate, user_dict['username'],
      params['source_params']['key'], params['source_params']['url'])

    if df.shape[0] > 0:
      df = utils.add_extracted_columns(df, extracted_at)
      utils.write_bigquery(df, 'sessions', params, 'WRITE_APPEND')

    query = (
      f"{q_pre} max_todate = date('{todate}'), "
      f"_extracted_at = timestamp('{extracted_at}'), "
      f"is_extracting = false {q_suf}"
    )
    val = df.shape[0]

  except Exception as e:
    print(f"Error for username {user_dict['username']}: {e}")
    query = f"{q_pre} is_extracting = false {q_suf}"
    val = -1

  utils.run_bigquery(query, params['credentials'])
  return val


def sync_data_to_warehouse(params, max_duration_mins, trigger_mode):
  col_name = '_extracted_at'

  if trigger_mode == 'continuing':
    q = f"select max({col_name}) from `{params['dataset']}.{USERS_TABLE_NAME}`"
    df = utils.read_bigquery(q, params['credentials'])
    extracted_at = df.item()

  if trigger_mode != 'continuing' or extracted_at is None:
    extracted_at = dt.datetime.now(dt.timezone.utc).replace(microsecond = 0)

  q = (
    f"select username, max_todate, _extracted_at "
    f"from `{params['dataset']}.{USERS_TABLE_NAME}`"
  )
  if trigger_mode == 'continuing':
    q += (
      f" where {col_name} is null or {col_name} != "
      f"(select max({col_name}) from `{params['dataset']}.{USERS_TABLE_NAME}`)"
    )

  users = utils.read_bigquery(q, params['credentials'])
  users = users.sort([col_name, 'username'])

  sync_sessions_by_user_p = functools.partial(
    sync_sessions_by_user, params = params, extracted_at = extracted_at)

  if max_duration_mins > 0:
    timeout = max_duration_mins * 60
  else:
    timeout = None

  try:  # api is easily overwhelmed
    with concurrent.futures.ThreadPoolExecutor(2) as executor:
      list(tqdm.tqdm(
        executor.map(
          sync_sessions_by_user_p, users.rows(named = True), timeout = timeout),
        total = users.shape[0]))

  except TimeoutError:
    if (trigger_mode in ('oneormore', 'continuing')
        and params['github_ref_name'] is not None):  # noqa: W503
      trigger_workflow(max_duration_mins)

  return True


def trigger_workflow(max_duration_mins, trigger_mode = 'continuing'):
  g = github.Github(login_or_token = os.getenv('GH_PAT'))
  repo = g.get_repo(os.getenv('GITHUB_REPOSITORY'))
  pattern = '(?<=\\.github/workflows/).+\\.ya?ml'
  workflow_name = re.search(pattern, os.getenv('GITHUB_WORKFLOW_REF')).group(0)
  workflow = repo.get_workflow(workflow_name)
  inputs = {'max_duration_mins': str(max_duration_mins), 'trigger_mode': trigger_mode}
  response = workflow.create_dispatch(ref = os.getenv('GITHUB_REF_NAME'), inputs = inputs)
  return response


def upload_users(params, data_dir = 'data'):
  file_name = 'MLHP CHO Second Batch - mlhp_data_10032_new.csv'
  schema_overrides = dict(nin_number = pl.String, mobile_no = pl.String)
  colname_mapping = dict(
    old_district = 'old_district_name',
    new_district = 'district_name',
    mandal = 'mandal_name',
    ysr_clinicname = 'ysr_clinic_name',
    nin_number = 'username',
    sec_code = 'secretariat_code',
    mobile_no = 'mobile_number'
  )
  created_at = dt.datetime.now(dt.timezone.utc).replace(microsecond = 0)

  df_raw = pl.read_csv(Path(data_dir, file_name), schema_overrides = schema_overrides)
  # df_raw.group_by('nin_number').len().filter(pl.col('len') > 1)  # hopefully empty
  df = (
    df_raw.rename(colname_mapping)
    .with_columns(
      created_at = created_at,
      max_todate = DEFAULT_EXTRACTED_AT.date(),
      _extracted_at = DEFAULT_EXTRACTED_AT,
      is_extracting = False
    )
    .unique('username', keep = 'first')
  )
  # with pl.Config(tbl_cols = -1):
  #   print(df)
  # TODO: combine and reconcile first and second batches, if necessary
  # TODO: deal with the next batch when it comes, without refetching all data

  utils.write_bigquery(df_raw, f'{USERS_TABLE_NAME}_raw', params, 'WRITE_EMPTY')
  utils.write_bigquery(df, USERS_TABLE_NAME, params, 'WRITE_EMPTY')


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--params-path', default = 'params.yaml', type = Path)
  parser.add_argument('--max-duration-mins', default = 60, type = int)
  parser.add_argument(
    '--trigger-mode', default = 'oneanddone',
    choices = ['oneanddone', 'oneormore', 'continuing'])
  args = parser.parse_args()
  return args


def main():
  try:
    source_name = 'andhra_pradesh_mlhp'
    args = parse_args()
    params = utils.get_params(source_name, args.params_path)
    sync_data_to_warehouse(params, args.max_duration_mins, args.trigger_mode)

  except Exception as e:
    if params['environment'] == 'prod':
      text = utils.get_slack_message_text(e, source_name)
      utils.send_message_to_slack(
        text, params['slack_channel_id'], params['slack_token'])
    raise e


if __name__ == '__main__':
  main()
