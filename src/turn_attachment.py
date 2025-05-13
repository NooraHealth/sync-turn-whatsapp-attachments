import requests
import argparse
import pandas as pd
import os
import mimetypes
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from . import utils




for _, row in df[2270:2300].iterrows():
    uri = row["uri"]
    phone = row["channel_phone"]
    media_type = row["media_type"]
    headers = turn_headers.get(phone)

    if headers is None:
        print(f"No headers configured for {phone!r}, skipping {uri}")
        continue

    resp = requests.get(uri, headers=headers)

    if resp.status_code != 200:
      print(f"Skipping {uri}: status {resp.status_code}")
      continue

    # derive a sane filename
    filename = os.path.basename(uri)  # e.g. "649092494159511" or "649092494159511.ogg"
    name, ext = os.path.splitext(filename)
    # if missing extension, derive one from mime_type
    if not ext:
        mime = row["mime_type"]
        ext = mimetypes.guess_extension(mime) if mime else None
        filename = f"{name}{ext or ''}"
        print(filename)
    filepath = os.path.join("samples", filename)
    # upload to GCS”
    destination = f"{media_type}/{filename}"
    blob = bucket.blob(destination)
    blob.upload_from_string(resp.content)
    # with open(filepath, "wb") as fp:
    #     fp.write(resp.content)
    print(f"Saved {filename}")

def main():
  try:
    source_name = 'andhra_pradesh_mlhp'
    args = parse_args()
    params = utils.get_params(source_name, args.params_path)
    

  except Exception as e:
    if params['environment'] == 'prod':
      text = utils.get_slack_message_text(e, source_name)
      utils.send_message_to_slack(
        text, params['slack_channel_id'], params['slack_token'])
    raise e


if __name__ == '__main__':
  main()