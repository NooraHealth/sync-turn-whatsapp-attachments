import requests
import mimetypes
import argparse
import utils
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="Sync WhatsApp attachments to GCS and notify on errors.")
    parser.add_argument(
        '--params-path',
        type=Path,
        default=Path('params.yaml'),
        help='Path to the params.yaml file'
    )
    return parser.parse_args()


def main():
    args = parse_args()
    params = utils.get_params(args.params_path)

    try:
        # Fetch recent attachments from BigQuery
        project_analytics = params['project_analytics']
        creds_analytics = params['credentials_analytics']
        query = f"""
          SELECT *
          FROM `{project_analytics}.prod.res_message_attachments`
          WHERE inserted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 HOUR)
            AND media_type NOT IN ('location', 'sticker')
          ORDER BY channel_phone
          """
        df = utils.run_read_bigquery(query, creds_analytics)

        # Prepare GCS bucket
        storage_client = utils.get_storage_client(params['project_raw'], params['credentials_raw'])
        bucket = storage_client.bucket(params['bucket_name'])

        # Download each attachment and upload to GCS
        for _, row in df.iterrows():
            uri = row['uri']
            media_type = row['media_type']
            mime_type = row['mime_type']
            phone = row['channel_phone']
            #headers = params['turn_headers'].get(phone)
            headers = params['turn_headers'][phone]

            if headers is None:
                #print(f"No headers configured for {phone!r}, skipping {uri}")
                continue

            response = requests.get(uri, headers=headers)
            if response.status_code == 200:
                filename = utils.derive_filename(uri, mime_type)
                destination = f"{media_type}/{filename}"
                blob = bucket.blob(destination)
                blob.upload_from_string(
                    response.content,
                    content_type=mimetypes.guess_type(filename)[0] or "application/octet-stream"
                )
                print(f"Saved {filename}")
            else:
                #print(f"Skipping {uri}: status {response.status_code}")
                continue

    except Exception as e:
        # Notify on Slack if running in prod
        text = utils.get_slack_message_text(e)
        utils.send_message_to_slack(
            text,
            params['slack_channel_id'],
            params['slack_token']
        )
        raise e


if __name__ == '__main__':
    main()
