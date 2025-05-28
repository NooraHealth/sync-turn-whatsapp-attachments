import requests
import mimetypes
import argparse
from pathlib import Path
from . import utils


def parse_args():
    parser = argparse.ArgumentParser(description="Sync WhatsApp attachments to GCS and notify on errors.")
    parser.add_argument(
        '--params-path',
        type=Path,
        default=Path('params.yaml'),
        help='Path to the params.yaml file'
    )
    parser.add_argument('--past-hours', default = 25, type = int)
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
          WHERE
            inserted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {args.past_hours} HOUR)
            AND direction = 'inbound'
            AND media_type NOT IN ('location', 'sticker')
          ORDER BY channel_phone
          """
        df = utils.run_read_bigquery(query, creds_analytics)

        # Prepare GCS bucket
        storage_client = utils.get_storage_client(params['project_raw'], params['credentials_raw'])
        bucket = storage_client.bucket(params['bucket_name'])

        # Download each attachment and upload to GCS
        df.apply(lambda row: utils.transfer_file(row, bucket, params['turn_headers']), axis=1)

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
