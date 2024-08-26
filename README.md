# Daily AP CCP Data Fetch and Slack Report Automation
This repository contains a scheduled GitHub Action that performs the following tasks daily:
1. Fetches data from the AP CCP API.
2. Sends the data as an Excel file via Slack.
3. Creates a GitHub release with that day's data for historical record-keeping purposes.

## Setup Requirements
To use this action, you need to configure the following repository secrets and variables:
1. Repository Secrets:
   - `USERNAME`: The username credential for the AP CCP API.
   - `PASSWORD`: The password credential for the AP CCP API.
   - `SLACK_TOKEN`: The token for the Slack bot account used to send the file.

2. Repository Variables:
   - `API_URL`: The URL of the AP CCP API. Note that this currently uses a proxy URL because the actual API is inaccessible within the GitHub actions due to a misconfiguration on API server that cannot be resolved.
   - `SLACK_RECEIVER_ID`: The Slack ID of the user who should receive the message. To obtain a user's Slack ID, refer to this [article](https://www.workast.com/help/article/how-to-find-a-slack-user-id/).

Note: If either `SLACK_RECEIVER_ID` or `SLACK_TOKEN` is not set, the action will skip sending the file via Slack and only create the GitHub release.
