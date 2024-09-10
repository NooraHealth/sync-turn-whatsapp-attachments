import argparse
import datetime as dt
import hashlib
import json
import os
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
import pandas_gbq
import requests
from google.oauth2 import service_account
from slack_sdk import WebClient


def dict_hash(dictionary):
    dictionary_json = json.dumps(dictionary, sort_keys=True)

    # Create an MD5 hash object
    hasher = hashlib.md5()

    # Encode the JSON string and update the hasher
    hasher.update(dictionary_json.encode("utf-8"))

    # Return the hexadecimal representation of the hash
    return hasher.hexdigest()


def dt_iterate(start, end, step):
    assert start <= end, "start should be less than end"
    while start <= end:
        yield start
        start += step


class Report:
    def __init__(self, base_url: str, username: str, password: str):
        self.url = base_url
        self.username = username
        self.password = password
        self.key = None

    def login(self):
        response = requests.get(
            self.url,
            json={
                "login": True,
                "username": self.username,
                "password": self.password,
            },
        )
        response.raise_for_status()

        data = response.json()

        assert data["result"] == "success", f"Login unsuccessful: {data}"

        print("Login successful. Auth-Key:", data["Auth-Key"])
        self.key = data["Auth-Key"]

    @staticmethod
    def has_token_expired(data):
        return data["result"] == "failed" and data["error"] in (
            "Invalid or token expired",
            "Expired token",
        )

    @property
    def headers(self):
        return {"Auth-Key": self.key, "Username": self.username}

    def extract_attendance(self, date: datetime):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={
                "get_total_ccp_class_attendancedata": True,
                "date": date.strftime("%d-%m-%Y"),
            },
        )
        response.raise_for_status()

        data = response.json()

        if Report.has_token_expired(data):
            self.login()
            return self.extract_attendance(date)
        else:
            print(f"Attendance data retrieved successfully for date: {date}")
            return data["data"] if data["result"] == "success" else []

    def extract_nurse_training(self, date: datetime):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={
                "get_total_nurse_training_sessiondata": True,
                "date": date.strftime("%d-%m-%Y"),
            },
        )
        response.raise_for_status()

        data = response.json()

        if Report.has_token_expired(data):
            self.login()
            return self.extract_nurse_training(date)
        else:
            print(f"Nurse Training data retrieved successfully for date: {date}")
            return data["data"] if data["result"] == "success" else []

    def fetch_nurse_details(self, phone_number: str):
        response = requests.get(
            self.url,
            headers=self.headers,
            json={"get_nurses_detailes_data": True, "username": phone_number},
        )
        response.raise_for_status()

        data = response.json()

        # check for expired token, and if expired, regenerate token
        if Report.has_token_expired(data):
            # i.e. token expired
            self.login()
            return self.fetch_nurse_details(phone_number)
        else:
            print(
                f"Nurse profile retrieved successfully for number: {phone_number[:2]}{"X"*8}"
            )
            return data["data"] if data["result"] == "success" else []


def init_envs():
    username = os.environ.get("USERNAME")
    password = os.environ.get("PASSWORD")
    api_url = os.environ.get("API_URL")
    slack_token = os.environ.get("SLACK_TOKEN")
    slack_user_id = os.environ.get("SLACK_RECEIVER_ID")
    bigquery_dataset = os.environ.get("BIGQUERY_DATASET_NAME")

    if username == None:
        raise Exception(
            "Unable to fetch data. Did you set correct API username in respository secrets?"
        )

    if password == None:
        raise Exception(
            "Unable to fetch data. Did you set correct API password in respository secrets?"
        )

    if api_url == None:
        raise Exception(
            "Unable to fetch data. Did you set correct API_URL in respository variables?"
        )

    if bigquery_dataset == None:
        print(
            "Unable to sync data to bigquery. Did you set correct BIGQUERY_DATASET_NAME in respository variables?"
        )

    if slack_token == None:
        print(
            "Unable to send message through slack. Did you set SLACK_TOKEN in repository secrets?"
        )

    if slack_user_id == None:
        print(
            "Unable to send message through slack. Did you set SLACK_RECEIVER_ID in repository variables?"
        )

    return {
        "username": username,
        "password": password,
        "api_url": api_url,
        "slack_token": slack_token,
        "slack_user_id": slack_user_id,
        "bigquery_dataset": bigquery_dataset,
    }


def execute(start_date: datetime, end_date: datetime, envs: dict, sync_bigquery: bool):
    username = envs["username"]
    password = envs["password"]
    api_url = envs["api_url"]
    report = Report(api_url, username, password)
    report.login()

    with Pool(8) as p:
        atts = p.map(
            report.extract_attendance,
            [*dt_iterate(start_date, end_date, timedelta(days=1))],
        )
    attendances = [attendance for att in atts for attendance in att]

    with Pool(8) as p:
        nurse_trainings = p.map(
            report.extract_nurse_training,
            [*dt_iterate(start_date, end_date, timedelta(days=1))],
        )
    nurse_training_data = [attendance for att in nurse_trainings for attendance in att]

    if not attendances:
        print("No attendances found")
        return

    for attendance in attendances:
        attendance["md5"] = dict_hash(attendance)

    for nurse_training in nurse_training_data:
        nurse_training["md5"] = dict_hash(nurse_training)

    phones_in_attendance = [
        item
        for attendee in attendances
        for item in attendee["session_conducted_by"].split(",")
    ]

    phones_in_training = []
    for i in nurse_training_data:
        if i.get("trainerdata1"):
            for trainer in i["trainerdata1"]:
                phones_in_training.append(trainer["phone_no"])

        if i.get("traineesdata1"):
            for trainee in i["traineesdata1"]:
                phones_in_training.append(trainee["phone_no"])

    all_phones = list(set(phones_in_attendance + phones_in_training))


    if sync_bigquery == True:
        credentials = service_account.Credentials.from_service_account_file("sc.json")

        existing_nurses = pandas_gbq.read_gbq(
            f"SELECT * FROM `{envs['bigquery_dataset']}.nurses_v1`",
            project_id="noorahealth-raw",
            credentials=credentials,
        )

        filtered_phones = []
        for phone in all_phones:
            search_nurse_df = existing_nurses.query(f'username == "{phone}"')
            if search_nurse_df.empty:
                filtered_phones.append(phone)

        phones_list = filtered_phones
    else:
        phones_list = phones_in_attendance

    with Pool(8) as p:
        nss = p.map(report.fetch_nurse_details, [p for p in phones_list])
    nurses = [n for ns in nss for n in ns]

    nurses_df = pd.DataFrame(nurses)
    nurses_df["user_created_dateandtime"] = pd.to_datetime(
        nurses_df["user_created_dateandtime"], format="%Y-%m-%d %H:%M:%S"
    )

    attendance_df = pd.DataFrame(attendances)
    attendance_df["data1"] = attendance_df["data1"].astype(str)
    attendance_df["date_of_session"] = pd.to_datetime(
        attendance_df["date_of_session"], format="%d-%m-%Y"
    )
    attendance_df["mothers_trained"] = attendance_df["mothers_trained"].astype(int)
    attendance_df["family_members_trained"] = attendance_df[
        "family_members_trained"
    ].astype(int)
    attendance_df["total_trained"] = attendance_df["total_trained"].astype(int)

    training_df = pd.DataFrame(nurse_training_data)
    training_df["trainerdata1"] = training_df["trainerdata1"].astype(str)
    training_df["traineesdata1"] = training_df["traineesdata1"].astype(str)
    training_df["sessiondateandtime"] = pd.to_datetime(
        training_df["sessiondateandtime"], format="%d-%m-%Y"
    )
    training_df["totalmaster_trainer"] = training_df["totalmaster_trainer"].astype(int)
    training_df["total_trainees"] = training_df["total_trainees"].astype(int)

    if sync_bigquery == True:
        pandas_gbq.to_gbq(
            nurses_df,
            f"{envs['bigquery_dataset']}.nurses_v1",
            project_id="noorahealth-raw",
            credentials=credentials,
            if_exists="append",
            table_schema=[
                {"type": "STRING", "name": "username", "mode": "REQUIRED"},
                {
                    "type": "TIMESTAMP",
                    "name": "user_created_dateandtime",
                    "mode": "NULLABLE",
                },
            ],
        )

        pandas_gbq.to_gbq(
            attendance_df,
            f"{envs['bigquery_dataset']}.patient_training_v1",
            project_id="noorahealth-raw",
            credentials=credentials,
            if_exists="append",
            table_schema=[
                {"type": "DATE", "name": "date_of_session", "mode": "NULLABLE"},
                {"type": "INT64", "name": "mothers_trained", "mode": "NULLABLE"},
                {"type": "INT64", "name": "family_members_trained", "mode": "NULLABLE"},
                {"type": "INT64", "name": "total_trained", "mode": "NULLABLE"},
            ],
        )

        pandas_gbq.to_gbq(
            training_df,
            f"{envs['bigquery_dataset']}.nurse_training_v1",
            project_id="noorahealth-raw",
            credentials=credentials,
            if_exists="append",
            table_schema=[
                {"type": "INT64", "name": "total_trainees", "mode": "NULLABLE"},
                {"type": "INT64", "name": "totalmaster_trainer", "mode": "NULLABLE"},
                {"type": "DATE", "name": "sessiondateandtime", "mode": "NULLABLE"},
                {"type": "STRING", "name": "traineesdata1", "mode": "NULLABLE"},
                {"type": "STRING", "name": "trainerdata1", "mode": "NULLABLE"},
            ],
        )
    else:
        with pd.ExcelWriter("data.xlsx", engine="xlsxwriter") as writer:
            pd.DataFrame(training_df).to_excel(
                writer, sheet_name="attendance", index=False
            )
            pd.DataFrame(nurses_df).to_excel(writer, sheet_name="nurses", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process different modes")

    parser.add_argument(
        "--mode",
        choices=["bq", "report"],
        required=True,
        help="Specify the operation mode (bq or report)",
    )

    args = parser.parse_args()
    dates = {}

    sync_bigquery = True if args.mode == "bq" else False

    if args.mode == "report":
        dates["end_date"] = datetime.now().date()
        dates["start_date"] = dates["end_date"] - timedelta(days=6)

    envs = init_envs()

    if args.mode == "bq":
        credentials = service_account.Credentials.from_service_account_file("sc.json")

        patient_training_df = pandas_gbq.read_gbq(
            f"SELECT MAX(date_of_session) AS max_session_date FROM `{envs['bigquery_dataset']}.patient_training_v1`",
            project_id="noorahealth-raw",
            credentials=credentials,
        )

        max_patient_session_date = patient_training_df["max_session_date"].iloc[0]

        nurse_training_df = pandas_gbq.read_gbq(
            f"SELECT MAX(sessiondateandtime) AS max_session_date FROM `{envs['bigquery_dataset']}.nurse_training_v1`;",
            project_id="noorahealth-raw",
            credentials=credentials,
        )

        max_nurse_session_date = nurse_training_df["max_session_date"].iloc[0]

        if not pd.isnull(max_nurse_session_date):
            dates["start_date"] = max_nurse_session_date

        if not pd.isnull(max_patient_session_date):
            dates["start_date"] = max_patient_session_date

        print(
            f"Automatically picked start_date as {dates['start_date']} based on existing data"
        )

    execute(dates["start_date"], dates["end_date"], envs, sync_bigquery=sync_bigquery)

    if args.mode == "report":
        if envs["slack_token"] != None and envs["slack_user_id"] != None:
            slack = WebClient(token=envs["slack_token"])
            file = open("data.xlsx", "rb")

            date_string = ""
            if dates["start_date"]:
                date_string += dates["start_date"].strftime("%d %b %Y")

            if dates["end_date"] and dates["start_date"] != dates["end_date"]:
                date_string += " - " + dates["end_date"].strftime("%d %b %Y")

            slack.files_upload(
                channels=f"@{envs["slack_user_id"]}",
                file=file,
                initial_comment=f"Here is the data for {date_string}",
                title=f"Data for {date_string}",
            )
