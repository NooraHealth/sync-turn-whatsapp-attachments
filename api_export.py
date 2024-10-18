import argparse
import datetime as dt
import hashlib
import json
import logging
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
import pandas_gbq
import requests
from decouple import config
from google.oauth2 import service_account
from slack_sdk import WebClient


class LogFilter(logging.Filter):
    def filter(self, record):
        return record.name == "root"


def setup_logging(verbose=False):
    logger = logging.getLogger()
    logger.propagate = False
    logger.setLevel(logging.DEBUG if verbose else logging.WARN)
    sh = logging.StreamHandler()
    sh.addFilter(LogFilter())
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


logger = setup_logging()


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

        logger.debug(f"Login successful. Auth-Key: {data["Auth-Key"]}")
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
            logger.debug(f"Attendance data retrieved successfully for date: {date}")
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
            logger.debug(f"Nurse Training data retrieved successfully for date: {date}")
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
            logger.debug(
                f"Nurse profile retrieved successfully for number: {phone_number[:2]}{"X"*8}"
            )
            return data["data"] if data["result"] == "success" else []


def process_json(value):
    if value != None:
        value = value.replace("'", '"')
        return json.loads(value)
    return value


def init_envs():
    api_username = config("API_USERNAME")
    api_password = config("API_PASSWORD")
    api_url = config("API_URL")
    slack_token = config("SLACK_TOKEN", default=None)
    slack_user_id = config("SLACK_RECEIVER_ID", default=None)
    bq_dataset = config("BQ_DATASET_NAME", default=None)
    bq_key = config("BQ_KEY", cast=process_json, default=None)
    bq_key_file_path = config("BQ_KEY_FILE_PATH", default=None)
    bq_key_value = None

    if api_username == None:
        raise Exception(
            "API_CONFIGURATION_ERROR: Missing API_USERNAME environment variable"
        )

    if api_password == None:
        logger.error(
            "API_CONFIGURATION_ERROR: Missing API_PASSWORD environment variable"
        )
        sys.exit(0)

    if api_url == None:
        logger.error("API_CONFIGURATION_ERROR: Missing API_URL environment variable")
        sys.exit(0)

    if bq_dataset == None:
        logger.warning(
            "BIGQUERY_CONFIGURATION_ERROR: Missing BQ_DATASET_NAME environment variable"
        )

    if bq_key == None and bq_key_file_path == None:
        logger.warning(
            "BIGQUERY_CONFIGURATION_ERROR: Missing BQ_KEY or BQ_KEY_FILE_PATH environment variable"
        )
    else:
        if bq_key != None:
            bq_key_value = service_account.Credentials.from_service_account_info(bq_key)
        if bq_key_file_path != None:
            bq_key_value = service_account.Credentials.from_service_account_file(
                bq_key_file_path
            )

    if slack_token == None:
        logger.warning(
            "SLACK_CONFIGURATION_ERROR: Missing SLACK_TOKEN environment variable"
        )

    if slack_user_id == None:
        logger.warning(
            "SLACK_CONFIGURATION_ERROR: Missing SLACK_RECEIVER_ID environment variable"
        )

    return {
        "username": api_username,
        "password": api_password,
        "api_url": api_url,
        "slack_token": slack_token,
        "slack_user_id": slack_user_id,
        "bigquery_dataset": bq_dataset,
        "bigquery_key": bq_key_value,
    }


def write_to_bigquery(dataframes, envs: dict):
    pandas_gbq.to_gbq(
        dataframes["nurses"],
        f"{envs['bigquery_dataset']}.nurses_v1",
        project_id="noorahealth-raw",
        credentials=envs["bigquery_key"],
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
        dataframes["patient_training"],
        f"{envs['bigquery_dataset']}.patient_training_v1",
        project_id="noorahealth-raw",
        credentials=envs["bigquery_key"],
        if_exists="append",
        table_schema=[
            {"type": "DATE", "name": "date_of_session", "mode": "NULLABLE"},
            {"type": "INT64", "name": "mothers_trained", "mode": "NULLABLE"},
            {"type": "INT64", "name": "family_members_trained", "mode": "NULLABLE"},
            {"type": "INT64", "name": "total_trained", "mode": "NULLABLE"},
        ],
    )

    pandas_gbq.to_gbq(
        dataframes["nurse_training"],
        f"{envs['bigquery_dataset']}.nurse_training_v1",
        project_id="noorahealth-raw",
        credentials=envs["bigquery_key"],
        if_exists="append",
        table_schema=[
            {"type": "INT64", "name": "total_trainees", "mode": "NULLABLE"},
            {"type": "INT64", "name": "totalmaster_trainer", "mode": "NULLABLE"},
            {"type": "DATE", "name": "sessiondateandtime", "mode": "NULLABLE"},
            {"type": "STRING", "name": "traineesdata1", "mode": "NULLABLE"},
            {"type": "STRING", "name": "trainerdata1", "mode": "NULLABLE"},
        ],
    )


def write_to_file(dataframes, sheets):
    with pd.ExcelWriter("data.xlsx", engine="xlsxwriter") as writer:
        for sheet in sheets:
            pd.DataFrame(dataframes[sheet]).to_excel(
                writer, sheet_name=sheet, index=False
            )


def execute(
    start_date: datetime,
    end_date: datetime,
    envs: dict,
    mode: str,
    loaders=["patient_training", "nurse_training", "nurses"],
):
    username = envs["username"]
    password = envs["password"]
    api_url = envs["api_url"]
    report = Report(api_url, username, password)
    report.login()

    dataframes = {}
    patient_training_data = []
    nurse_training_data = []

    if "patient_training" in loaders:
        with Pool(8) as p:
            atts = p.map(
                report.extract_attendance,
                [*dt_iterate(start_date, end_date, timedelta(days=1))],
            )
        patient_training_data = [attendance for att in atts for attendance in att]

        for attendance in patient_training_data:
            attendance["md5"] = dict_hash(attendance)

        attendance_df = pd.DataFrame(patient_training_data)
        attendance_df["data1"] = attendance_df["data1"].astype(str)
        attendance_df["date_of_session"] = pd.to_datetime(
            attendance_df["date_of_session"], format="%d-%m-%Y"
        )
        attendance_df["mothers_trained"] = attendance_df["mothers_trained"].astype(int)
        attendance_df["family_members_trained"] = attendance_df[
            "family_members_trained"
        ].astype(int)
        attendance_df["total_trained"] = attendance_df["total_trained"].astype(int)
        attendance_df.sort_values("date_of_session", inplace=True)

        dataframes["patient_training"] = attendance_df

    if "nurse_training" in loaders:
        with Pool(8) as p:
            nurse_trainings = p.map(
                report.extract_nurse_training,
                [*dt_iterate(start_date, end_date, timedelta(days=1))],
            )
        nurse_training_data = [
            attendance for att in nurse_trainings for attendance in att
        ]

        for nurse_training in nurse_training_data:
            nurse_training["md5"] = dict_hash(nurse_training)

        nurse_training_df = pd.DataFrame(nurse_training_data)

        if 'trainerdata1' in nurse_training_df.columns:
            nurse_training_df["trainerdata1"] = nurse_training_df["trainerdata1"].astype(
                str
            )

        if 'traineesdata1' in nurse_training_df.columns:
            nurse_training_df["traineesdata1"] = nurse_training_df["traineesdata1"].astype(
                str
            )

        if "sessiondateandtime" in nurse_training_df.columns:
            nurse_training_df["sessiondateandtime"] = pd.to_datetime(
                nurse_training_df["sessiondateandtime"], format="%d-%m-%Y"
            )

        if "totalmaster_trainer" in nurse_training_df.columns:
            nurse_training_df["totalmaster_trainer"] = nurse_training_df[
                "totalmaster_trainer"
            ].astype(int)

        if "total_trainees" in nurse_training_df.columns:
            nurse_training_df["total_trainees"] = nurse_training_df[
                "total_trainees"
            ].astype(int)

        dataframes["nurse_training"] = nurse_training_df

    if "nurses" in loaders:
        phones_in_attendance = [
            item
            for attendee in patient_training_data
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

        if mode == "bq":
            existing_nurses = pandas_gbq.read_gbq(
                f"SELECT * FROM `{envs['bigquery_dataset']}.nurses_v1`",
                project_id="noorahealth-raw",
                credentials=envs["bigquery_key"],
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

        if "user_created_dateandtime" in nurses_df.columns:
            nurses_df["user_created_dateandtime"] = pd.to_datetime(
                nurses_df["user_created_dateandtime"], format="%Y-%m-%d %H:%M:%S"
            )

        nurses_df.sort_values("user_created_dateandtime", inplace=True)

        dataframes["nurses"] = nurses_df

    if mode == "bq":
        write_to_bigquery(dataframes, envs)
    elif mode == "report":
        write_to_file(dataframes, sheets=loaders)
    else:
        write_to_file(dataframes, sheets=loaders)


def parse_args():
    parser = argparse.ArgumentParser(
        description="AP CCP Data Exporter"
        "\nExport data from AP CCP API to various destinations: "
        "Local file, BigQuery database, or Slack report.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=["bq", "report", "normal"],
        default="normal",
        help="Specify the operation mode:\n"
        "- bq: Sync data to BigQuery\n"
        "- report: Generate an Excel report and send it via Slack\n"
        "- normal (default): Save data locally to a file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase verbosity (use -v for debug output)",
    )
    parser.add_argument(
        "--start-date",
        help="(Optional) Start date in DD-MM-YYYY format. This parameter is optional; if omitted, the default start date will be used.",
    )
    parser.add_argument("--end-date", help="(Optional) End date in DD-MM-YYYY format")
    args = parser.parse_args()

    if args.mode == "normal" and args.start_date == None and args.end_date == None:
        logger.error(
            "Please specify a mode (bq, report or normal) or pick a start date and end date"
        )
        sys.exit(0)
    logger.info(f"Mode: {args.mode}")

    return args


if __name__ == "__main__":
    args = parse_args()

    logger.info("Starting AP CCP Data Exporter")
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    envs = init_envs()
    dates = {}

    if args.mode == "report":
        dates["end_date"] = datetime.now().date()
        dates["start_date"] = dates["end_date"] - timedelta(days=6)
        loaders = ["patient_training", "nurses"]
    elif args.mode == "bq":
        patient_training_df = pandas_gbq.read_gbq(
            f"SELECT MAX(date_of_session) AS max_session_date FROM `{envs['bigquery_dataset']}.patient_training_v1`",
            project_id="noorahealth-raw",
            credentials=envs["bigquery_key"],
        )

        max_patient_session_date = patient_training_df["max_session_date"].iloc[0]

        nurse_training_df = pandas_gbq.read_gbq(
            f"SELECT MAX(sessiondateandtime) AS max_session_date FROM `{envs['bigquery_dataset']}.nurse_training_v1`;",
            project_id="noorahealth-raw",
            credentials=envs["bigquery_key"],
        )

        max_nurse_session_date = nurse_training_df["max_session_date"].iloc[0]

        if not pd.isnull(max_nurse_session_date):
            dates["start_date"] = max_nurse_session_date

        if not pd.isnull(max_patient_session_date):
            dates["start_date"] = max_patient_session_date

        dates["end_date"] = datetime.now().date()

        logger.info(
            f"Automatically picked start_date as {dates['start_date']} based on existing data in BigQuery dataset"
        )
        loaders = ["patient_training", "nurse_training", "nurses"]
    else:
        if args.end_date != None:
            dates["end_date"] = datetime.strptime(args.end_date, "%d-%m-%Y").date()
        if args.start_date != None:
            dates["start_date"] = datetime.strptime(args.start_date, "%d-%m-%Y").date()
        loaders = ["patient_training", "nurse_training", "nurses"]

    execute(
        dates["start_date"], dates["end_date"], envs, mode=args.mode, loaders=loaders
    )

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
            logger.info("Sent report to slack successfully")
