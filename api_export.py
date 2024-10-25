import argparse
import datetime as dt
import hashlib
import json
import os
import oyaml as yaml
import uuid
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
import pandas_gbq
import requests
from google.oauth2.service_account import Credentials
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
        error_vals = ("Invalid or token expired", "Expired token")
        return data["result"] == "failed" and data["error"] in error_vals

    @property
    def headers(self):
        return {"Auth-Key": self.key, "Username": self.username}

    def get_patient_training(self, date: datetime):
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
            return self.get_patient_training(date)
        else:
            print(f"Fetched patient training sessions for date {date}")
            return data["data"] if data["result"] == "success" else []

    def get_nurse_training(self, date: datetime):
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
            return self.get_nurse_training(date)
        else:
            print(f"Fetched nurse training sessions for date {date}")
            return data["data"] if data["result"] == "success" else []

    def get_nurse_details(self, phone_number: str):
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
            return self.get_nurse_details(phone_number)
        else:
            print(f"Fetched nurse details for phone ending in {phone_number[-4:]}")
            return data["data"] if data["result"] == "success" else []


def check_keys(x, required_keys, name):
    xk = set({}) if x is None else x.keys()
    if required_keys > xk:
        missing_keys = required_keys.difference(xk)
        jk = ", ".join(missing_keys)
        raise Exception(f"The following {name} parameters were not found: {jk}")


def get_params(dest):
    github_ref_name = os.getenv("GITHUB_REF_NAME")
    params = {}

    if github_ref_name is None:
        with open(os.path.join("secrets", "api.yml")) as f:
            params["api"] = yaml.safe_load(f)
    else:
        params["api"] = yaml.safe_load(os.getenv("API_PARAMS"))

    if dest == "bigquery":
        with open(os.path.join("params", "bigquery.yml")) as f:
            params[dest] = yaml.safe_load(f)
        envir = "prod" if github_ref_name == "main" else "dev"
        y = [x for x in params[dest]["environments"] if x["name"] == envir][0]
        y["environment"] = y.pop("name")
        del params[dest]["environments"]
        params[dest].update(y)

        key = "service_account_key"
        if github_ref_name is None:
            key_path = os.path.join("secrets", params[dest][key])
            with open(key_path) as f:
                params[dest][key] = json.load(f)
        else:
            params[dest][key] = json.loads(os.getenv("BIGQUERY_SERVICE_ACCOUNT_KEY"))

    elif github_ref_name is None:
        with open(os.path.join("secrets", "slack.yml")) as f:
            params[dest] = yaml.safe_load(f)
    else:
        params[dest] = yaml.safe_load(os.getenv("SLACK_PARAMS"))

    check_keys(params["api"], {"url", "username", "password"}, "api")
    if dest == "bigquery":
        check_keys(params[dest], {"project", "dataset", "service_account_key"}, dest)
    elif dest == "slack":
        params[dest]["is_test"] = github_ref_name == "main"
        check_keys(params[dest], {"token", "channel_id", "is_test"}, dest)

    params.update({"github_ref_name": github_ref_name})
    return params


def read_data_from_api(params, dates):
    report = Report(params["url"], params["username"], params["password"])
    report.login()

    with Pool(8) as p:
        patient_trainings_nested = p.map(
            report.get_patient_training,
            [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
        )
    patient_trainings = [x2 for x1 in patient_trainings_nested for x2 in x1]

    with Pool(8) as p:
        nurse_trainings_nested = p.map(
            report.get_nurse_training,
            [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
        )
    nurse_trainings = [x2 for x1 in nurse_trainings_nested for x2 in x1]

    if not patient_trainings:
        print("No patient training sessions found")
        return

    for x in patient_trainings:
        x["md5"] = dict_hash(x)

    for x in nurse_trainings:
        x["md5"] = dict_hash(x)

    phones_in_patient_trainings = [
        x2 for x1 in patient_trainings for x2 in x1["session_conducted_by"].split(",")
    ]

    phones_in_nurse_trainings = []
    for i in nurse_trainings:
        if i.get("trainerdata1"):
            for trainer in i["trainerdata1"]:
                phones_in_nurse_trainings.append(trainer["phone_no"])

        if i.get("traineesdata1"):
            for trainee in i["traineesdata1"]:
                phones_in_nurse_trainings.append(trainee["phone_no"])

    all_phones = list(set(phones_in_patient_trainings + phones_in_nurse_trainings))

    with Pool(8) as p:
        nurse_details_nested = p.map(report.get_nurse_details, all_phones)
    nurses = [x2 for x1 in nurse_details_nested for x2 in x1]

    nurses_df = pd.DataFrame(nurses)
    col = "user_created_dateandtime"
    if col in nurses_df.columns:
        nurses_df[col] = pd.to_datetime(nurses_df[col], format="%Y-%m-%d %H:%M:%S")

    patient_trainings_df = pd.DataFrame(patient_trainings)
    col_types = {
        "mothers_trained": "int",
        "family_members_trained": "int",
        "total_trained": "int",
        "data1": "str",
    }
    for col, typ in col_types.items():
        if col in patient_trainings_df.columns:
            patient_trainings_df[col] = patient_trainings_df[col].astype(typ)
    col = "date_of_session"
    patient_trainings_df[col] = pd.to_datetime(
        patient_trainings_df[col], format="%d-%m-%Y"
    )

    nurse_trainings_df = pd.DataFrame(nurse_trainings)
    col_types = {
        "trainerdata1": "str",
        "traineesdata1": "str",
        "totalmaster_trainer": "int",
        "total_trainees": "int",
    }
    for col, typ in col_types.items():
        if col in nurse_trainings_df.columns:
            nurse_trainings_df[col] = nurse_trainings_df[col].astype(typ)
    col = "sessiondateandtime"
    if col in nurse_trainings_df.columns:
        nurse_trainings_df[col] = pd.to_datetime(
            nurse_trainings_df[col], format="%d-%m-%Y")

    data_frames = {
        "nurses": nurses_df,
        "patient_training_sessions": patient_trainings_df,
        "nurse_training_sessions": nurse_trainings_df,
    }
    return data_frames


def get_table_exists(table_name, params, creds):
    q = f"select * from `{params['dataset']}.__TABLES__` where table_id = '{table_name}'"
    df = pandas_gbq.read_gbq(q, project_id=params["project"], credentials=creds)
    return df.shape[0] > 0


def get_dates_from_bigquery(params):
    creds = Credentials.from_service_account_info(params["service_account_key"])
    table_name = "patient_training_sessions"
    col_name = "date_of_session"
    dates = {"start": None, "end": datetime.now().date() - timedelta(days=1)}

    table_exists = get_table_exists(table_name, params, creds)
    if table_exists:
        q = f"select max({col_name}) as max_date from `{params['dataset']}.{table_name}`"
        df = pandas_gbq.read_gbq(q, project_id=params["project"], credentials=creds)
        if pd.notna(df["max_date"].iloc[0]):
            dates["start"] = df["max_date"].iloc[0] + timedelta(days=1)
    return dates


def get_date_label(dates):
    date_label = dates["start"].strftime("%d %b %Y")
    if dates["start"] != dates["end"]:
        date_label += " - " + dates["end"].strftime("%d %b %Y")
    return date_label


def get_output_filename(dates):
    for key in dates:
        dates[key] = dates[key].strftime("%Y%m%d")
    return f"data_{dates['start']}_{dates['end']}.xlsx"


def write_data_to_excel(data_frames, filepath="data.xlsx"):
    with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
        for key, df in sorted(data_frames.items()):
            pd.DataFrame(df).to_excel(writer, sheet=key, index=False)


def write_data_to_slack(params, data_frames, dates):
    date_label = get_date_label(dates)
    filename = get_output_filename(dates)
    write_data_to_excel(data_frames, filename)

    pre = "This is a test. " if params["is_test"] else ""

    slack = WebClient(token=params["token"])
    slack.files_upload_v2(
        channel=params["channel_id"],
        file=filename,
        initial_comment=f"{pre}Here are the data for {date_label}.",
        title=f"Data for {date_label}",
    )


def add_extracted_columns(df, extracted_at=None):
    if extracted_at is not None:
        df["_extracted_at"] = extracted_at
    df["_extracted_uuid"] = [str(uuid.uuid4()) for _ in range(len(df.index))]
    return df


def write_data_to_bigquery(params, data_frames):
    creds = Credentials.from_service_account_info(
        params["service_account_key"]
    )

    extracted_at = datetime.now(dt.timezone.utc).replace(microsecond=0)
    for key in data_frames:
        data_frames[key] = add_extracted_columns(data_frames[key], extracted_at)

    nurses_exists = get_table_exists("nurses", params, creds)
    if nurses_exists:
        nurses_old = pandas_gbq.read_gbq(
            f"select * from `{params['dataset']}.nurses`", project_id=params["project"],
            credentials=creds
        )
        # may be overkill, this keeps the latest data for each username
        nurses_concat = pd.concat([nurses_old, data_frames["nurses"]])
        data_frames["nurses"] = nurses_concat.groupby("username").tail(1)

    if_exists = {
        "nurses": "replace",
        # "nurses": "append", # alternate
        "patient_training_sessions": "append",
        "nurse_training_sessions": "append",
    }

    table_schemas = {
        "nurses": [
            {"type": "STRING", "name": "username", "mode": "REQUIRED"},
            {"type": "TIMESTAMP", "name": "user_created_dateandtime", "mode": "NULLABLE"},
            {"type": "TIMESTAMP", "name": "_extracted_at", "mode": "NULLABLE"},
        ],
        "patient_training_sessions": [
            {"type": "DATE", "name": "date_of_session", "mode": "NULLABLE"},
            {"type": "INT64", "name": "mothers_trained", "mode": "NULLABLE"},
            {"type": "INT64", "name": "family_members_trained", "mode": "NULLABLE"},
            {"type": "INT64", "name": "total_trained", "mode": "NULLABLE"},
            {"type": "TIMESTAMP", "name": "_extracted_at", "mode": "NULLABLE"},
        ],
        "nurse_training_sessions": [
            {"type": "INT64", "name": "total_trainees", "mode": "NULLABLE"},
            {"type": "INT64", "name": "totalmaster_trainer", "mode": "NULLABLE"},
            {"type": "DATE", "name": "sessiondateandtime", "mode": "NULLABLE"},
            {"type": "STRING", "name": "traineesdata1", "mode": "NULLABLE"},
            {"type": "STRING", "name": "trainerdata1", "mode": "NULLABLE"},
            {"type": "TIMESTAMP", "name": "_extracted_at", "mode": "NULLABLE"},
        ],
    }

    for table_name in sorted(table_schemas.keys()):
        pandas_gbq.to_gbq(
            data_frames[table_name],
            f"{params['dataset']}.{table_name}",
            project_id=params["project"],
            credentials=creds,
            if_exists=if_exists[table_name],
            table_schema=table_schemas[table_name],
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract and load for the Andhra Pradesh CCP API.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--dest",
        choices=["bigquery", "slack", "local"],
        default="local",
        help="Destination to write the data (bigquery, slack, or local)."
    )
    parser.add_argument(
        "--start-date",
        help="(Optional) Start date in DD-MM-YYYY format. Only used if --dest=local."
    )
    parser.add_argument(
        "--end-date",
        help="(Optional) End date in DD-MM-YYYY format. Only used if --dest=local."
    )

    args = parser.parse_args()
    return args


def get_dates(args, params):
    if args.dest == "bigquery":
        default_start_date = dt.date(2023, 1, 1)  # TODO: what should this be?
        overlap = timedelta(days=30)  # TODO: how far back can past data change?
        dates = get_dates_from_bigquery(params[args.dest])
        if dates["start"] is None:
            dates["start"] = default_start_date
        else:
            dates["start"] = max(default_start_date, dates["start"] - overlap)
    elif args.dest in ("slack", "local"):
        today = datetime.now().date()
        dates = {"start": today - timedelta(days=7), "end": today - timedelta(days=1)}

        if args.dest == "local" and args.start_date is not None:
            dates["start"] = datetime.strptime(args.start_date, "%d-%m-%Y").date()
        if args.dest == "local" and args.end_date is not None:
            dates["end"] = datetime.strptime(args.end_date, "%d-%m-%Y").date()

    if dates["start"] > dates["end"]:
        raise Exception("Start date cannot be later than end date.")
    if dates["end"] > datetime.now().date():
        raise Exception("End date cannot be later than today.")

    print(f"Will attempt to fetch data using start date "
          f"{dates['start']} and end date {dates['end']}.")
    return dates


if __name__ == "__main__":
    args = parse_args()
    params = get_params(args.dest)
    dates = get_dates(args, params)
    data = read_data_from_api(params["api"], dates)

    if data is not None:
        if args.dest == "bigquery":
            write_data_to_bigquery(params[args.dest], data)
        elif args.dest == "slack":
            write_data_to_slack(params[args.dest], data, dates)
        elif args.dest == "local":
            write_data_to_excel(data)
