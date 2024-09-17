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
            print(f"Fetched nurse details for phone {phone_number[:2]}{"X"*8}")
            return data["data"] if data["result"] == "success" else []


def check_params(dest, params):
    required_keys = {"github_ref_name", "api_url", "api_username", "api_password"}
    if dest == "bigquery":
        required_keys.update({
            f"bigquery_{x}" for x in ["project", "dataset", "service_account_key"]
        })
    else:
        required_keys.update({"slack_token", "slack_channel_id"})

    if required_keys > params.keys():
        missing_keys = required_keys.difference(params.keys())
        jk = ", ".join(missing_keys)
        raise Exception(
            f"dest is {dest}, but the following parameters were not found: {jk}"
        )


def get_params(dest):
    github_ref_name = os.getenv("GITHUB_REF_NAME")

    if github_ref_name is None:
        with open(os.path.join("secrets", "api.yml")) as f:
            params = yaml.safe_load(f)
    else:
        params = yaml.safe_load(os.getenv("API_PARAMS"))

    if dest == "bigquery":
        with open(os.path.join("params", "bigquery.yml")) as f:
            dest_params = yaml.safe_load(f)
        envir = "prod" if github_ref_name == "main" else "dev"
        y = [x for x in dest_params["environments"] if x["name"] == envir][0]
        y["environment"] = y.pop("name")
        del dest_params["environments"]
        dest_params.update(y)

        key = "service_account_key"
        if github_ref_name is None:
            key_path = os.path.join("secrets", dest_params[key])
            with open(key_path) as f:
                dest_params[key] = json.load(f)
        else:
            dest_params[key] = json.loads(os.getenv("BIGQUERY_SERVICE_ACCOUNT_KEY"))

    elif github_ref_name is None:
        with open(os.path.join("secrets", "slack.yml")) as f:
            dest_params = yaml.safe_load(f)
    else:
        dest_params = yaml.safe_load(os.getenv("SLACK_PARAMS"))

    params = {f"api_{key.lower()}": val for key, val in params.items()}
    params.update({"github_ref_name": github_ref_name})
    dest_params = {f"{dest}_{key.lower()}": val for key, val in dest_params.items()}
    params.update(dest_params)
    check_params(dest, params)
    return params


def read_data_from_api(params, dates):
    report = Report(params["api_url"], params["api_username"], params["api_password"])
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
    nurses_df[col] = pd.to_datetime(nurses_df[col], format="%Y-%m-%d %H:%M:%S")

    patient_trainings_df = pd.DataFrame(patient_trainings)
    col_types = {
        "mothers_trained": "int",
        "family_members_trained": "int",
        "total_trained": "int",
        "data1": "str",
    }
    for col, typ in col_types.items():
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
        nurse_trainings_df[col] = nurse_trainings_df[col].astype(typ)
    col = "sessiondateandtime"
    nurse_trainings_df[col] = pd.to_datetime(nurse_trainings_df[col], format="%d-%m-%Y")

    data_frames = {
        "nurses": nurses_df,
        "patient_training_sessions": patient_trainings_df,
        "nurse_training_sessions": nurse_trainings_df,
    }
    return data_frames


def get_dates_from_bigquery(params, default_start_date=dt.date(2024, 6, 1)):
    creds = Credentials.from_service_account_info(params["bigquery_service_account_key"])
    table_name = "patient_training_sessions"
    column_name = "date_of_session"

    q = (f"SELECT MAX({column_name}) AS max_date "
         f"FROM `{params['bigquery_dataset']}.{table_name}`")
    df = pandas_gbq.read_gbq(q, project_id=params["bigquery_project"], credentials=creds)

    dates = {"start": default_start_date,
             "end": datetime.now().date() - timedelta(days=1)}
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

    pre = "" if params["github_ref_name"] == "main" else "This is a test. "

    slack = WebClient(token=params["slack_token"])
    slack.files_upload_v2(
        channel=params["slack_channel_id"],
        file=filename,
        initial_comment=f"{pre}Here are the data for {date_label}.",
        title=f"Data for {date_label}",
    )


def add_extracted_columns(df, extracted_at=None):
    if extracted_at is not None:
        df["_extracted_at"] = extracted_at
    df["_extracted_uuid"] = [uuid.uuid4() for _ in range(len(df.index))]
    return df


def write_data_to_bigquery(params, data_frames):
    creds = Credentials.from_service_account_info(
        params["bigquery_service_account_key"]
    )

    extracted_at = datetime.now(dt.timezone.utc).replace(microsecond=0)
    for key in data_frames:
        data_frames[key] = add_extracted_columns(data_frames[key], extracted_at)

    nurses_old = pandas_gbq.read_gbq( # TODO: what if table doesn't exist
        f"`{params['bigquery_dataset']}.nurses`",
        # f"SELECT distinct username FROM `{params['bigquery_dataset']}.nurses`", # alternate
        project_id=params["bigquery_project"],
        credentials=creds,
    )
    if nurses_old is not None and nurses_old.shape[0] > 0:
        # may be overkill, this keeps the latest data for each username
        nurses_concat = pd.concat(nurses_old, data_frames["nurses"])
        data_frames["nurses"] = nurses_concat.group_by("username").tail(1)
        # alternate if nurses data never change
        # nurses_new = data_frames["nurses"].merge(
        #     nurses_old, on = "username", how = "left", indicator = True)
        # data_frames["nurses"] = nurses_new[nurses_new["_merge"] == "left_only"]

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
            f"{params['bigquery_dataset']}.{table_name}",
            project_id=params["bigquery_project"],
            credentials=creds,
            if_exists=if_exists[table_name],
            table_schema=table_schemas[table_name],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process different destinations")
    parser.add_argument(
        "--dest",
        choices=["bigquery", "slack"],
        required=True,
        help="Specify the destination (bigquery or slack)",
    )

    args = parser.parse_args()
    params = get_params(args.dest)

    # assumes data for previous dates never change
    if args.dest == "bigquery":
        # TODO: set a proper default start date
        dates = get_dates_from_bigquery(params)
    elif args.dest == "slack":
        today = datetime.now().date()
        dates = {"start": today - timedelta(days=7), "end": today - timedelta(days=1)}

    data = read_data_from_api(params, dates)

    if data is not None:
        if args.dest == "bigquery":
            write_data_to_bigquery(params, data)
        elif args.dest == "slack":
            write_data_to_slack(params, data, dates)
