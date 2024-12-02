import argparse
import datetime as dt
import hashlib
import io
import json
import os
import oyaml as yaml
import polars as pl
import polars.selectors as cs
import requests
import uuid
import warnings
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from multiprocessing import get_context
from slack_sdk import WebClient
from xlsxwriter import Workbook


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
            json={"login": True, "username": self.username, "password": self.password})
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
        params[dest]["credentials"] = Credentials.from_service_account_info(
            params[dest][key])
        del params[dest][key]
    elif dest == "slack":
        params[dest]["is_test"] = github_ref_name == "main"
        check_keys(params[dest], {"token", "channel_id", "is_test"}, dest)

    params.update({"github_ref_name": github_ref_name})
    return params


def json_dumps_list(x):
    return json.dumps(x.to_list())


def read_data_from_api(params, dates):
    report = Report(params["url"], params["username"], params["password"])
    report.login()

    with get_context("spawn").Pool() as p:
        patient_trainings_nested = p.map(
            report.get_patient_training,
            [*dt_iterate(dates["start"], dates["end"], timedelta(days=1))],
        )
    patient_trainings = [x2 for x1 in patient_trainings_nested for x2 in x1]

    with get_context("spawn").Pool() as p:
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

    with get_context("spawn").Pool() as p:
        nurse_details_nested = p.map(report.get_nurse_details, all_phones)
    nurses = [x2 for x1 in nurse_details_nested for x2 in x1]

    nurses_df = pl.from_dicts(nurses)
    nurses_df = nurses_df.with_columns(
        cs.by_name("user_created_dateandtime", require_all=False)
        .str.to_datetime("%Y-%m-%d %H:%M:%S")
    )

    patient_trainings_df = pl.from_dicts(patient_trainings)
    int_cols = ["mothers_trained", "family_members_trained", "total_trained"]
    patient_trainings_df = (
        patient_trainings_df
        .with_columns(cs.by_name(int_cols, require_all=False).cast(pl.Int64))
        .with_columns(pl.col("date_of_session").str.to_date("%d-%m-%Y"))
        .with_columns(
            cs.by_name("data1", require_all=False)
            .map_elements(json_dumps_list, return_dtype=pl.String))
    )

    nurse_trainings_df = pl.from_dicts(nurse_trainings)
    int_cols = ["totalmaster_trainer", "total_trainees"]
    struct_cols = ["trainerdata1", "traineesdata1"]
    nurse_trainings_df = (
        nurse_trainings_df
        .with_columns(cs.by_name(int_cols, require_all=False).cast(pl.Int64))
        .with_columns(
            cs.by_name("sessiondateandtime", require_all=False).str.to_date("%d-%m-%Y"))
        .with_columns(
            cs.by_name(struct_cols, require_all=False)
            .map_elements(json_dumps_list, return_dtype=pl.String))
    )

    data_frames = {
        "nurses": nurses_df,
        "patient_training_sessions": patient_trainings_df,
        "nurse_training_sessions": nurse_trainings_df,
    }
    return data_frames


def get_bigquery_schema(df):
    n = "NUMERIC"
    i = "INT64"
    s = "STRING"
    dtype_mappings = {
        pl.Decimal: n, pl.Float32: n, pl.Float64: n,
        pl.Int8: i, pl.Int16: i, pl.Int32: i, pl.Int64: i,
        pl.UInt8: i, pl.UInt16: i, pl.UInt32: i, pl.UInt64: i,
        pl.String: s, pl.Categorical: s, pl.Enum: s, pl.Utf8: s,
        pl.Binary: "BOOLEAN", pl.Boolean: "BOOLEAN", pl.Date: "DATE",
        pl.Datetime("us", time_zone="UTC"): "TIMESTAMP",
        pl.Datetime("us", time_zone=None): "DATETIME",  # won't work for other time zones
        pl.Time: "TIME", pl.Duration: "INTERVAL"
    }
    schema = []
    for j in range(df.shape[1]):
        dtype = dtype_mappings[df.dtypes[j]]
        schema.append(bigquery.SchemaField(df.columns[j], dtype))
    return schema


def write_bigquery(df, table_name, params, write_disposition="WRITE_EMPTY"):
    schema = get_bigquery_schema(df)
    client = bigquery.Client(credentials=params["credentials"])
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream, destination=f"{params['dataset']}.{table_name}",
            project=params["project"],
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                schema=schema, write_disposition=write_disposition),
        )
    job.result()
    return df.shape[0]


def read_bigquery(query, credentials):
    with warnings.catch_warnings(action="ignore"):
        client = bigquery.Client(credentials=credentials)
        query_job = client.query(query)
        rows = query_job.result()
        df = pl.from_arrow(rows.to_arrow())
    return df


def read_bigquery_exists(table_name, params):
    q = f"select * from `{params['dataset']}.__TABLES__` where table_id = '{table_name}'"
    df = read_bigquery(q, params["credentials"])
    return df.shape[0] > 0


def get_dates_from_bigquery(params):
    table_name = "patient_training_sessions"
    col_name = "date_of_session"
    dates = {"start": None, "end": datetime.now().date() - timedelta(days=1)}

    if read_bigquery_exists(table_name, params):
        q = f"select max({col_name}) as max_date from `{params['dataset']}.{table_name}`"
        df = read_bigquery(q, params["credentials"])
        dates["start"] = df.item() + timedelta(days=1)
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
    with Workbook(filepath) as wb:
        for key, df in sorted(data_frames.items()):
            df.write_excel(workbook=wb, worksheet=key)


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
        df = df.with_columns(_extracted_at=pl.lit(extracted_at))
    uuids = [str(uuid.uuid4()) for _ in range(df.shape[0])]
    df = df.with_columns(pl.Series("_extracted_uuid", uuids))
    return df


def write_data_to_bigquery(params, data_frames):
    extracted_at = datetime.now(dt.timezone.utc).replace(microsecond=0)
    for key in data_frames:
        data_frames[key] = add_extracted_columns(data_frames[key], extracted_at)

    if read_bigquery_exists("nurses", params):
        # keep the latest data for each username
        nurses_old = read_bigquery(
            f"select * from `{params['dataset']}.nurses`", params["credentials"])
        nurses_concat = pl.concat(
            [nurses_old, data_frames["nurses"]], how="diagonal_relaxed")
        data_frames["nurses"] = nurses_concat.group_by("username").tail(1)

    write_dispositions = {
        "nurses": "WRITE_TRUNCATE",
        "patient_training_sessions": "WRITE_APPEND",
        "nurse_training_sessions": "WRITE_APPEND",
    }

    for table_name in sorted(data_frames.keys()):
        write_bigquery(
            data_frames[table_name], table_name, params, write_dispositions[table_name])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract and load for the CCP Andhra Pradesh API.",
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--dest",
        choices=["bigquery", "slack", "local"],
        default="local",
        help="Destination to write the data (bigquery, slack, or local).")
    parser.add_argument(
        "--start-date",
        help="(Optional) Start date in DD-MM-YYYY format. Only used if --dest=local.")
    parser.add_argument(
        "--end-date",
        help="(Optional) End date in DD-MM-YYYY format. Only used if --dest=local.")

    args = parser.parse_args()
    return args


def get_dates(args, params):
    if args.dest == "bigquery":
        # earliest nurse training 2023-06-06, earliest patient training 2023-08-16
        default_start_date = dt.date(2023, 6, 1)
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

    print(f"Attempting to fetch data between {dates['start']} "
          f"and {dates['end']}, inclusive.")
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
