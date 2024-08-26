import argparse
import datetime as dt
import hashlib
import json
import os
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
import requests
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
    }


def execute(start_date: datetime, end_date: datetime):
    envs = init_envs()
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

    if not attendances:
        print("No attendances found")
        return

    for attendance in attendances:
        attendance["md5"] = dict_hash(attendance)
        attendance.pop("data1")

    pd.DataFrame(attendances).to_csv("attendance.csv", index=False)

    nurse_phones = (a["session_conducted_by"].split(",") for a in attendances)
    nurse_phones = [*{n for ns in nurse_phones for n in ns if n}]

    with Pool(8) as p:
        nss = p.map(report.fetch_nurse_details, [p for p in nurse_phones])
    nurses = [n for ns in nss for n in ns]

    pd.DataFrame(nurses).to_csv("nurses.csv", index=False)

    with pd.ExcelWriter("data.xlsx", engine="xlsxwriter") as writer:
        pd.DataFrame(attendances).to_excel(writer, sheet_name="attendance", index=False)
        pd.DataFrame(nurses).to_excel(writer, sheet_name="nurses", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process start-date and end-date.")

    parser.add_argument(
        "--start-date", help="(Optional) Start date in DD-MM-YYYY format"
    )
    parser.add_argument("--end-date", help="(Optional) End date in DD-MM-YYYY format")

    args = parser.parse_args()
    dates = {}

    if args.start_date != None:
        dates["start_date"] = datetime.strptime(args.start_date, "%d-%m-%Y").date()
    else:
        dates["start_date"] = datetime.now().date()

    if args.end_date != None:
        dates["end_date"] = datetime.strptime(args.end_date, "%d-%m-%Y").date()
    else:
        dates["end_date"] = datetime.now().date()

    envs = init_envs()
    execute(dates["start_date"], dates["end_date"])

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
