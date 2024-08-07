import firebase_admin  # type: ignore
from firebase_admin import credentials
import os
import datetime
import convert_dat
import polars as pl


def init_bucket():
    cred = credentials.Certificate("serviceAccount.json")
    firebase_admin.initialize_app(
        cred, {"storageBucket": "hastings-picarro.appspot.com"}
    )


def list_files(path):
    return list(map(lambda x: os.path.join(os.path.abspath(path), x), os.listdir(path)))


def upload_data(directory: str, today: datetime, archive: bool):
    # get filenames for upload
    if archive:
        # previous and next date to ensure all datapoints captured
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)
        paths = [
            f"{directory}/{day.year}/{day.month:02}/{day.day:02}"
            for day in [yesterday, today, tomorrow]
        ]
        filenames = []
        for path in paths:
            try:
                filenames += list_files(path)
            except FileNotFoundError:
                pass
    else:
        filenames = list_files(directory)

    # read all files
    dfs = []
    for filename in filenames:
        dfs.append(convert_dat.convert(os.path.join(directory, filename)))

    # strip out all the incorrect dates
    df = pl.concat(dfs)
    df = df.filter(pl.col("DATE") == f"{today.year}-{today.month:02}-{today.day:02}")
    # df.to_pandas().to_csv(f"gs://hastings-picarro.appspot.com/{today.year}/{today.month:02}/{today.day:02}")
