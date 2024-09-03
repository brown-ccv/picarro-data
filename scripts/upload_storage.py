import firebase_admin  # type: ignore
from firebase_admin import credentials
import pathlib
import datetime
import convert_dat
import polars as pl


def init_bucket():
    """Creates bucket for storage"""
    cred = credentials.Certificate("serviceAccount.json")
    firebase_admin.initialize_app(
        cred, {"storageBucket": "hastings-picarro.appspot.com"}
    )


def upload_data(directory: str, today: datetime, archive: bool):
    """Uploads data to google cloud storage
    args:
        directory: directory where files to upload are stored
        today: date to upload
        archive: whether this is an upload of previous dates
    """
    # get filenames for upload
    if archive:
        # previous and next date to ensure all datapoints captured
        # might be able to do this a bit more efficiently with pathlib, *except* that the day before might start in a previous month and therefore previous dir
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)
        paths = [
            pathlib.Path(f"{directory}/{day.year}/{day.month:02}/{day.day:02}")
            for day in [yesterday, today, tomorrow]
            if pathlib.Path(
                f"{directory}/{day.year}/{day.month:02}/{day.day:02}"
            ).is_dir()
        ]
        filenames = []
        for path in paths:
            filenames += [filename for filename in pathlib.Path(path).iterdir()]

    else:
        filenames = pathlib.Path(directory).iterdir()

    # read all files
    dfs = []
    for filename in filenames:
        dfs.append(convert_dat.convert(filename))

    # strip out all the incorrect dates
    df = pl.concat(dfs)
    df = df.filter(pl.col("DATE") == f"{today.year}-{today.month:02}-{today.day:02}")

    # upload zip file to google cloud storage
    df.to_pandas().to_csv(
        f"gs://hastings-picarro.appspot.com/{today.year}/{today.month:02}/{today.day:02}.zip",
        index=False,
        compression={
            "method": "zip",
            "archive_name": f"{today.year}_{today.month:02}_{today.day:02}.csv",
        },
    )
    return df
