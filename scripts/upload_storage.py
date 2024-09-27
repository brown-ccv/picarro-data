"""Uploads data to Firestore.

Initializes a firestore database and uploads the raw version of the base data.
"""

import firebase_admin  # type: ignore
from firebase_admin import credentials
from pathlib import Path
import datetime
import convert_dat
import polars as pl
import logging
from google.cloud import storage


def init_bucket():
    """Creates bucket for storage."""
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(
        cred, {"storageBucket": "hastings-picarro.appspot.com"}
    )


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition
    )

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def upload_data(directory: str, today: datetime, archive: bool):
    """Uploads data to google cloud storage.

    Args:
        directory: directory where files to upload are stored
        today: date to upload
        archive: whether this is an upload of previous dates
    """
    # get filenames for upload
    if archive:
        # need to get all filenames here
        try:
            filenames = [
                str(x)
                for x in Path.iterdir(
                    Path(directory)
                    / f"{today.year}"
                    / f"{today.month:02}"
                    / f"{today.day:02}"
                )
            ]
        except:
            filenames = [str(x) for x in Path.iterdir(Path(directory))]
        # logging.info("Uploading archive to google cloud storage")
        # upload_blob("hastings-picarro.appspot.com", filename, destination_name)
        dfs = []
        for filename in filenames:
            dfs.append(convert_dat.read_h5(filename))
        df = pl.concat(dfs)
    else:
        logging.info("Uploading recent files")
        filenames = Path(directory).iterdir()

        # read all files
        dfs = []
        for filename in filenames:
            if not filename.match("backup_copy"):
                dfs.append(convert_dat.convert(filename, archive=archive))

        # strip out all the incorrect dates
        try:
            df = pl.concat(dfs)
        except ValueError:
            logging.error("cannot concatenate empty dataframes")
            raise
        df = df.filter(
            pl.col("DATE") == f"{today.year}-{today.month:02}-{today.day:02}"
        )
    try:
        # upload zip file to google cloud storage
        logging.info("Uploading to google cloud storage")
        df.to_pandas().to_csv(
            f"gs://hastings-picarro.appspot.com/{today.year}/{today.month:02}/{today.year}_{today.month:02}_{today.day:02}.zip",
            index=False,
            compression={
                "method": "zip",
                "archive_name": f"{today.year}_{today.month:02}_{today.day:02}.csv",
            },
        )
    except Exception as e:
        logging.error(e)
        raise

    return df
