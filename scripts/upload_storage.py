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

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def upload_data(directory: str, today: datetime, archive: bool):
    """Uploads data to google cloud storage.

    Args:
        directory: directory where files to upload are stored
        today: date to upload
        archive: whether this is an upload of previous dates
    """
    # get filenames for upload
    if archive:
        destination_name = f"{today.year}/{today.month:02}/{today.year}_{today.month:02}_{today.day:02}.zip"
        filename = str(Path(directory) / f"{today.year}" / f"{today.month:02}" / f"{today.day:02}" / "DataLog_Private_20240901_091137.zip")
        upload_blob("hastings-picarro.appspot.com", filename, destination_name)
        """
        logging.info("Uploading archive files")
        # previous and next date to ensure all datapoints captured
        # might be able to do this a bit more efficiently with pathlib, *except* that the day before might start in a previous month and therefore previous dir
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)
        paths = [
            Path(directory) / f"{day.year}" / f"{day.month:02}" / f"{day.day:02}"
            for day in [yesterday, today, tomorrow]
            if (
                Path(directory) / f"{day.year}" / f"{day.month:02}" / f"{day.day:02}"
            ).is_dir()
        ]
        logging.info(paths)

        filenames = []
        for path in paths:
            filenames += [filename for filename in Path(path).iterdir()]
      """

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
        
    if not archive:
        df = df.filter(pl.col("DATE") == f"{today.year}-{today.month:02}-{today.day:02}")

    logging.info("Uploading to google cloud storage")
    if archive:
        """
        storage_client = Client()
        bucket = storage_client.bucket("hastings-picarro.appspot.com")
        results = transfer_manager.upload_many_from_filenames(
            bucket, [str(filename) for filename in filenames], source_directory="", max_workers=8
        )
        for name, result in zip(filenames, results):
            # The results list is either `None` or an exception for each filename in
            # the input list, in order.

            if isinstance(result, Exception):
                logging.info("Failed to upload {} due to exception: {}".format(name, result))
            else:
                logging.info("Uploaded {} to {}.".format(name, bucket.name))"""
    else:
        try:
            # upload zip file to google cloud storage
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
