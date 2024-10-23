"""Allows download of data from Firestore."""

import argparse
import datetime
import os

import pandas as pd
from google.cloud import storage

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date in YYYY-MM-DD format")
parser.add_argument("--end", help="End date in YYY-MM-DD format (not inclusive)")
parser.add_argument(
    "-f", "--filepath", help="Where to store the downloaded data", default="data/"
)
args = parser.parse_args()

if args.end:
    dates = pd.date_range(start=args.date, end=args.end, freq="D")
else:
    dates = [datetime.date.fromisoformat(args.date)]


def download_many_blobs_with_transfer_manager(
    bucket_name, blob_names, destination_directory="", workers=1
):
    """Download blobs in a list by name, concurrently in a process pool.

    The filename of each blob once downloaded is derived from the blob name and
    the `destination_directory `parameter. For complete control of the filename
    of each blob, use transfer_manager.download_many() instead.

    Directories will be created automatically as needed to accommodate blob
    names that include slashes.
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The list of blob names to download. The names of each blobs will also
    # be the name of each destination file (use transfer_manager.download_many()
    # instead to control each destination file name). If there is a "/" in the
    # blob name, then corresponding directories will be created on download.
    # blob_names = ["myblob", "myblob2"]

    # The directory on your computer to which to download all of the files. This
    # string is prepended (with os.path.join()) to the name of each blob to form
    # the full path. Relative paths and absolute paths are both accepted. An
    # empty string means "the current working directory". Note that this
    # parameter allows accepts directory traversal ("../" etc.) and is not
    # intended for unsanitized end user input.
    # destination_directory = ""

    # The maximum number of processes to use for the operation. The performance
    # impact of this value depends on the use case, but smaller files usually
    # benefit from a higher number of processes. Each additional process occupies
    # some CPU and memory resources until finished. Threads can be used instead
    # of processes by passing `worker_type=transfer_manager.THREAD`.
    # workers=8

    from google.cloud.storage import Client, transfer_manager

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    results = transfer_manager.download_many_to_path(
        bucket,
        blob_names,
        destination_directory=destination_directory,
        max_workers=workers,
        worker_type=transfer_manager.THREAD,
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
            print(f"Removing {destination_directory + name}")
            os.remove(destination_directory + name)
        else:
            print("Downloaded {} to {}.".format(name, destination_directory + name))


def main():
    """Run the download function."""
    bucket_name = "hastings-picarro.appspot.com"
    blob_names = [
        f"{date.year}/{date.month:02}/{date.year}_{date.month:02}_{date.day:02}.zip"
        for date in dates
    ]
    download_many_blobs_with_transfer_manager(
        bucket_name, blob_names, destination_directory=args.filepath, workers=8
    )
