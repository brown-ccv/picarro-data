"""Runs conversion and upload to Cloud Storage and Firestore."""

from picarro_data.upload_storage import upload_data
from picarro_data.upload_firestore import initialize, upload_df
from picarro_data.convert_dat import aggregate_df

import argparse
import datetime
from pathlib import Path

import logging

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
parser.add_argument("--archive", action="store_true")

def main():
    args = parser.parse_args()
    logger = logging.getLogger("picarro")

    if args.date:
        date = datetime.date.fromisoformat(args.date)
    else:  # if no date provided, use yesterday's date
        date = datetime.date.today() - datetime.timedelta(days=1)

    logfile = Path("logs", date.year, date.month, f"{date}.log")

    logfile.parent.mkdir(parents=True, exist_ok=True)

    directory = args.directory
    if args.archive:
        directory = Path(directory, date.year, date.month, date.day)

    logging.basicConfig(
        filename=logfile,
        encoding="utf-8",
        filemode="a",
        format="{asctime} - {levelname} - {message}",
        style="{",
        level=logging.INFO,
    )

    logger.info(f"Storage upload for {date}")
    df = upload_data(directory, date)
    logger.info(df.columns)

    app = initialize()
    logger.debug(app)

    try:
        df = aggregate_df(df)
    except Exception as e:
        logger.error(f"df aggregation failed: {e}")
        raise

    try:
        upload_df(app, df, date)
    except Exception as e:
        logger.error("Could not upload to firestore: {e}")
        raise

    logger.info("Upload complete")

if __name__ == "__main__":
    main()
