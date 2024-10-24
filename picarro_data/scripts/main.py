"""Runs conversion and upload to Cloud Storage and Firestore."""

import upload_storage
import upload_firestore
import convert_dat
import argparse
import datetime
from pathlib import Path

import logging

logger = logging.getLogger("picarro")

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
args = parser.parse_args()

if args.date:
    date = datetime.date.fromisoformat(args.date)
else:  # if no date provided, use yesterday's date
    date = datetime.date.today() - datetime.timedelta(days=1)

logfile = Path("logs", f"{date.year}", f"{date.month}", f"{date}.log")

logfile.parent.mkdir(parents=True, exist_ok=True)

directory = args.directory

logging.basicConfig(
    filename=logfile,
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    level=logging.INFO,
)

logger.info(f"Storage upload for {date}")
df = upload_storage.upload_data(directory, date)
logger.info(df.columns)

app = upload_firestore.initialize()
logger.debug(app)

try:
    df = convert_dat.aggregate_df(df)
except Exception as e:
    logger.error(f"df aggregation failed: {e}")
    raise

try:
    upload_firestore.upload_df(app, df, date)
except Exception as e:
    logger.error("Could not upload to firestore: {e}")
    raise

logger.info("Upload complete")
