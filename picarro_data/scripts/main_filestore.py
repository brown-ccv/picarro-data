"""Runs upload to Firestore only."""

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
else:
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

logger.info(f"Firestore upload for {date}")

app = upload_firestore.initialize()
logger.debug(app)

try:
    df = convert_dat.aggregate_df(directory, date)
except Exception as e:
    logger.error(f"df aggregation failed: {e}")
    raise

try:
    upload_firestore.upload_df(app, df, date)
except Exception as e:
    logger.error(f"Could not upload to firestore: {e}")
    raise

logger.info("Firestore upload complete")
