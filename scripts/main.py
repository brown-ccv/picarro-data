"""Runs conversion and upload to Cloud Storage and Firestore."""

import upload_storage
import upload_firestore
import convert_dat
import argparse
import datetime
from pathlib import WindowsPath, Path

import logging
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
parser.add_argument("--archive", action="store_true")
args = parser.parse_args()

if args.date:
    date = datetime.date.fromisoformat(args.date)
else:  # if no date provided, use yesterday's date
    date = datetime.date.today() - datetime.timedelta(days=1)
    
try:    
    logfile = WindowsPath("C:/Users", "picarro", "Documents", "picarro-data", "logs", f"{date}.log")
except NotImplementedError:
    logfile = Path("./log.txt")

logging.basicConfig(
    filename=logfile,
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    level=logging.INFO
)

logging.info(f"Storage upload for {date}")
df = upload_storage.upload_data(args.directory, date, args.archive)
logging.info(df.columns)

app = upload_firestore.initialize()
logging.debug(app)

try:
    df = convert_dat.aggregate_df(df, args.archive)
except Exception as e:
    logging.error(f"df aggregation failed: {e}")
    raise

try:
    upload_firestore.upload_df(
        app, df, date
    )
except Exception as e:
    logging.error("Could not upload to firestore: {e}")
    raise
    
logging.info("Upload complete")
