"""Runs conversion and upload to Cloud Storage and Firestore."""

import upload_storage
import upload_firestore
import convert_dat
import argparse
import datetime
from pathlib import WindowsPath
import sys

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
parser.add_argument("--archive", action="store_true")
args = parser.parse_args()

sys.stdout=open(WindowsPath("C:/Users", "picarro", "Documents", "picarro-data", "logs", "log.txt"), 'w')

if args.date:
    date = datetime.date.fromisoformat(args.date)
else:  # if no date provided, use yesterday's date
    date = datetime.date.today() - datetime.timedelta(days=1)

df = upload_storage.upload_data(args.directory, date, args.archive)

# upload_firestore.upload_df(
#     upload_firestore.initialize(), convert_dat.aggregate_df(df), date
# )
