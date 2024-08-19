import upload_storage
import upload_firestore
import convert_dat
import argparse
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
parser.add_argument("--archive", action="store_true")
parser.add_argument(
    "--threshold",
    help="What percent of full hour is needed in aggregation",
    default=0.5,
)
args = parser.parse_args()

if args.date:
    date = datetime.date.fromisoformat(args.date)
else:
    date = datetime.date.today()

df = upload_storage.upload_data(args.directory, date, args.archive)
upload_firestore.upload_df(
    upload_firestore.initialize(), convert_dat.aggregate_df(df), args.threshold, date
)
