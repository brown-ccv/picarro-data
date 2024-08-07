import upload_storage
import argparse
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
parser.add_argument("--date", help="Date in YYYY-MM-DD format")
parser.add_argument("--archive", action="store_true")
args = parser.parse_args()

if args.date:
    date = datetime.date.fromisoformat(args.date)
else:
    date = datetime.date.today()

upload_storage.upload_data(args.directory, date, args.archive)
