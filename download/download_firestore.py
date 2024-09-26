"""Allows download of data from Firestore."""

import argparse
import datetime

import pandas as pd
import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date in YYYY-MM format")
parser.add_argument("--end", help="End date in YYY-MM format (not inclusive)")
parser.add_argument(
    "-f", "--filepath", help="Where to store the downloaded data", default="data/"
)
args = parser.parse_args()

if args.end:
    dates = pd.date_range(start=args.date, end=args.end, freq="ME")
else:
    dates = datetime.date.fromisoformat(args.date)

print(dates)
# initialize sdk
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred)

# initialize firestore instance
db = firestore.client()

all_days = []
for date in dates:
    collection = db.collection("picarro").document(f"{date.year}").collection(f"{date.month:02}")

    docs = collection.stream()  # get all of the data in that collection

    dfs = []
    for doc in docs:
        dfs.append(doc.to_dict())
    print(dfs)

    all_days.append(pd.DataFrame.from_records(dfs))

pd.concat(all_days).to_csv(f"{args.filepath}/data_{args.date}.csv", index=False)
