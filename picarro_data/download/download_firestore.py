"""Allows download of data from Firestore."""

import argparse
import datetime

import pandas as pd
from pathlib import Path
import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date in YYYY-MM format")
parser.add_argument("--end", help="End date in YYYY-MM format (not inclusive)")
parser.add_argument(
    "-f", "--filepath", help="Where to store the downloaded data", default="data/"
)
args = parser.parse_args()

if args.end:
    dates = pd.date_range(start=args.date, end=args.end, freq="ME")
else:
    dates = [datetime.date.fromisoformat(args.date + "-01")]


def main():
    """Downloads a dataframe from the firestore database."""
    # initialize sdk
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

    # initialize firestore instance
    db = firestore.client()

    all_days = []
    for date in dates:
        collection = (
            db.collection("picarro")
            .document(f"{date.year}")
            .collection(f"{date.month:02}")
        )

        docs = collection.stream()  # get all of the data in that collection

        dfs = []
        for doc in docs:
            dfs.append(doc.to_dict())

        all_days.append(pd.DataFrame.from_records(dfs))

    # make filepath if it doesn't exist
    Path(args.filepath).mkdir(parents=True, exist_ok=True)

    save_path = Path(args.filepath)
    save_path.mkdir(parents=True, exist_ok=True)

    save_filename = (
        f"data_{args.date}_{args.end}.csv" if args.end else f"data_{args.date}.csv"
    )

    pd.concat(all_days).to_csv(save_path / save_filename, index=False)


if __name__ == "__main__":
    main()
