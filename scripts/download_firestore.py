"""Allows download of data from Firestore."""

import argparse

import pandas as pd
import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date in YYYY-MM-DD format")
parser.add_argument(
    "-f", "--filepath", help="Where to store the downloaded data", default="data/"
)
args = parser.parse_args()

year, month, day = args.date.split("-")

# initialize sdk
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred)

# initialize firestore instance
db = firestore.client()

collection = db.collection("picarro").document(year).collection(month)

docs = collection.stream()  # get all of the data in that collection

dfs = []
for doc in docs:
    dfs.append(doc.to_dict())

df = pd.DataFrame.from_records(dfs)

df.to_csv(f"{args.filepath}/data_{args.date}.csv", index=False)
