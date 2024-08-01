import os
import argparse

import convert_dat
import upload_firestore

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date in YYYY-MM-DD format")
args = parser.parse_args()

year, month, day = args.date.split("-")
directory = f"/Users/sbessey/Documents/picarro/data/Data_Backup/2023DecToCurrent/UserData/DataLog_User/{year}/{month}/{day}/"
db = upload_firestore.initialize()

for filename in os.listdir(directory):
    df = convert_dat.convert(os.path.join(directory, filename))
    upload_firestore.upload_df(db, df, filename, args.date)
