"""Runs upload to Firestore."""

import upload_firestore
import convert_dat
import concat_dat
import argparse
import datetime
from pathlib import Path
import logging
import polars as pl

logger = logging.getLogger("picarro")

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="Directory path")
args = parser.parse_args()

# use current date and hour for hourly uploads
date = datetime.date.today()
hour = datetime.datetime.now().hour

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

logger.info(f"Firestore upload for {date} hour {hour}")

app = upload_firestore.initialize()
logger.debug(app)

try:
    # Read and concatenate .dat files
    df = concat_dat.concat_dat_files(directory)
    
    # Filter by date
    df = df.filter(pl.col("DATE") == f"{date.year}-{date.month:02}-{date.day:02}")
    
    # Aggregate the data for the specified hour
    df = convert_dat.aggregate_df(df, hour=hour)
except Exception as e:
    logger.error(f"df aggregation failed: {e}")
    raise

try:
    upload_firestore.upload_df(app, df, date)
except Exception as e:
    logger.error(f"Could not upload to firestore: {e}")
    raise

logger.info("Firestore upload complete")
