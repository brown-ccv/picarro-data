"""Uploads data to Firestore.

Initializes a firestore database and uploads an aggregated version of the base data.

Typical usage:
    db = initialize()
    upload_df(db, data, date)
"""

import firebase_admin  # type: ignore
from firebase_admin import firestore
import polars as pl
import datetime
import logging


def initialize():
    """Initializes and returns the firestore database."""
    # initialize sdk
    logging.info("Initializing firestore sdk")
    app = firebase_admin.initialize_app(options={"projectId": "hastings-picarro"})
    logging.debug(app)
    # initialize firestore instance
    return firestore.client()


def upload_df(db, data, date) -> None:
    """Uploads a dataframe into the firestore database.

    Args:
        db: firestore database
        data: dataframe with data to upload
        date: date when the data was generaged in YYYY-MM-DD format
    """
    logging.info("uploading dataframe")
    datadict = dict(
        [(f"{d['hour']}:00", d) for d in data.to_pandas().to_dict(orient="records")]
    )
    try:
        for key, value in datadict.items():
            db.collection("picarro").document(f"{date.year}").collection(
                f"{date.month:02}"
            ).document(f"{date.day:02}_{key}").set(value)
    except Exception as e:
        logging.error("Firestore upload failed: {e}")
