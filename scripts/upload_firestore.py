import firebase_admin  # type: ignore
from firebase_admin import firestore
from polars import DataFrame


def initialize():
    """
    Initializes and returns the firestore database
    """
    # initialize sdk
    firebase_admin.initialize_app(options={"projectId": "hastings-picarro"})
    # initialize firestore instance
    return firestore.client()


def upload_df(db, data: DataFrame, date: str) -> None:
    """
    Uploads a dataframe into the firestore database
    args:
        db: firestore database
        data: dataframe with data to upload
        date: date when the data was generaged in YYYY-MM-DD format
    """
    datadict = dict(
        [(f"{d['hour']}:00", d) for d in data.to_pandas().to_dict(orient="records")]
    )

    for key, value in datadict.items():
        db.collection("picarro").document(f"{date.year}").collection(
            f"{date.month:02}"
        ).document(f"{date.day:02}_{key}").set(value)
