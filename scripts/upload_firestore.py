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


def upload_df(db, data: DataFrame, filename: str, date: str) -> None:
    """
    Uploads a dataframe into the firestore database
    args:
        db: firestore database
        data: dataframe with data to upload
        filename: name of the file for collection naming
        date: date when the data was generaged in YYYY-MM-DD format
    """
    year, month, day = date.split("-")
    for i, frame in enumerate(data.iter_slices(n_rows=720)):
        datadict = dict(
            [
                (str(d["datetime"]), d)
                for d in frame.drop(["DATE", "TIME"])
                .to_pandas()
                .to_dict(orient="records")
            ]
        )
        db.collection("picarro").document(year).collection(month).document(
            day
        ).collection(f"{filename}").document(f"data_{i}").set(datadict)
