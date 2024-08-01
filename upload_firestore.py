import firebase_admin # type: ignore
from firebase_admin import credentials, firestore
from polars import DataFrame


def initialize():
    # initialize sdk
    cred = credentials.Certificate("serviceAccount.json")
    firebase_admin.initialize_app(cred)

    # initialize firestore instance
    return firestore.client()


def upload_df(db, data: DataFrame, filename: str, date: str) -> None:
    year, month, day = date.split("-")
    for i, frame in enumerate(data.iter_slices(n_rows=600)):
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
