import firebase_admin
from firebase_admin import credentials, firestore
from collections import namedtuple


def upload_df(data, filename):
    # initialize sdk
    cred = credentials.Certificate("serviceAccount.json")
    firebase_admin.initialize_app(cred)

    # initialize firestore instance
    db = firestore.client()

    for i, frame in enumerate(data.iter_slices(n_rows=2000)):
        year, month, day = frame["DATE"][0].split("-")
        # datadict = dict(
        #         [
        #             (str(d["datetime"]), d)
        #             for d in frame.drop(["DATE", "TIME"]).to_pandas().to_dict(orient="records")
        #         ]
        #     )
        datadict = (
            frame.drop(["DATE", "TIME"]).to_pandas().to_dict(orient="list")
        )  # WORKS, don't love the data display
        db.collection("picarro").document(year).collection(month).document(day).collection(f"{filename}").document(f"data_{i}").set(datadict)


# google.api_core.exceptions.InvalidArgument: 400 Document 'projects/hastings-picarro/databases/(default)/documents/col/doc' cannot be written because its size (1,780,951 bytes) exceeds the maximum allowed size of 1,048,576 bytes.
