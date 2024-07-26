import firebase_admin
from firebase_admin import credentials, firestore

def upload_df(data):
# initialize sdk
    cred = credentials.Certificate("serviceAccount.json")
    firebase_admin.initialize_app(cred)

    # initialize firestore instance
    db = firestore.client()

    db.collection('col').document('doc').set(data.to_pandas().to_dict(orient='list')) # : google.api_core.exceptions.InvalidArgument: 400 too many index entries for entity

