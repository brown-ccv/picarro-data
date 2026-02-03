cd C:\Users\picarro\Documents\picarro-data\

# Upload to Cloud Storage
poetry run python -u C:\Users\picarro\Documents\picarro-data\scripts\main_storage.py C:\UserData\DataLog_User\2023\10\05 --date 2023-10-05 > C:\Users\picarro\Documents\picarro-data\logs\out.txt

# Upload to Firestore
poetry run python -u C:\Users\picarro\Documents\picarro-data\scripts\main_firestore.py C:\UserData\DataLog_User\2023\10\05 --date 2023-10-05 >> C:\Users\picarro\Documents\picarro-data\logs\out.txt
