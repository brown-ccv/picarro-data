cd C:\Users\picarro\Documents\picarro-data\

# Upload to Cloud Storage
poetry run python -u C:\Users\picarro\Documents\picarro-data\picarro_data\scripts\main_storage.py C:\Picarro\G2000\Log\DataLogger\DataLog_User > C:\Users\picarro\Documents\picarro-data\logs\out.txt

# Upload to Firestore
poetry run python -u C:\Users\picarro\Documents\picarro-data\picarro_data\scripts\main_firestore.py C:\Picarro\G2000\Log\DataLogger\DataLog_User > C:\Users\picarro\Documents\picarro-data\logs\out.txt