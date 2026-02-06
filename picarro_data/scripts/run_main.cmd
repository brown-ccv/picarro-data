REM Upload to Cloud Storage
PowerShell poetry run python C:\Users\picarro\Documents\picarro-data\picarro_data\scripts\main_storage.py C:\Picarro\G2000\Log\DataLogger\DataLog_Users

REM Upload to Firestore
PowerShell poetry run python C:\Users\picarro\Documents\picarro-data\picarro_data\scripts\main_firestore.py C:\Picarro\G2000\Log\DataLogger\DataLog_Users