# Run hourly Firestore upload
# This script uploads the current hour's data to Firestore
# Set this up in Windows Task Scheduler to run every hour

# Upload to Firestore for the current date and hour (automatic)
poetry run python -u C:\Users\picarro\Documents\picarro-data\picarro-data\scripts\main_firestore.py C:\Picarro\G2000\Log\DataLogger\DataLog_User >> C:\Users\picarro\Documents\picarro-data\logs\firestore_hourly.txt
