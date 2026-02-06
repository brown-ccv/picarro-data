# Uploading Data
The `scripts/` folder includes everything needed to upload data from the Picarro machine to the cloud, as described below. The files consist of:
* `main_storage.py`: script runner for Cloud Storage uploads. Handles raw data upload to Google Cloud Storage (daily).
* `main_firestore.py`: script runner for Firestore uploads. Handles aggregated data upload to Firestore (hourly).
* `convert_dat.py`: converts data files into usable files for upload. Also contains aggregation scripts.
* `upload_storage.py`: uploads data to Google Cloud Storage
* `upload_firestore.py`: uploads aggregated data to Firestore

Scripts also print output to a log file. See the `logs` folder in the picarro data folder.

## Automatic uploads
- **Cloud Storage**: Uploaded daily at 04:00 (set up in Windows Task Scheduler)
- **Firestore**: Uploaded hourly (every hour on the hour)
  - Set up in Windows Task Scheduler or cron to run `run_firestore_hourly.ps1` every hour
  - Automatically uploads the current hour's data

## Manual Upload
To manually trigger an upload for the current hour:

```bash
# Upload current hour's data to Firestore
poetry run python main_firestore.py C:\Picarro\G2000\Log\DataLogger\DataLog_User

# Upload current day's data to Cloud Storage
poetry run python main_storage.py C:\Picarro\G2000\Log\DataLogger\DataLog_User
```