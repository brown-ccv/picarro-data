# Uploading Data
The `scripts/` folder includes everything needed to upload data from the Picarro machine to the cloud, as described below. The files consist of:
* `main.py`: script runner. This handles all connections between the files and command-line arguments.
* `convert_dat.py`: converts data files into usable files for upload. Also contains aggregation scripts.
* `upload_storage.py`: uploads data to Google Cloud Storage
* `upload_firestore.py`: uploads aggregated data to Firestore

Scripts also print output to a log file. See the `logs` folder in the picarro data folder.

## Automatic uploads
New data is uploaded daily at 04:00. This is set up in Windows Task Scheduler on the computer connected to the Picarro machine.

## Uploading recent data
Uploading data that has been printed in the past few days. Files consist of:
* `run_main.*`: these scripts may be run on the picarro machine through Powershell or the command line to upload the most recent day's data.
* `poetry run python <path_to_main.py> <path_to_data> --date <YYYY-MM-DD>`
