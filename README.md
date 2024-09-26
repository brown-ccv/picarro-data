# Picarro Data Upload and Download
This github repository contains all scripts for uploading and downloading data from Google Cloud Storage and Firestore

* The firestore database consists of *hourly aggregate* data for the Picarro machine,combined into `Year/Month` collections
* Google Cloud Storage holds the raw data with no aggregation

## Setting Up Your Machine
To run these scripts, you'll need the following:
* Python version 3.5 or greater. This may be pre-installed on your system; to check use `python3 --version`
* [Poetry](https://python-poetry.org/), a package manager for python. You can follow the instructions on their page to install

Once both are installed, clone this repository. In the repository directory, run `poetry install` to install all necessary packages.

After installing packages, use `poetry run gcloud init`. This will allow you to log in with your google firebase credentials so that you can access files.

## Uploading Data
The `scripts/` folder includes everything needed to upload data from the Picarro machine to the cloud, as described below. The files consist of:
* `main.py`: script runner. This handles all connections between the files and command-line arguments.
* `convert_dat.py`: converts data files into usable files for upload. Also contains aggregation scripts.
* `upload_storage.py`: uploads data to Google Cloud Storage
* `upload_firestore.py`: uploads aggregated data to Firestore

Scripts also print output to a log file. See the `logs` folder in the picarro data folder.

### Uploading recent data
Uploading data that has been printed in the past few days. Files consist of:
* `run_main.*`: these scripts may be run on the picarro machine through Powershell or the command line to upload the most recent day's data.
* To run older data that has not yet been archived, use `poetry run python <path_to_main.py> <path_to_data> --date <YYYY-MM-DD>`

### Uploading archived data
* `run_archive.ps1`: This script may be run to upload older data. Note: this file currently needs manual editing of the date to run!
* Alternately, you can use the `main.py` script as above, just add `--archive` to the end

### Automatic uploads
New data is uploaded daily at 04:00. This is set up in Windows Task Scheduler.

## Downloading data
Download scripts are found in the `download/` directory.
* `download_firestore.py` downloads the hourly aggregated data for one or more months. It accepts a start date, (optional) end date, and (optional) path to download location. It can be run with `poetry run python download_firestore.py <start_date as YYYY-MM> --end <end_date as YYYY-MM> -f <output_file_path>`
* `download_cloud.py` downloads the daily full date for one or more days. It accepts a start date, (optional) end date, and (optional) path to download location. It can be run with `poetry run python download_cloud.py <start_date as YYYY-MM-DD> --end <end_date as YYYY-MM-DD> -f <output_file_path>`. Please note: as of now, it will download an empty zip archive for days that do not exist
