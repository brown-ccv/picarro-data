# Picarro Data Upload and Download
This github repository contains all scripts for uploading and downloading data from Google Cloud Storage and Firestore

* The firestore database consists of *hourly aggregate* data for the Picarro machine,combined into `Year/Month` collections
* Google Cloud Storage holds the raw data with no aggregation

## First time setup
### Installing scripts
To run these scripts, you'll need the following:
* Python version 3.10 or greater. This may be pre-installed on your system; to check use `python3 --version` (on windows, also check `python --version` and `py --version` if python3 doesn't work)

Once you've checked that you have the proper version of python installed, create and activate a virtual environment:

Mac:
```
python3 -m venv venv
source venv/bin/activate
```
Windows:
```
python3 -m venv .venv
source .venv\Scripts\activate.bat
```
note! The python executable might be different on your computer. If this doesn't work, you may need to use `python` or `py` instead of `python3`

After activating your virtual environment, you'll need to install the git repository:
```pip install git+https://github.com/brown-ccv/picarro-data.git```


### Logging in to Google Cloud
In order to upload and download files, you'll need to log in to Google Cloud. First, you'll need to be added to the project on firebase. Grace Berg, Ruby Ho, and Sam Bessey can add you.

There are two options:
1. Navigate to the Firebase console to generate a private key (Settings > Service Accounts).
2. Download the `serviceAccount.json` file to a convenient location.
3. Run `export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"` where `KEY_PATH` is the location you stored your file. Example: `export GOOGLE_APPLICATION_CREDENTIALS="/Users/<username>/Documents/picarro/serviceAccount.json"`

Alternately, run `gcloud auth application-default login --impersonate-service-account data-download@hastings-picarro.iam.gserviceaccount.com` and follow the steps to log in.

## Downloading data
Once you've installed the package and logged in, you can use the provided scripts to download data from the cloud. Once you've set up the first time, you will no longer need to do the steps above (logging in to google cloud, installing via pip). However, you will need to navigate to the directory you used before and reactivate your virtual environment via:
```
source venv/bin/activate
```
on Mac or 
```
source .venv\Scripts\activate.bat
```
on Windows.

### Downloading hourly aggregate data (by month)
To download the aggregate data, use `download_firestore [date] [options]`

For example, to download all data from August 2024 to your `Documents/data` folder, run:
```download_firestore 2024-08 -f Documents/data```

To get data for multiple months, use the `--end` flag. For example, to download data from both August and September 2023, run:
```download_firestore 2023-08 --end 2023-10```
Note that the end date is non-inclusive.

For more information on the script, run `download_firestore -h`

### Downloading daily data
To download unedited data from a given day, run `download_cloud [date] [options]`. To download data from August 1, 2024 to your `Documents/data` folder, run:
`download_cloud 2024-08-01 -f Documents/data`

To get data for multiple days, use the `--end` flag. For example, to download data from August 1st and 2nd, 2023, run:
```download_cloud 2023-08-01 --end 2023-08-03```
Note that the end data is non-inclusive.

Additionally, please be aware that this can be a lot of data; downloads may take time.

For more information on the script, run `download_cloud -h`
