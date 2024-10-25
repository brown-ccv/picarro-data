# Downloading data
These are the download scripts for the Picarro data.
* `download_firestore.py` downloads the hourly aggregated data for one or more months. It accepts a start date, (optional) end date, and (optional) path to download location. It can be run with `poetry run python download_firestore.py <start_date as YYYY-MM> --end <end_date as YYYY-MM> -f <output_file_path>`
* `download_cloud.py` downloads the daily full date for one or more days. It accepts a start date, (optional) end date, and (optional) path to download location. It can be run with `poetry run python download_cloud.py <start_date as YYYY-MM-DD> --end <end_date as YYYY-MM-DD> -f <output_file_path>`. Please note: as of now, it will download an empty zip archive for days that do not exist

