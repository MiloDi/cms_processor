# CMS Data Processor: Hospital Datasets

## Description
This Python script is designed to download, process, and manage datasets from the CMS provider data metastore related to the theme "Hospitals". It converts column names to snake_case, processes datasets in parallel, and tracks modifications to avoid re-downloading unchanged files.

## Requirements
- Python 3.x
- `pandas` library
- `requests` library
- `concurrent.futures` library
- `io` library

## Features
- Downloads datasets related to "Hospitals" from the CMS data metastore.
- Converts CSV column names from mixed case with spaces and special characters to snake_case.
- Processes multiple datasets in parallel using threads.
- Checks if datasets have been modified since the last run to avoid redundant downloads.
- Logs errors, successes, and skipped files.
- Saves processed CSV files locally and updates run metadata.

## Installation
1. Clone the repository:
   ```bash
   git clone <repository_url>
   ```

2. Navigate into the project directory:
``` bash
cd processor
```

3. Install the required dependencies:
```bash
pip install -r requirements.txt
```
## Configuration
- BASE_URL: Base URL for the CMS API endpoint.
- OUTPUT_DIR: Directory to store downloaded CSV files.
- METADATA_FILE: JSON file to store processing metadata.
Modify the BASE_URL, OUTPUT_DIR, and METADATA_FILE variables in the main() function based on your CMS data metastore endpoint.

## Usage
Run the script by executing the following command:
```bash
python process.py
```
The script will download datasets related to "Hospitals", process them, convert column names to snake_case, and save the files locally.

## Logging
Logs are saved to errors.log and success.log in the current directory.

## Output
Processed CSV files will be saved in the hospital_data directory.
run_metadata.json stores the last run date and processed files metadata.

## Notes
Ensure your environment meets the required dependencies.
The script does not handle API authentication and assumes the base URL requires no authentication.