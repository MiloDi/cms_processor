import re
import pandas as pd
import json
import requests
import io
import concurrent.futures
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

"""
Requirements: 

    Given the CMS provider data metastore, write a script that downloads all data sets related to the theme "Hospitals". 

Conditions:

    The column names in the csv headers are currently in mixed case with spaces and special characters. Convert all column names to snake_case.
    The csv files should be downloaded and processed in parallel, and the job should be designed to run every day,
    but only download files that have been modified since the previous run (need to track runs/metadata). 
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

"""

class CMSDataProcessor:
    def __init__(self, base_url: str, output_dir: str, metadata_file: str):
        """
        Initialize the CMS Data Processor.
        
        Args:
            base_url: Base URL for the CMS API
            output_dir: Directory to store downloaded files
            metadata_file: File to store run metadata
        """
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.metadata_file = Path(metadata_file)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load previous run metadata if it exists
        self.previous_runs = self.load_metadata()


    def load_metadata(self) -> Dict:
        """ Load metadata from previous runs. """
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {"last_run": None, "processed_files": {}}

    def save_metadata(self)  -> None:
        """ Save current run metadata. """
        with open(self.metadata_file, 'w') as f:
            json.dump(self.previous_runs, f, indent=2)

    def convert_columns_to_snake_case(self, cols: list) -> list:
        """
         Remove special characters convert to snake case
        
        Args:
            cols: a list of dataframe columns
        """   
        return [re.sub(r'[^\w\s]', '', c).strip().lower().replace(' ', '_') for c in cols]

    
    def logger(self, message: str, log_file : str) -> None:
        """
        Basic logging function.
        
        Args:
            message: Output string for log file
            log_file: Name of file for success, failure, or other
        """   
        with open(log_file, "a") as f:
            f.write(f"{datetime.now()} - {message}\n")

    def connect(self, url: str) -> requests.Response: 
        """
        Connect to the url and log issues.
        
        Args:
            url: Connection address
        """      
        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
             self.logger(message = f"Failed to connect to {url}, with exception {e}",  log_file="errors.log")

        return response

    def get_hospital_datasets(self) -> List[Dict]:
        """ Download all data sets related to the theme 'Hospitals' """
        try:
            response = self.connect(f"{self.base_url}")
            datasets = response.json()
            hospital_datasets = [
                dataset for dataset in datasets
                if 'Hospitals' in dataset.get('theme', [])
            ]
            return hospital_datasets
            
        except Exception as e:
            self.logger(message = f"Failed to indentify Hospital theme, with exception {e}",  log_file="errors.log")
            return [{}]
    
    def process_async_requests(self, max_workers: int = 4)  -> None:
        """
        Process all hospital datasets in parallel.
        
        Args:
            max_workers: Maximum number of parallel workers
        """
        datasets = self.get_hospital_datasets()
        processed_dfs = []

        # Run asynchronous requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_dataset = {
                executor.submit(self.process_hospital_data, dataset): dataset
                for dataset in datasets
            }
            
            for future in concurrent.futures.as_completed(future_to_dataset):
                dataset = future_to_dataset[future]
                try:
                    df = future.result()
                    if df is not None:
                        processed_dfs.append(df)
                except Exception as e:
                    self.logger(message=f"Error processing {dataset['identifier']}: {str(e)}",  log_file="errors.log")

        self.previous_runs['last_run'] = datetime.now().isoformat()
        self.save_metadata()

        return processed_dfs
        
    
    def process_hospital_data(self, dataset: Dict) -> Optional[pd.DataFrame]:
        """
        Process a single dataset.
        
        Args:
            dataset: Dictionary containing dataset metadata
            
        Returns:
            Processed DataFrame or None if no updates needed
        """
        try:
            dataset_id = dataset['identifier']
            last_modified = dataset['modified']
            
            # Check to see if we have processed this data already
            if dataset_id in self.previous_runs['processed_files']: 
               if self.previous_runs['processed_files'][dataset_id] >= last_modified:
                self.logger(message = f"Dataset {dataset_id} is up to date, skipping",  log_file="success.log")
                return None
            
            # Extract and connect to the download url
            download_url = dataset.get('distribution', [{}])[0].get('downloadURL')
            if download_url:
                response = self.connect(download_url)
            else:
                self.logger(message = f"Failed to get download url for {dataset_id}", log_file= 'errors.log')

            # Read downloaded hospital csv into pandas dataframe
            df = pd.read_csv(io.StringIO(response.text), skip_blank_lines=True, quotechar='"', low_memory=False)
            df.columns = self.convert_columns_to_snake_case(df.columns)

            #Save data
            output_file = self.output_dir / f"{dataset_id}.csv"
            df.to_csv(output_file, index=False)

            # Update metadata
            self.previous_runs['processed_files'][dataset_id] = last_modified
            self.logger(message = f"Successfully read csv for {dataset_id}", log_file="success.log")
            
            return df
            
        except Exception as e:
            self.logger(message = f"Failed to read csv for {dataset_id}", log_file="errors.log")
            return None


def main():
    # Configuration
    BASE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
    OUTPUT_DIR = "hospital_data"
    METADATA_FILE = "run_metadata.json"
    
    # Initialize and run processor
    processor = CMSDataProcessor(BASE_URL, OUTPUT_DIR, METADATA_FILE)
    processor.process_async_requests()
    

if __name__ == "__main__":
    main()