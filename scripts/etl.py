import pandas as pd
import os
from data_cleaning import clean_fraud_ds1, clean_fraud_ds2, clean_money_laundering_ds
from transformations import data_transform

# Define directories
RAW_DATA_DIR = "data/raw/"
CLEANED_DATA_DIR = "data/cleaned/"
TRANSFORMED_DATA_DIR = "data/transformed/"

# Define dataset filenames
datasets = {
    "fraud_ds1": "fraud_ds1.csv.gz",
    "fraud_ds2": "fraud_ds2.csv.gz",
    "money_laundering_ds": "money_laundering_ds.csv.gz",
}

def etl_process():
    """ETL process to clean, transform and save datasets."""
    # Ensure directories exist
    os.makedirs(CLEANED_DATA_DIR, exist_ok=True)
    os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)

    # Step 1: Clean Data
    print("Cleaning fraud_ds1...")
    clean_fraud_ds1(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds1"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds1"])
    )
    
    print("Cleaning fraud_ds2...")
    clean_fraud_ds2(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds2"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds2"])
    )
    
    print("Cleaning money_laundering_ds...")
    clean_money_laundering_ds(
        os.path.join(RAW_DATA_DIR, datasets["money_laundering_ds"]),
        os.path.join(CLEANED_DATA_DIR, datasets["money_laundering_ds"])
    )

    # Step 2: Verify Cleaned Data Exists
    for file in datasets.values():
        cleaned_file_path = os.path.join(CLEANED_DATA_DIR, file)
        if not os.path.exists(cleaned_file_path):
            raise FileNotFoundError(f"Cleaned file not found: {cleaned_file_path}")
    
    # Step 3: Transform Data
    print("Running data transformation...")
    data_transform()

if __name__ == "__main__":
    etl_process()
