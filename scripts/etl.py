import pandas as pd
import os
from scripts.data_cleaning import clean_fraud_ds1, clean_fraud_ds2, clean_money_laundering_ds
from scripts.transformations import data_transform

# Define directories
RAW_DATA_DIR = "data/raw/"
CLEANED_DATA_DIR = "data/cleaned/"
TRANSFORMED_DATA_DIR = "data/transformed/"

# Define dataset filenames
datasets = {
    "fraud_ds": "fraud_ds.csv",
    "fraud_ds1": "fraud_ds1.csv",
    "money_laundering_ds": "money_laundering_ds.csv",
}

def etl_process():
    """ETL process to clean, transform and save datasets."""
    # Step 1: Clean Data
    clean_fraud_ds1(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds"])
    )
    
    clean_fraud_ds2(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds1"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds1"])
    )
    
    clean_money_laundering_ds(
        os.path.join(RAW_DATA_DIR, datasets["money_laundering_ds"]),
        os.path.join(CLEANED_DATA_DIR, datasets["money_laundering_ds"])
    )

    # Step 2: Transform Data
    data_transform()

if __name__ == "__main__":
    etl_process()
