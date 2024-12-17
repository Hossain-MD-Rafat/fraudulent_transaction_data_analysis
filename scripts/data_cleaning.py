import pandas as pd
import os

# Define input and output directories
RAW_DATA_DIR = "data/raw/"
CLEANED_DATA_DIR = "data/cleaned/"

# List of datasets and their filenames
datasets = {
    "fraud_ds1": "fraud_ds1.csv.gz",
    "fraud_ds2": "fraud_ds2.csv.gz",
    "money_laundering_ds": "money_laundering_ds.csv.gz",
}


df1 = pd.read_csv(os.path.join(RAW_DATA_DIR, datasets["fraud_ds1"]))
df2 = pd.read_csv(os.path.join(RAW_DATA_DIR, datasets["fraud_ds2"]))
df3 = pd.read_csv(os.path.join(RAW_DATA_DIR, datasets["money_laundering_ds"]))


def clean_fraud_ds1(input_file, output_file):
    """Cleans fraud_ds1 dataset."""
    df = pd.read_csv(input_file)

    # Handle missing values
    df = df.dropna(how="all")  # Drop rows with all null values
    df = df.fillna({"merchant": "Unknown", "category": "Unknown", "job": "Unemployed"})  # Fill specific columns
    
    # Rename a column
    df = df.rename(columns={'Unnamed: 0': 'trans_id'})

    # Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"Cleaned fraud_ds1 saved to {output_file}")


def clean_fraud_ds2(input_file, output_file):
    """Cleans fraud_ds2 dataset."""
    df = pd.read_csv(input_file)

    # Handle missing values
    df = df.dropna(how="all")
    df = df.fillna({"TransactionType": "Unknown", "Location": "Unknown"})

    # Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"Cleaned fraud_ds2 saved to {output_file}")


def clean_money_laundering_ds(input_file, output_file):
    """Cleans money_laundering_ds dataset."""
    df = pd.read_csv(input_file)

    # Handle missing values
    df = df.dropna(how="all")
    df = df.fillna({"Payment Format": "Unknown", "Receiving Currency": "USD", "Payment Currency": "USD"})

    # Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_").replace(".", "_") for col in df.columns]

    # Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"Cleaned money_laundering_ds saved to {output_file}")


def main():
    """Main function to clean all datasets."""
    os.makedirs(CLEANED_DATA_DIR, exist_ok=True)

    # Process each dataset
    clean_fraud_ds1(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds1"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds1"])
    )

    clean_fraud_ds2(
        os.path.join(RAW_DATA_DIR, datasets["fraud_ds2"]),
        os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds2"])
    )

    clean_money_laundering_ds(
        os.path.join(RAW_DATA_DIR, datasets["money_laundering_ds"]),
        os.path.join(CLEANED_DATA_DIR, datasets["money_laundering_ds"])
    )


if __name__ == "__main__":
    main()
