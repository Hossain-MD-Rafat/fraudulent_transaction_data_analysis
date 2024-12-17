import pandas as pd
import os

# Define input and output directories
CLEANED_DATA_DIR = "data/cleaned/"
TRANSFORMED_DATA_DIR = "data/transformed/"

# List of cleaned datasets
datasets = {
    "fraud_ds1": "fraud_ds1.csv.gz",
    "fraud_ds2": "fraud_ds2.csv.gz",
    "money_laundering_ds": "money_laundering_ds.csv.gz",
}

def normalize_columns(df):
    """Normalize column names to lowercase."""
    df.columns = df.columns.str.lower()
    return df

def data_transform():
    # Load and normalize column names for datasets
    fraud_ds1 = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds1"]))
    fraud_ds2 = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds2"]))
    money_laundering_ds = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["money_laundering_ds"]))

    fraud_ds1 = normalize_columns(fraud_ds1)
    fraud_ds2 = normalize_columns(fraud_ds2)
    money_laundering_ds = normalize_columns(money_laundering_ds)
    
    # 1. Customers (Nodes)
    customers = fraud_ds1[['cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'job', 'dob']].drop_duplicates()
    customers.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'customers.csv.gz'), index=False)

    # 2. Transactions (Nodes)
    transactions1 = fraud_ds1[['trans_num', 'trans_date_trans_time', 'amt', 'is_fraud']].drop_duplicates()
    transactions1.rename(columns={'trans_num': 'TransactionID', 
                                  'trans_date_trans_time': 'TransactionDate', 
                                  'amt': 'Amount', 
                                  'is_fraud': 'IsFraud'}, inplace=True)
    
    transactions2 = fraud_ds2[['transactionid', 'transactiondate', 'amount', 'isfraud']].drop_duplicates()
    transactions2.rename(columns={'transactionid': 'TransactionID', 
                                  'transactiondate': 'TransactionDate', 
                                  'amount': 'Amount', 
                                  'isfraud': 'IsFraud'}, inplace=True)

    transactions = pd.concat([transactions1, transactions2], ignore_index=True).drop_duplicates()
    transactions.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transactions.csv.gz'), index=False)

    # 3. Merchants (Nodes)
    merchants1 = fraud_ds1[['merchant', 'category', 'merch_lat', 'merch_long']].drop_duplicates()
    merchants1.rename(columns={'merchant': 'MerchantID', 
                               'category': 'Category', 
                               'merch_lat': 'Latitude', 
                               'merch_long': 'Longitude'}, inplace=True)

    merchants2 = fraud_ds2[['merchantid', 'transactiontype', 'location']].drop_duplicates()
    merchants2.rename(columns={'merchantid': 'MerchantID', 
                               'transactiontype': 'Category', 
                               'location': 'Location'}, inplace=True)

    merchants = pd.concat([merchants1, merchants2], ignore_index=True).drop_duplicates()
    merchants = merchants.dropna(subset=['Latitude', 'Longitude'], how='all')
    merchants['Location'] = merchants['Location'].fillna('unknown')
    merchants.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'merchants.csv.gz'), index=False)

    # 4. Banks (Nodes)
    banks = pd.concat([
        money_laundering_ds[['from_bank']].rename(columns={'from_bank': 'BankName'}),
        money_laundering_ds[['to_bank']].rename(columns={'to_bank': 'BankName'})
    ]).drop_duplicates()
    banks.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'banks.csv.gz'), index=False)

    # 5. Accounts (Nodes)
    accounts = pd.concat([
        money_laundering_ds[['account']].rename(columns={'account': 'AccountID'}),
        money_laundering_ds[['account_1']].rename(columns={'account_1': 'AccountID'})
    ]).drop_duplicates()
    accounts.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'accounts.csv.gz'), index=False)

    # 6. Relationships
    # Customer -> Transaction
    customer_transactions = fraud_ds1[['cc_num', 'trans_num']].rename(columns={'cc_num': 'CustomerID', 
                                                                               'trans_num': 'TransactionID'})
    customer_transactions.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'customer_transactions.csv.gz'), index=False)

    # Transaction -> Merchant
    transaction_merchants = fraud_ds1[['trans_num', 'merchant']].rename(columns={'trans_num': 'TransactionID', 
                                                                                 'merchant': 'MerchantID'})
    transaction_merchants.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transaction_merchants.csv.gz'), index=False)

    # Transaction -> Bank (Relationships)
    transaction_accounts = money_laundering_ds[['timestamp', 'from_bank', 'account', 'to_bank', 'account_1', 
                                                'amount_received', 'amount_paid', 'is_laundering']].rename(
        columns={
            'timestamp': 'Timestamp',
            'from_bank': 'FromBank',
            'to_bank': 'ToBank',
            'account': 'FromAccount',
            'account_1': 'ToAccount',
            'amount_received': 'AmountReceived',
            'amount_paid': 'AmountPaid'
        }
    )
    transaction_accounts.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transaction_accounts.csv.gz'), index=False)

    # Account -> Bank
    account_banks = pd.concat([
        money_laundering_ds[['account', 'from_bank']].rename(columns={'account': 'AccountID', 'from_bank': 'BankName'}),
        money_laundering_ds[['account_1', 'to_bank']].rename(columns={'account_1': 'AccountID', 'to_bank': 'BankName'})
    ]).drop_duplicates()
    account_banks.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'account_banks.csv.gz'), index=False)

# Main function
if __name__ == "__main__":
    data_transform()