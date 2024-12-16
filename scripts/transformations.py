import pandas as pd
import os
#from geopy.geocoders import Nominatim

# Define input and output directories
CLEANED_DATA_DIR = "data/cleaned/"
TRANSFORMED_DATA_DIR = "data/transformed/"

# List of cleaned datasets
datasets = {
    "fraud_ds1": "fraud_ds1.csv",
    "fraud_ds2": "fraud_ds2.csv",
    "money_laundering_ds": "money_laundering_ds.csv",
}

fraud_ds1 = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds1"]))
fraud_ds2 = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["fraud_ds2"]))
money_laundering_ds = pd.read_csv(os.path.join(CLEANED_DATA_DIR, datasets["money_laundering_ds"]))

def data_transform():
    # 1. Customers (Nodes)
    customers = fraud_ds1[['cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'job', 'dob']].drop_duplicates()
    customers.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'customers.csv'), index=False)

    # 2. Transactions (Nodes) - From fraud_ds
    transactions1 = fraud_ds1[['trans_num', 'trans_date_trans_time', 'amt', 'is_fraud']].drop_duplicates()
    transactions1.rename(columns={'trans_num': 'TransactionID', 'trans_date_trans_time': 'TransactionDate', 'amt': 'Amount', 'is_fraud': 'IsFraud'}, inplace=True)
    
    # Transactions (Nodes) - From fraud_ds2
    transactions2 = fraud_ds2[['transactionid', 'transactiondate', 'amount', 'isfraud']].drop_duplicates()

    # Combine Transactions from both datasets
    transactions = pd.concat([transactions1, transactions2], ignore_index=True).drop_duplicates()
    transactions.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transactions.csv'), index=False)

    # 3. Merchants (Nodes)

    merchants1 = fraud_ds1[['merchant', 'category', 'merch_lat', 'merch_long']].drop_duplicates()
    merchants1.rename(columns={'merchant': 'MerchantID', 'category': 'Category', 'merch_lat': 'Latitude', 'merch_long': 'Longitude'}, inplace=True)

    merchants2 = fraud_ds2[['merchantid', 'transactiontype', 'location']].drop_duplicates()
    merchants2.rename(columns={'merchantid': 'MerchantID', 'transactiontype': 'Category'}, inplace=True)
    
    # Combine Merchants
    merchants = pd.concat([merchants1, merchants2], ignore_index=True).drop_duplicates()
    merchants = merchants.dropna(subset=['Latitude', 'Longitude'])
    merchants['location'] = merchants['location'].fillna('unknown')
    merchants.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'merchants.csv'), index=False)

    # 4. Banks (Nodes)
    banks = pd.concat([
        money_laundering_ds[['from_bank']].rename(columns={'from_bank': 'BankName'}),
        money_laundering_ds[['to_bank']].rename(columns={'to_bank': 'BankName'})
    ]).drop_duplicates()
    banks.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'banks.csv'), index=False)

    # 5. Accounts (Nodes)
    accounts = pd.concat([
        money_laundering_ds[['account']].rename(columns={'account': 'AccountID'}),
        money_laundering_ds[['account_1']].rename(columns={'account_1': 'AccountID'})
    ]).drop_duplicates()
    accounts.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'accounts.csv'), index=False)

    # 6. Relationships

    # Customer -> Transaction (Relationships)
    customer_transactions = fraud_ds1[['cc_num', 'trans_num']].rename(columns={'cc_num': 'CustomerID', 'trans_num': 'TransactionID'})
    customer_transactions.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'customer_transactions.csv'), index=False)

    # Transaction -> Merchant (Relationships)
    transaction_merchants = fraud_ds1[['trans_num', 'merchant']].rename(columns={'trans_num': 'TransactionID', 'merchant': 'MerchantID'})
    transaction_merchants.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transaction_merchants.csv'), index=False)

    # Transaction -> Bank (Relationships)
    transaction_accounts = money_laundering_ds[['timestamp', 'from_bank', 'account', 'to_bank', 'account_1', 'amount_paid', 'amount_received', 'is_laundering']].rename(
        columns={
            'timestamp': 'Timestamp',
            'account': 'FromAccount',
            'account_1': 'ToAccount',
            'amount_paid': 'AmountPaid',
            'amount_received': 'AmountReceived'
        }
    )
    transaction_accounts.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'transaction_accounts.csv'), index=False)

    # Account -> Bank (Relationships)
    account_banks = pd.concat([
        money_laundering_ds[['account', 'from_bank']].rename(columns={'Account': 'AccountID'}),
        money_laundering_ds[['account_1', 'to_bank']].rename(columns={'Account.1': 'AccountID'})
    ]).drop_duplicates()
    account_banks.to_csv(os.path.join(TRANSFORMED_DATA_DIR, 'account_banks.csv'), index=False)

# main function
if __name__ == "__main__":
    data_transform()
