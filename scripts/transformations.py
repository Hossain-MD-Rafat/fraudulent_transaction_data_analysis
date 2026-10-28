"""Transform cleaned datasets into Neo4j import tables.

Phase 2 design principles
-------------------------
1. Keep the three source datasets logically distinct; IDs are namespaced so
   unrelated datasets cannot accidentally collide.
2. Preserve every fraud transaction and every AML transfer as its own event.
3. Connect fraud_ds2 transactions to merchants even though that dataset does
   not contain customer/card identifiers or merchant coordinates.
4. Preserve AML currencies, payment format, banks, accounts, timestamp and
   laundering label on an AMLTransfer event node.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEANED_DATA_DIR = PROJECT_ROOT / "data" / "cleaned"
TRANSFORMED_DATA_DIR = PROJECT_ROOT / "data" / "transformed"

DATASETS = {
    "fraud_ds1": "fraud_ds1.csv.gz",
    "fraud_ds2": "fraud_ds2.csv.gz",
    "money_laundering_ds": "money_laundering_ds.csv.gz",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with normalized lowercase snake-case column names."""
    out = df.copy()
    out.columns = [
        col.strip().lower().replace(" ", "_").replace(".", "_")
        for col in out.columns
    ]
    return out


def _write(df: pd.DataFrame, filename: str) -> None:
    TRANSFORMED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(TRANSFORMED_DATA_DIR / filename, index=False)


def _fraud_transaction_id(source: str, values: pd.Series) -> pd.Series:
    """Namespace transaction IDs so independent datasets cannot collide."""
    return source + ":" + values.astype(str)


def _merchant_id(source: str, values: pd.Series) -> pd.Series:
    """Namespace merchant IDs because source systems use different semantics."""
    return source + ":" + values.astype(str)


def data_transform() -> None:
    fraud_ds1 = normalize_columns(
        pd.read_csv(CLEANED_DATA_DIR / DATASETS["fraud_ds1"])
    )
    fraud_ds2 = normalize_columns(
        pd.read_csv(CLEANED_DATA_DIR / DATASETS["fraud_ds2"])
    )
    aml = normalize_columns(
        pd.read_csv(CLEANED_DATA_DIR / DATASETS["money_laundering_ds"])
    )

    # ------------------------------------------------------------------
    # Fraud / card-transaction graph
    # ------------------------------------------------------------------
    customers = (
        fraud_ds1[
            [
                "cc_num",
                "first",
                "last",
                "gender",
                "street",
                "city",
                "state",
                "zip",
                "job",
                "dob",
            ]
        ]
        .drop_duplicates(subset=["cc_num"])
        .rename(columns={"cc_num": "CustomerID"})
    )
    customers["CustomerID"] = "fraud_ds1:" + customers["CustomerID"].astype(str)
    _write(customers, "customers.csv.gz")

    transactions1 = fraud_ds1[
        ["trans_num", "trans_date_trans_time", "amt", "is_fraud"]
    ].copy()
    transactions1["TransactionID"] = _fraud_transaction_id(
        "fraud_ds1", transactions1["trans_num"]
    )
    transactions1 = transactions1.rename(
        columns={
            "trans_date_trans_time": "TransactionDate",
            "amt": "Amount",
            "is_fraud": "IsFraud",
        }
    )
    transactions1["SourceDataset"] = "fraud_ds1"
    transactions1 = transactions1[
        ["TransactionID", "TransactionDate", "Amount", "IsFraud", "SourceDataset"]
    ]

    transactions2 = fraud_ds2[
        ["transactionid", "transactiondate", "amount", "isfraud"]
    ].copy()
    transactions2["TransactionID"] = _fraud_transaction_id(
        "fraud_ds2", transactions2["transactionid"]
    )
    transactions2 = transactions2.rename(
        columns={
            "transactiondate": "TransactionDate",
            "amount": "Amount",
            "isfraud": "IsFraud",
        }
    )
    transactions2["SourceDataset"] = "fraud_ds2"
    transactions2 = transactions2[
        ["TransactionID", "TransactionDate", "Amount", "IsFraud", "SourceDataset"]
    ]

    transactions = pd.concat([transactions1, transactions2], ignore_index=True)
    transactions = transactions.drop_duplicates(subset=["TransactionID"])
    _write(transactions, "transactions.csv.gz")

    merchants1 = fraud_ds1[
        ["merchant", "category", "merch_lat", "merch_long"]
    ].drop_duplicates().copy()
    merchants1["MerchantID"] = _merchant_id("fraud_ds1", merchants1["merchant"])
    merchants1 = merchants1.rename(
        columns={
            "category": "Category",
            "merch_lat": "Latitude",
            "merch_long": "Longitude",
        }
    )
    merchants1["Location"] = pd.NA
    merchants1["SourceDataset"] = "fraud_ds1"

    merchants2 = fraud_ds2[
        ["merchantid", "transactiontype", "location"]
    ].drop_duplicates().copy()
    merchants2["MerchantID"] = _merchant_id("fraud_ds2", merchants2["merchantid"])
    merchants2 = merchants2.rename(
        columns={"transactiontype": "Category", "location": "Location"}
    )
    # fraud_ds2 legitimately has no merchant coordinates. Keep these rows
    # instead of dropping the merchants as the original transformation did.
    merchants2["Latitude"] = pd.NA
    merchants2["Longitude"] = pd.NA
    merchants2["SourceDataset"] = "fraud_ds2"

    merchants = pd.concat(
        [
            merchants1[
                [
                    "MerchantID",
                    "Category",
                    "Location",
                    "Latitude",
                    "Longitude",
                    "SourceDataset",
                ]
            ],
            merchants2[
                [
                    "MerchantID",
                    "Category",
                    "Location",
                    "Latitude",
                    "Longitude",
                    "SourceDataset",
                ]
            ],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["MerchantID"])
    _write(merchants, "merchants.csv.gz")

    customer_transactions = fraud_ds1[["cc_num", "trans_num"]].copy()
    customer_transactions["CustomerID"] = (
        "fraud_ds1:" + customer_transactions["cc_num"].astype(str)
    )
    customer_transactions["TransactionID"] = _fraud_transaction_id(
        "fraud_ds1", customer_transactions["trans_num"]
    )
    _write(
        customer_transactions[["CustomerID", "TransactionID"]],
        "customer_transactions.csv.gz",
    )

    transaction_merchants1 = fraud_ds1[["trans_num", "merchant"]].copy()
    transaction_merchants1["TransactionID"] = _fraud_transaction_id(
        "fraud_ds1", transaction_merchants1["trans_num"]
    )
    transaction_merchants1["MerchantID"] = _merchant_id(
        "fraud_ds1", transaction_merchants1["merchant"]
    )

    transaction_merchants2 = fraud_ds2[["transactionid", "merchantid"]].copy()
    transaction_merchants2["TransactionID"] = _fraud_transaction_id(
        "fraud_ds2", transaction_merchants2["transactionid"]
    )
    transaction_merchants2["MerchantID"] = _merchant_id(
        "fraud_ds2", transaction_merchants2["merchantid"]
    )

    transaction_merchants = pd.concat(
        [
            transaction_merchants1[["TransactionID", "MerchantID"]],
            transaction_merchants2[["TransactionID", "MerchantID"]],
        ],
        ignore_index=True,
    ).drop_duplicates()
    _write(transaction_merchants, "transaction_merchants.csv.gz")

    # ------------------------------------------------------------------
    # AML graph: preserve every row as an event node.
    # ------------------------------------------------------------------
    banks = pd.concat(
        [
            aml[["from_bank"]].rename(columns={"from_bank": "BankID"}),
            aml[["to_bank"]].rename(columns={"to_bank": "BankID"}),
        ],
        ignore_index=True,
    ).drop_duplicates()
    banks["BankID"] = banks["BankID"].astype(str)
    _write(banks, "banks.csv.gz")

    accounts = pd.concat(
        [
            aml[["account"]].rename(columns={"account": "AccountID"}),
            aml[["account_1"]].rename(columns={"account_1": "AccountID"}),
        ],
        ignore_index=True,
    ).drop_duplicates()
    accounts["AccountID"] = accounts["AccountID"].astype(str)
    _write(accounts, "accounts.csv.gz")

    # Stable event identity within this source snapshot. A row-level ID avoids
    # collapsing repeated transfers between the same account pair/timestamp.
    aml_events = aml.reset_index(drop=True).copy()
    aml_events.insert(
        0,
        "TransferID",
        [f"aml:{idx:08d}" for idx in range(len(aml_events))],
    )

    rename_map = {
        "timestamp": "Timestamp",
        "from_bank": "FromBank",
        "account": "FromAccount",
        "to_bank": "ToBank",
        "account_1": "ToAccount",
        "amount_received": "AmountReceived",
        "receiving_currency": "ReceivingCurrency",
        "amount_paid": "AmountPaid",
        "payment_currency": "PaymentCurrency",
        "payment_format": "PaymentFormat",
        "is_laundering": "IsLaundering",
    }
    aml_events = aml_events.rename(columns=rename_map)
    expected_aml_columns = [
        "TransferID",
        "Timestamp",
        "FromBank",
        "FromAccount",
        "ToBank",
        "ToAccount",
        "AmountReceived",
        "ReceivingCurrency",
        "AmountPaid",
        "PaymentCurrency",
        "PaymentFormat",
        "IsLaundering",
    ]
    missing = [column for column in expected_aml_columns if column not in aml_events]
    if missing:
        raise ValueError(f"AML dataset is missing required columns: {missing}")
    _write(aml_events[expected_aml_columns], "aml_transfers.csv.gz")

    account_banks = pd.concat(
        [
            aml[["account", "from_bank"]].rename(
                columns={"account": "AccountID", "from_bank": "BankID"}
            ),
            aml[["account_1", "to_bank"]].rename(
                columns={"account_1": "AccountID", "to_bank": "BankID"}
            ),
        ],
        ignore_index=True,
    ).drop_duplicates()
    account_banks["AccountID"] = account_banks["AccountID"].astype(str)
    account_banks["BankID"] = account_banks["BankID"].astype(str)
    _write(account_banks, "account_banks.csv.gz")

    print("Phase 2 graph import tables written to", TRANSFORMED_DATA_DIR)
    print(f"Fraud transactions preserved: {len(transactions):,}")
    print(f"Transaction→merchant edges: {len(transaction_merchants):,}")
    print(f"AML transfer events preserved: {len(aml_events):,}")


if __name__ == "__main__":
    data_transform()
