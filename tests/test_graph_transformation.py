from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import transformations


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def test_phase2_graph_tables_preserve_events_and_references(tmp_path, monkeypatch):
    """Validate the Phase 2 graph export against the committed cleaned data."""
    monkeypatch.setattr(transformations, "TRANSFORMED_DATA_DIR", tmp_path)

    transformations.data_transform()

    transactions = _read(tmp_path / "transactions.csv.gz")
    merchants = _read(tmp_path / "merchants.csv.gz")
    customer_transactions = _read(tmp_path / "customer_transactions.csv.gz")
    transaction_merchants = _read(tmp_path / "transaction_merchants.csv.gz")
    aml_transfers = _read(tmp_path / "aml_transfers.csv.gz")
    accounts = _read(tmp_path / "accounts.csv.gz")
    banks = _read(tmp_path / "banks.csv.gz")
    account_banks = _read(tmp_path / "account_banks.csv.gz")

    # Dataset-level event preservation.
    assert len(transactions) == 40_360
    assert transactions["TransactionID"].is_unique
    assert len(transaction_merchants) == 40_360
    assert len(aml_transfers) == 21_060
    assert aml_transfers["TransferID"].is_unique

    # fraud_ds2 must remain connected to merchants even without coordinates.
    ds2_transactions = transactions.loc[
        transactions["SourceDataset"] == "fraud_ds2", "TransactionID"
    ]
    ds2_edges = transaction_merchants[
        transaction_merchants["TransactionID"].str.startswith("fraud_ds2:")
    ]
    assert len(ds2_transactions) == 34_060
    assert len(ds2_edges) == 34_060

    # Referential integrity for fraud graph edges.
    transaction_ids = set(transactions["TransactionID"])
    merchant_ids = set(merchants["MerchantID"])
    assert set(customer_transactions["TransactionID"]).issubset(transaction_ids)
    assert set(transaction_merchants["TransactionID"]).issubset(transaction_ids)
    assert set(transaction_merchants["MerchantID"]).issubset(merchant_ids)

    # Referential integrity for AML graph entities.
    account_ids = set(accounts["AccountID"].astype(str))
    bank_ids = set(banks["BankID"].astype(str))
    assert set(aml_transfers["FromAccount"].astype(str)).issubset(account_ids)
    assert set(aml_transfers["ToAccount"].astype(str)).issubset(account_ids)
    assert set(aml_transfers["FromBank"].astype(str)).issubset(bank_ids)
    assert set(aml_transfers["ToBank"].astype(str)).issubset(bank_ids)
    assert set(account_banks["AccountID"].astype(str)).issubset(account_ids)
    assert set(account_banks["BankID"].astype(str)).issubset(bank_ids)


def test_namespaces_prevent_cross_source_transaction_collisions(tmp_path, monkeypatch):
    monkeypatch.setattr(transformations, "TRANSFORMED_DATA_DIR", tmp_path)
    transformations.data_transform()

    transactions = _read(tmp_path / "transactions.csv.gz")
    ids = transactions["TransactionID"]

    assert ids.str.startswith(("fraud_ds1:", "fraud_ds2:")).all()
    assert ids.is_unique
