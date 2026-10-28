# Financial Fraud Intelligence Platform

A multi-stage fraud and anti-money-laundering project that combines transaction-level machine learning, behavioral analytics, and graph intelligence.

## Project goal

The project is being upgraded from a graph-analysis prototype into an industry-style fraud decision system with three phases:

1. **Phase 1 — Transaction Fraud Detection Foundation**
   - leakage-aware feature engineering
   - chronological train/validation/test evaluation
   - class-imbalance-aware models
   - fraud-focused metrics such as precision, recall, F1, PR-AUC, and ROC-AUC
2. **Phase 2 — Graph Fraud & AML Intelligence**
   - redesigned Neo4j transaction graph
   - account-network features
   - suspicious community / fraud-ring analysis
   - hybrid graph + ML risk features
3. **Phase 3 — Fraud Decision Platform**
   - risk score and decision engine
   - explainability
   - API and analyst dashboard
   - monitoring and reproducible deployment

## Current Phase 1 implementation

The Phase 1 branch introduces behavioral features for `fraud_ds1` that only use information available before the current transaction. This helps reduce target/data leakage and more closely matches how a real fraud system scores incoming transactions.

### Behavioral features

- transaction hour, weekday, month, weekend/night indicators
- customer age
- customer-to-merchant Haversine distance
- prior transaction count
- prior average and standard deviation of transaction amount
- amount relative to the customer's prior average
- prior-only amount z-score
- time since the previous transaction
- transaction velocity over the previous 1 hour and 24 hours
- prior merchant/category usage
- new-merchant and new-category indicators

### Baseline model

`scripts/train_fraud_baseline.py` trains a class-weighted Logistic Regression model with:

- chronological 70/15/15 train/validation/test split
- threshold tuning on validation data only
- median numeric imputation
- categorical one-hot encoding
- numeric scaling
- fraud-focused evaluation on the untouched test set
- saved predictions, metrics, and model artifact

The baseline intentionally starts with an interpretable model. Stronger tree-based models will be compared after the evaluation framework is established.

## Repository structure

```text
fraudulent_transaction_data_analysis/
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── transformed/
├── scripts/
│   ├── data_cleaning.py
│   ├── etl.py
│   ├── transformations.py
│   ├── graph_modeling.cypher
│   └── train_fraud_baseline.py
├── src/
│   └── fraud_detection/
│       ├── __init__.py
│       └── features.py
├── tests/
│   └── test_features.py
├── outputs/
├── requirements.txt
└── README.md
```

## Setup

```bash
python -m venv .venv
```

Activate the environment and install dependencies:

```bash
pip install -r requirements.txt
```

Run the baseline:

```bash
python scripts/train_fraud_baseline.py
```

Run tests:

```bash
pytest
```

## Phase 1 outputs

After training, generated files are written to:

```text
outputs/phase1/
├── fraud_baseline.joblib
├── metrics.json
└── test_predictions.csv
```

Generated artifacts are intentionally excluded from version control.

## Existing graph foundation

The repository already includes a Neo4j-oriented ETL and graph pipeline for customers, merchants, accounts, banks, transactions, and money-laundering transfers. Phase 2 will redesign the graph so every financial event is preserved as its own transaction node and can participate in network-level fraud analysis.

## Evaluation philosophy

Because fraud is highly imbalanced, model quality should not be judged primarily by accuracy. The project prioritizes:

- precision
- recall
- F1
- PR-AUC
- ROC-AUC
- threshold-dependent operational tradeoffs
- later: expected financial loss and analyst-review capacity

## Roadmap

- [x] Reproducible Python dependencies
- [x] Project `.gitignore`
- [x] Leakage-aware behavioral feature engineering
- [x] Chronological baseline training pipeline
- [x] Initial feature unit tests
- [ ] Run and record benchmark metrics
- [ ] Add Random Forest / XGBoost comparison
- [ ] Add cost-sensitive threshold selection
- [ ] Add SHAP explanations
- [ ] Redesign AML graph transaction model
- [ ] Add graph features and fraud-ring analysis
- [ ] Add risk-scoring API and analyst dashboard
