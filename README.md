# Financial Fraud Intelligence Platform

A multi-stage fraud and anti-money-laundering project that combines transaction-level machine learning, behavioral analytics, cost-sensitive decisioning, explainability, and graph intelligence.

## Project goal

The project is being upgraded from a graph-analysis prototype into an industry-style fraud decision system with three phases:

1. **Phase 1 — Transaction Fraud Detection Foundation**
   - leakage-aware feature engineering
   - chronological train/validation/test evaluation
   - Logistic Regression, Random Forest, and XGBoost benchmarking
   - class-imbalance-aware learning
   - cost-sensitive threshold selection
   - risk score / expected fraud exposure / decision output
   - SHAP explainability
2. **Phase 2 — Graph Fraud & AML Intelligence**
   - redesigned Neo4j transaction graph
   - account-network features
   - suspicious community / fraud-ring analysis
   - hybrid graph + ML risk features
3. **Phase 3 — Fraud Decision Platform**
   - risk-scoring API
   - analyst investigation dashboard
   - monitoring and reproducible deployment
   - model and decision-policy observability

## Current Phase 1 implementation

The Phase 1 branch introduces behavioral features for `fraud_ds1` that only use information available before the current transaction. This reduces leakage and more closely matches how a real fraud system scores incoming events.

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

### Evaluation design

The project uses a chronological split:

```text
oldest data                                  newest data
|----------------|---------------|----------------|
     train 70%       validation 15%      test 15%
```

The validation set is used for model comparison and decision-threshold selection. The test set remains untouched until the model and operating threshold have been selected.

This is intentionally different from a random split because a production fraud model trains on past behavior and scores future transactions.

### Model benchmark

`scripts/benchmark_fraud_models.py` compares:

- class-weighted Logistic Regression
- class-balanced Random Forest
- XGBoost with positive-class weighting

The primary model-selection metric is validation **PR-AUC**, which is more informative than accuracy for highly imbalanced fraud data.

Each candidate model receives its own validation-only cost-sensitive threshold.

### Cost-sensitive fraud decisioning

A missed fraudulent transaction and an unnecessary analyst review do not have the same financial impact.

The Phase 1 decision layer models:

```text
false negative cost = missed fraudulent transaction amount × loss multiplier
false positive cost = manual-review cost
```

The defaults are intentionally configurable. They are project assumptions rather than claimed bank-specific economics.

For the final model, each transaction receives:

- fraud probability
- risk score from 0–100
- expected fraud loss (`probability × amount`)
- operational action: `APPROVE`, `REVIEW`, or `BLOCK`

### Explainability

`scripts/explain_fraud_model.py` generates global SHAP feature importance for the selected model so the project can answer not only **which transactions are risky**, but also **which behavioral signals drive model risk**.

## Repository structure

```text
fraudulent_transaction_data_analysis/
├── .github/
│   └── workflows/
│       └── tests.yml
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── transformed/
├── scripts/
│   ├── data_cleaning.py
│   ├── etl.py
│   ├── transformations.py
│   ├── graph_modeling.cypher
│   ├── train_fraud_baseline.py
│   ├── benchmark_fraud_models.py
│   └── explain_fraud_model.py
├── src/
│   └── fraud_detection/
│       ├── __init__.py
│       ├── features.py
│       └── risk.py
├── tests/
│   ├── test_features.py
│   └── test_risk.py
├── outputs/
├── pyproject.toml
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

Run the interpretable baseline:

```bash
python scripts/train_fraud_baseline.py
```

Run the full Phase 1 benchmark:

```bash
python scripts/benchmark_fraud_models.py
```

Generate SHAP importance after the benchmark:

```bash
python scripts/explain_fraud_model.py
```

Run tests:

```bash
pytest
```

## Phase 1 outputs

Generated model artifacts are intentionally excluded from version control. After running the Phase 1 scripts locally, outputs are written under:

```text
outputs/phase1/
├── fraud_baseline.joblib
├── metrics.json
├── test_predictions.csv
├── best_fraud_model.joblib
├── model_benchmark.json
├── best_model_test_predictions.csv
├── shap_feature_importance.csv
└── shap_top_features.json
```

## Evaluation philosophy

Because fraud is highly imbalanced, model quality should not be judged primarily by accuracy. The project prioritizes:

- Precision
- Recall
- F1
- PR-AUC
- ROC-AUC
- validation-only operating thresholds
- estimated fraud loss / review cost
- expected dollar exposure
- analyst-review decisions

This creates two layers of evaluation:

1. **Model quality:** how well the model ranks risky transactions.
2. **Decision quality:** how useful that ranking becomes under operational costs and thresholds.

## Existing graph foundation

The repository already includes a Neo4j-oriented ETL and graph pipeline for customers, merchants, accounts, banks, transactions, and money-laundering transfers.

Phase 2 will redesign the AML graph so every transfer is preserved as its own financial-event node rather than allowing repeated transfers between the same account pair to collapse into one relationship. The redesign will also address currently disconnected transaction records and enable graph features such as degree, centrality, suspicious-neighbor ratios, communities, cycles, funnels, and rapid movement patterns.

## CI

GitHub Actions runs the unit-test suite on pushes and pull requests, helping keep the feature-engineering and risk-scoring logic reproducible as the project grows.

## Roadmap

- [x] Reproducible Python dependencies
- [x] Project `.gitignore`
- [x] Leakage-aware behavioral feature engineering
- [x] Chronological train/validation/test pipeline
- [x] Logistic Regression baseline
- [x] Random Forest / XGBoost comparison pipeline
- [x] Cost-sensitive threshold selection
- [x] Expected fraud exposure and risk decision layer
- [x] SHAP explainability pipeline
- [x] Unit tests and GitHub Actions CI
- [ ] Run and record benchmark metrics
- [ ] Tune final model from measured benchmark results
- [ ] Redesign AML graph transaction model
- [ ] Add graph features and fraud-ring analysis
- [ ] Add risk-scoring API and analyst dashboard
