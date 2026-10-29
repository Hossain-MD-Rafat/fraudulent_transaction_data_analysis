# FraudGraph Intelligence — Financial Fraud & AML Decision Platform

An end-to-end fraud intelligence project that combines leakage-aware transaction machine learning, behavioral analytics, cost-sensitive decisioning, SHAP explanations, Neo4j network intelligence, monitoring, drift detection, FastAPI scoring, and a Streamlit analyst console.

The project is designed to answer more than **"is this transaction fraudulent?"** It also answers:

- how risky is it?
- how much financial exposure does it represent?
- why did the model consider it risky?
- is the related account/network suspicious?
- should the transaction be approved, reviewed, or blocked?
- is the model/data distribution changing over time?

## Architecture

```text
Historical transactions
        |
        v
Leakage-aware behavioral features
        |
        v
Logistic Regression / Random Forest / XGBoost benchmark
        |
        v
Versioned selected model artifact
        |
        +----------------------------+
        |                            |
        v                            v
Transaction model risk        Neo4j network risk
        |                            |
        +-------------+--------------+
                      |
                      v
              Unified operational risk
                      |
          +-----------+-----------+
          |           |           |
       APPROVE      REVIEW       BLOCK
                      |
                      v
              Analyst investigation
```

## What makes the project production-oriented

### 1. Leakage-aware behavioral modeling

Behavioral features use information available **before** the transaction being scored. Examples include:

- prior transaction count
- prior mean / standard deviation of spend
- amount deviation from customer history
- previous 1-hour / 24-hour velocity
- time since prior transaction
- prior merchant and category usage
- new-merchant and new-category indicators
- customer-to-merchant distance
- nighttime / weekend behavior

The project uses chronological train/validation/test splits rather than random splitting.

### 2. Imbalanced fraud evaluation

Fraud is rare, so accuracy is not the main success metric. The benchmark emphasizes:

- PR-AUC
- ROC-AUC
- precision
- recall
- F1
- cost-sensitive thresholds
- expected fraud exposure

The measured XGBoost benchmark on the untouched test window produced approximately:

| Metric | Result |
|---|---:|
| PR-AUC | 0.673 |
| ROC-AUC | 0.965 |
| Precision | 35.7% |
| Recall | 71.4% |
| F1 | 47.6% |
| Fraud cases detected | 5 / 7 |

Because the test period contains only a small number of fraud events, the project also includes expanding-window temporal backtesting and avoids over-claiming stability from a single split.

### 3. Cost-sensitive decisioning

The decision layer recognizes that missed fraud and unnecessary analyst reviews have different costs.

Each scored transaction can produce:

- fraud probability
- model risk score (0–100)
- expected fraud loss
- graph risk score (when Neo4j is available)
- unified operational risk score
- `APPROVE`, `REVIEW`, or `BLOCK`

### 4. Explainability

The platform supports local SHAP explanations for compatible packaged models and returns top signed feature contributions.

If SHAP is unavailable, the API explicitly falls back to transparent investigator cues such as:

- unusually high amount vs prior behavior
- large customer-to-merchant distance
- high transaction velocity
- new merchant
- new category
- nighttime transaction

Fallback signals are clearly labeled and are not presented as SHAP values.

### 5. Graph / AML intelligence

AML transfers are modeled as first-class event nodes rather than collapsed account-to-account relationships:

```text
(Account)-[:SENT]->(AMLTransfer)-[:RECEIVED_BY]->(Account)
```

This preserves repeated transfers and supports investigation of:

- laundering-neighbor exposure
- many-to-one funnels
- one-to-many fan-out behavior
- rapid pass-through movement
- short circular transfer patterns
- suspicious counterparties

Graph enrichment is optional and fail-open: if Neo4j is unavailable, transaction model scoring still works.

### 6. Governed graph escalation

Network intelligence is allowed to **escalate but never de-escalate** a model decision.

Default thresholds:

```text
Graph risk < 60      -> no graph escalation
Graph risk 60–84    -> escalate to REVIEW when needed
Graph risk >= 85    -> escalate to BLOCK when needed
```

Thresholds are configurable through environment variables.

The response exposes both:

- `model_decision`
- final `decision`

and includes a decision reason when graph intelligence caused an escalation.

### 7. Model artifact governance

Model artifacts are kept outside Git and packaged with metadata containing:

- model version
- feature-contract version
- exact feature order
- validation-selected threshold
- benchmark metadata
- SHA-256 artifact checksum

The API rejects a missing, incompatible, or checksum-mismatched artifact rather than silently falling back to another model.

### 8. Monitoring and drift detection

Operational scoring events can be written to JSONL with:

- hashed transaction identifiers
- model/version information
- risk scores
- decisions
- expected exposure
- graph-enrichment availability

Raw feature payloads are not written to monitoring logs.

The project also includes:

- numeric drift monitoring with Population Stability Index (PSI)
- categorical distribution-distance checks
- low / moderate / high drift flags
- aggregate-only reference profiles
- operational KPI summaries

## Analyst console

The Streamlit console supports:

- transaction counts
- review and block queues
- aggregate expected exposure
- risk-score distributions
- decision distributions
- investigation prioritization by expected loss
- single-transaction decision simulation

## Portfolio visuals

Saved project visuals are available under:

```text
outputs/visualizations/portfolio/
```

Included visuals cover:

- class imbalance
- fraud vs legitimate amount distribution
- fraud rate by hour
- category-level fraud rates
- AML payment-format activity
- model benchmark comparison

## Repository structure

```text
fraudulent_transaction_data_analysis/
├── api/
│   └── main.py
├── dashboard/
│   └── app.py
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── transformed/
├── docs/
│   └── PHASE3_PLATFORM.md
├── models/                  # generated locally, Git-ignored
├── outputs/
│   └── visualizations/
├── scripts/
│   ├── benchmark_fraud_models.py
│   ├── package_fraud_model.py
│   ├── smoke_test_packaged_model.py
│   ├── explain_fraud_model.py
│   ├── temporal_backtest_fraud_models.py
│   ├── graph_modeling.cypher
│   └── graph_queries.cypher
├── src/
│   └── fraud_detection/
│       ├── features.py
│       ├── inference.py
│       ├── explainability.py
│       ├── graph_risk.py
│       ├── monitoring.py
│       ├── drift.py
│       └── risk.py
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup

Create an environment and install dependencies:

```bash
python -m venv .venv
pip install -r requirements.txt
```

## Train and package the fraud model

Run the benchmark:

```bash
python scripts/benchmark_fraud_models.py
```

Package the selected model for Phase 3:

```bash
python scripts/package_fraud_model.py
```

Run the real-artifact smoke test:

```bash
python scripts/smoke_test_packaged_model.py
```

This creates a local versioned artifact such as:

```text
models/fraud_ds1_xgb_v1/
├── model.joblib
└── metadata.json
```

## Run the platform

Start the API:

```bash
uvicorn api.main:app --reload
```

API docs:

```text
http://localhost:8000/docs
```

Start the analyst console:

```bash
streamlit run dashboard/app.py
```

Or run the application stack with Docker:

```bash
docker compose up --build
```

## API endpoints

### `GET /health`

Service health information.

### `POST /v1/decision`

Converts an already-computed fraud probability into a risk score, expected exposure, and decision.

### `POST /v1/score`

Scores the strict `fraud_ds1_features_v1` feature contract with the versioned model artifact and optionally enriches the result with Neo4j account/network risk.

The response can include:

- model version
- feature-contract version
- fraud probability
- model risk score
- graph risk score
- unified risk score
- model decision
- final governed decision
- expected fraud loss
- reason codes
- SHAP contributions
- graph signals

## Configuration

Copy `.env.example` and configure values as needed.

Important environment variables include:

```text
FRAUD_MODEL_PATH
FRAUD_MODEL_METADATA_PATH
NEO4J_URI
NEO4J_USER
NEO4J_PASSWORD
NEO4J_DATABASE
GRAPH_REVIEW_THRESHOLD
GRAPH_BLOCK_THRESHOLD
FRAUD_MONITORING_LOG
```

## Validation and CI

GitHub Actions currently validates three layers:

1. **Unit tests** — feature engineering, scoring, graph policy, monitoring, drift, and API contracts.
2. **Real-model smoke test** — trains the benchmark, packages the selected model, validates checksum metadata, reloads the artifact, and performs a real score.
3. **Docker validation** — validates the Compose file and builds the full application image.

## Project phases

- [x] Phase 1 — leakage-aware transaction fraud ML
- [x] Phase 1 — cost-sensitive thresholding and expected exposure
- [x] Phase 1 — Logistic Regression / Random Forest / XGBoost benchmark
- [x] Phase 1 — expanding-window temporal evaluation
- [x] Phase 1 — SHAP explainability
- [x] Phase 2 — AML transfer-event graph redesign
- [x] Phase 2 — graph integrity validation
- [x] Phase 2 — fraud / AML investigation queries
- [x] Phase 3 — FastAPI scoring service
- [x] Phase 3 — versioned model artifact contract
- [x] Phase 3 — local explanations
- [x] Phase 3 — optional Neo4j risk enrichment
- [x] Phase 3 — governed graph escalation
- [x] Phase 3 — analyst dashboard
- [x] Phase 3 — monitoring and drift detection
- [x] Phase 3 — Docker and CI validation

## Design principle

> Build a system that detects, explains, prioritizes, and investigates financial risk — not just a model that predicts fraud.
