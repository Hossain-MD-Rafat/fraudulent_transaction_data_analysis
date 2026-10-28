# Phase 3 — Fraud Intelligence Platform

Phase 3 turns the validated ML + graph foundation into an analyst-facing, reproducible application layer.

## Current architecture

```text
Engineered transaction features
        |
        v
Versioned fraud model artifact
        |
        v
Fraud probability
        |
        +-------------------------+
        |                         |
        v                         v
FastAPI Scoring API        Streamlit Analyst Console
        |                         |
        v                         v
Risk score 0-100           Portfolio overview
Expected exposure          Investigation queue
Reason codes               Decision sandbox
APPROVE/REVIEW/BLOCK
```

The service uses a strict versioned feature contract. Behavioral features must be calculated from transactions that occurred before the transaction being scored; the API does not reconstruct historical customer behavior from a single raw transaction.

## Prepare the model artifact

First run the benchmark that selects and serializes the best Phase 1 pipeline:

```bash
python scripts/benchmark_fraud_models.py
```

Then package it for the scoring service:

```bash
python scripts/package_fraud_model.py
```

This creates the local, Git-ignored directory:

```text
models/fraud_ds1_xgb_v1/
├── model.joblib
└── metadata.json
```

The metadata records the model version, feature-contract version, exact feature order, validation-selected review threshold, split strategy, and final test metrics. The API rejects missing or incompatible artifacts instead of silently falling back to another model.

## Run locally

```bash
pip install -r requirements.txt
uvicorn api.main:app --reload
```

Open `http://localhost:8000/docs` for the interactive API documentation.

In another terminal:

```bash
streamlit run dashboard/app.py
```

The dashboard is available at `http://localhost:8501`.

## Run with Docker Compose

```bash
docker compose up --build
```

Services:

- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Analyst dashboard: `http://localhost:8501`

The API mounts `./models` read-only. Model binaries remain external deployable artifacts rather than being baked into the application image. Raw and transformed datasets are excluded from the Docker build context.

## API contracts

### Health

`GET /health`

### Decision-only endpoint

`POST /v1/decision`

Use this when an upstream system already has a fraud probability and only needs the policy layer: risk score, expected exposure, and APPROVE/REVIEW/BLOCK action.

### Model scoring endpoint

`POST /v1/score`

The request contains a transaction ID plus the `fraud_ds1_features_v1` engineered feature contract. The response includes:

- model version
- feature-contract version
- fraud probability
- risk score (0-100)
- expected fraud exposure
- APPROVE / REVIEW / BLOCK action
- effective review and block thresholds
- reason codes
- explanation method identifier

The current reason codes are transparent heuristic investigator cues such as unusual amount, geographic distance, velocity, novelty, and nighttime activity. They are deliberately labeled `heuristic_input_signals_v1`; they are not presented as SHAP/model attribution yet.

If the model or metadata is unavailable or incompatible, `/v1/score` returns HTTP 503.

## Analyst console

The Streamlit console can ingest a scored transaction CSV and provides:

- transaction count
- review queue size
- block-candidate count
- aggregate expected fraud exposure
- risk-score distribution
- decision distribution
- investigation queue ranked by expected fraud exposure
- minimum-exposure and decision filters
- single-transaction decision simulator

## Next production increments

1. Add genuine SHAP-based model reason codes and local explanations.
2. Add Neo4j-backed account/network risk enrichment.
3. Combine transaction probability and graph-risk signals into a governed unified risk policy.
4. Add request/decision audit logging and model/version identifiers to persisted events.
5. Add drift and operational monitoring.
6. Add Docker build checks and deployment configuration.
