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
Fraud probability + local SHAP explanation
        |
        +-------------------------+
        |                         |
        v                         v
FastAPI Scoring API        Streamlit Analyst Console
        |                         |
        +----> optional Neo4j account/network enrichment
        |                         |
        v                         v
Model risk score           Portfolio overview
Graph risk score           Investigation queue
Unified risk score         Decision sandbox
Expected exposure
APPROVE/REVIEW/BLOCK
        |
        v
Privacy-conscious JSONL monitoring stream
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
cp .env.example .env
# edit Neo4j credentials if graph enrichment is desired
docker compose up --build
```

Services:

- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Analyst dashboard: `http://localhost:8501`

The API mounts `./models` read-only and `./runtime` for operational logs. Model binaries remain external deployable artifacts rather than being baked into the application image. Raw and transformed datasets are excluded from the Docker build context.

## API contracts

### Health

`GET /health`

### Decision-only endpoint

`POST /v1/decision`

Use this when an upstream system already has a fraud probability and only needs the policy layer: risk score, expected exposure, and APPROVE/REVIEW/BLOCK action.

### Model scoring endpoint

`POST /v1/score`

The request contains a transaction ID, an optional AML `account_id`, and the `fraud_ds1_features_v1` engineered feature contract. The response includes:

- model version
- feature-contract version
- calibrated model fraud probability
- model risk score (0-100)
- optional graph risk score (0-100)
- unified operational risk score (0-100)
- expected fraud exposure
- APPROVE / REVIEW / BLOCK action
- effective review and block thresholds
- heuristic investigator reason codes
- top local SHAP contributions when the packaged pipeline supports SHAP
- graph-enrichment availability and network signals

The unified risk score is an operational prioritization score, not a second fraud probability. When graph enrichment is unavailable, it falls back to the model risk score.

If the model or metadata is unavailable or incompatible, `/v1/score` returns HTTP 503. If Neo4j is unavailable, the model score still succeeds and the response explicitly reports why graph enrichment was skipped.

## Local explanations

For supported packaged pipelines, the API computes local SHAP contributions on the transformed model feature space. Each returned contribution has:

- transformed feature name
- signed SHAP value
- direction (`toward_fraud` or `toward_legitimate`)

If SHAP cannot be computed, the service safely falls back to the transparent heuristic input signals already used by investigators.

## Neo4j graph enrichment

Set these environment variables to enable account-level enrichment:

```text
NEO4J_URI
NEO4J_USER
NEO4J_PASSWORD
NEO4J_DATABASE
```

The graph layer currently measures interpretable indicators such as:

- transfers sent/received in the last 24 hours
- distinct counterparties
- neighbors connected to known laundering behavior
- rapid pass-through activity
- three-hop circular movement

These indicators are converted into a bounded graph-risk score and blended with the model score using a transparent 80/20 operational weighting. This weighting is a project policy assumption and should be governed/tuned by a real fraud-risk team before production use.

## Monitoring and audit events

Set `FRAUD_MONITORING_LOG` to enable append-only JSONL scoring events. The log stores model/version fields, probability, model/graph/unified scores, decision, expected exposure, graph availability, and explanation method.

Raw feature payloads are not logged, and transaction IDs are SHA-256 hashed before persistence. Monitoring failures never cause a scoring request to fail.

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

1. Add drift monitoring against training/reference feature distributions.
2. Add operational KPI summaries from the JSONL scoring stream.
3. Add Docker build checks and deployment configuration.
4. Add model-registry / artifact checksum validation.
5. Add a governed policy for whether graph risk can escalate an APPROVE decision to REVIEW/BLOCK.
