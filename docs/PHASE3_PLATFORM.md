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

## Prepare and validate the model artifact

Run the benchmark that selects and serializes the best Phase 1 pipeline:

```bash
python scripts/benchmark_fraud_models.py
```

Package it for the scoring service:

```bash
python scripts/package_fraud_model.py
```

Validate the exact packaged artifact through the production loader:

```bash
python scripts/smoke_test_packaged_model.py
```

This creates the local, Git-ignored directory:

```text
models/fraud_ds1_xgb_v1/
├── model.joblib
└── metadata.json
```

Metadata records the model version, feature-contract version, exact feature order, validation-selected review threshold, split strategy, final test metrics, and SHA-256 checksum. The API rejects missing, incompatible, or checksum-mismatched artifacts instead of silently falling back to another model.

## Run locally

```bash
pip install -r requirements.txt
uvicorn api.main:app --reload
```

Open `http://localhost:8000/docs` for interactive API documentation.

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
- model risk score (0–100)
- optional graph risk score (0–100)
- unified operational risk score (0–100)
- expected fraud exposure
- model decision
- final governed decision
- decision reason when network intelligence escalates the action
- heuristic investigator reason codes
- top local SHAP contributions when the packaged pipeline supports SHAP
- graph-enrichment availability and network signals

The unified risk score is an operational prioritization score, not a second fraud probability. When graph enrichment is unavailable, it falls back to the model risk score.

If the model or metadata is unavailable or incompatible, `/v1/score` returns HTTP 503. If Neo4j is unavailable, model scoring still succeeds and the response explicitly reports why graph enrichment was skipped.

## Local explanations

For supported packaged pipelines, the API computes local SHAP contributions on the transformed model feature space. Each returned contribution has:

- transformed feature name
- signed SHAP value
- direction (`toward_fraud` or `toward_legitimate`)

If SHAP cannot be computed, the service safely falls back to transparent heuristic input signals.

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

These indicators are converted into a bounded graph-risk score and blended with the model score using a transparent 80/20 operational weighting. This weighting is a project policy assumption, not a calibrated fraud probability.

## Governed graph escalation

Network evidence can escalate a transaction-model decision, but it can never lower one.

Default policy:

```text
Graph risk < 60      -> no escalation
Graph risk 60–84    -> at least REVIEW
Graph risk >= 85    -> BLOCK
```

Configure with:

```text
GRAPH_REVIEW_THRESHOLD
GRAPH_BLOCK_THRESHOLD
```

Examples:

```text
Model APPROVE + graph 72 -> REVIEW
Model REVIEW  + graph 91 -> BLOCK
Model BLOCK   + graph 10 -> BLOCK
```

The API exposes both `model_decision` and final `decision`, plus `decision_reason` when escalation occurs.

## Monitoring and audit events

Set `FRAUD_MONITORING_LOG` to enable append-only JSONL scoring events. The log stores model/version fields, probability, model/graph/unified scores, decision, expected exposure, graph availability, and explanation method.

Raw feature payloads are not logged, and transaction IDs are SHA-256 hashed before persistence. Monitoring failures never cause a scoring request to fail.

Operational KPI tooling summarizes:

- decision distribution
- average fraud probability
- average unified risk
- expected fraud exposure
- graph-enrichment coverage
- model-version usage

## Drift monitoring

The project can generate aggregate-only reference distributions from the historical feature set and compare later batches using:

- Population Stability Index (PSI) for numeric features
- categorical total-variation distance
- low / moderate / high drift flags

Reference files contain distribution summaries, not raw training records.

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

## CI quality gates

GitHub Actions validates three independent layers:

1. unit tests for ML/decision/graph/monitoring/drift/API logic
2. real-model smoke test that retrains, packages, reloads, and scores with the selected XGBoost artifact
3. Docker Compose validation and full application image build

All three gates have been validated successfully on the completed Phase 3 branch validation run.
