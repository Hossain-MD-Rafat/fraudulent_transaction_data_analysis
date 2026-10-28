# Phase 3 — Fraud Intelligence Platform

Phase 3 turns the validated ML + graph foundation into an analyst-facing, reproducible application layer.

## Current architecture

```text
Model / benchmark output
        |
        v
Fraud probability + amount
        |
        +--------------------+
        |                    |
        v                    v
FastAPI Decision API    Streamlit Analyst Console
        |                    |
        v                    v
Risk score 0-100        Portfolio overview
Expected exposure       Investigation queue
APPROVE/REVIEW/BLOCK    Decision sandbox
```

The API currently accepts a fraud probability rather than loading a trained model artifact. This is intentional: model serving should only be added after a stable feature contract, artifact version, and inference pipeline are defined.

## Run locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the API:

```bash
uvicorn api.main:app --reload
```

Open the interactive API documentation at `http://localhost:8000/docs`.

Start the analyst dashboard in another terminal:

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

The Docker build intentionally excludes raw/cleaned/transformed datasets and generated artifacts. The Phase 3 application image should contain application code, not training data.

## API contract

### Health

`GET /health`

### Fraud decision

`POST /v1/decision`

Example request:

```json
{
  "transaction_id": "txn-001",
  "amount": 1000.0,
  "fraud_probability": 0.60,
  "review_threshold": 0.50,
  "block_threshold": 0.90
}
```

Example decision fields:

- fraud probability
- risk score (0-100)
- expected fraud exposure
- APPROVE / REVIEW / BLOCK action
- effective review and block thresholds

The thresholds are demonstration policy controls. They are not represented as bank-specific operating rules.

## Analyst console

The Streamlit console can ingest a scored transaction CSV. It normalizes common probability/amount/id column names and provides:

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

1. Define and version a strict inference feature contract.
2. Persist a selected trained model artifact with metadata and threshold configuration.
3. Add `/v1/score` to transform a transaction payload into features and model probability.
4. Add SHAP reason codes to the scoring response.
5. Add Neo4j-backed account/network risk enrichment.
6. Add request/decision logging and model/version identifiers.
7. Add drift and operational monitoring.
8. Add Docker build checks and deployment configuration.
