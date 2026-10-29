# FraudGraph Intelligence — Portfolio Summary

## One-line description

A production-style financial fraud intelligence platform that combines leakage-aware transaction machine learning, cost-sensitive decisioning, SHAP explainability, Neo4j AML graph analysis, FastAPI model serving, Streamlit investigation workflows, monitoring, drift detection, and Docker-based reproducibility.

## Problem

Fraud detection is not only a binary classification problem. A useful fraud system also needs to answer:

- Which transactions should analysts investigate first?
- What financial exposure does a transaction represent?
- Which behavioral signals contributed to risk?
- Is the related account connected to suspicious network behavior?
- Should the transaction be approved, reviewed, or blocked?
- Is the model or incoming data distribution changing over time?

## Solution

FraudGraph Intelligence combines two complementary risk layers:

1. **Transaction intelligence** — XGBoost selected from Logistic Regression, Random Forest, and XGBoost candidates using chronological validation and PR-AUC.
2. **Network intelligence** — Neo4j-based AML transfer analysis that preserves every transfer as an event and detects suspicious graph patterns such as funnels, fan-out behavior, rapid pass-through movement, suspicious neighbors, and short transfer cycles.

The model and graph signals are converted into a transparent operational risk workflow with expected exposure, local explanations, and APPROVE / REVIEW / BLOCK decisions.

## Measured result

On the untouched chronological test window, the selected XGBoost model achieved approximately:

| Metric | Result |
|---|---:|
| PR-AUC | 0.673 |
| ROC-AUC | 0.965 |
| Precision | 35.7% |
| Recall | 71.4% |
| F1 | 47.6% |
| Fraud detected | 5 / 7 |

The test window contains few positive events, so the project includes expanding-window temporal validation and explicitly avoids over-claiming statistical stability.

## Engineering highlights

- Prior-only behavioral feature engineering to reduce target/future leakage
- Chronological train/validation/test design
- Validation-only model and threshold selection
- Cost-sensitive fraud decisions and expected dollar exposure
- Versioned inference feature contract
- SHA-256 model-artifact integrity verification
- Local SHAP explanations with safe heuristic fallback
- Neo4j AML transfer-event graph
- Governed graph escalation: graph risk may increase but never lower an action
- Privacy-conscious event logging with hashed transaction identifiers
- PSI and categorical distribution drift monitoring
- FastAPI scoring API
- Streamlit analyst console
- Docker / Docker Compose
- GitHub Actions unit, real-model, and Docker quality gates

## Technical stack

Python, Pandas, NumPy, scikit-learn, XGBoost, SHAP, Neo4j, Cypher, FastAPI, Streamlit, Docker, GitHub Actions.

## Interview talking point

The most important design decision was treating fraud detection as a **decision and investigation system**, not only a classification notebook. Model ranking, operating thresholds, graph intelligence, explainability, exposure prioritization, monitoring, and deployment are deliberately separated so each layer can be evaluated and governed independently.
