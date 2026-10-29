"""Summarize privacy-safe operational KPIs from scoring JSONL events.

Usage:
    python scripts/summarize_scoring_kpis.py runtime/scoring_events.jsonl
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path


def load_events(path: Path) -> list[dict]:
    events: list[dict] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def summarize(events: list[dict]) -> dict:
    if not events:
        return {
            "event_count": 0,
            "decision_counts": {},
            "decision_rates": {},
            "average_fraud_probability": 0.0,
            "average_unified_risk_score": 0.0,
            "total_expected_fraud_loss": 0.0,
            "graph_enrichment_rate": 0.0,
            "models": {},
        }

    decisions = Counter(str(event.get("decision", "UNKNOWN")) for event in events)
    models = Counter(str(event.get("model_version", "unknown")) for event in events)
    n = len(events)
    return {
        "event_count": n,
        "decision_counts": dict(decisions),
        "decision_rates": {key: value / n for key, value in decisions.items()},
        "average_fraud_probability": sum(float(e.get("fraud_probability", 0.0)) for e in events) / n,
        "average_unified_risk_score": sum(float(e.get("unified_risk_score", 0.0)) for e in events) / n,
        "total_expected_fraud_loss": sum(float(e.get("expected_fraud_loss", 0.0)) for e in events),
        "graph_enrichment_rate": sum(bool(e.get("graph_available", False)) for e in events) / n,
        "models": dict(models),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("log_path", type=Path)
    parser.add_argument("--output", type=Path, default=Path("runtime/scoring_kpis.json"))
    args = parser.parse_args()

    summary = summarize(load_events(args.log_path))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
