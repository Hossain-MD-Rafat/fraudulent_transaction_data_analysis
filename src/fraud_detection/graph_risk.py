"""Optional Neo4j-backed account/network risk enrichment.

The transaction scorer must remain available even when Neo4j is not configured.
This module therefore returns an explicit unavailable status instead of failing
base model scoring.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import os
from typing import Any


@dataclass(frozen=True)
class GraphRiskResult:
    available: bool
    account_id: str | None
    graph_risk_score: int | None
    signals: dict[str, float | int]
    error: str | None = None


def score_graph_signals(signals: dict[str, float | int]) -> int:
    """Convert interpretable graph indicators into a 0-100 operational score."""
    laundering_neighbors = float(signals.get("laundering_neighbors", 0))
    sent_24h = float(signals.get("sent_24h", 0))
    received_24h = float(signals.get("received_24h", 0))
    counterparties_24h = float(signals.get("counterparties_24h", 0))
    rapid_pass_throughs = float(signals.get("rapid_pass_throughs", 0))
    cycles_3hop = float(signals.get("cycles_3hop", 0))

    score = (
        min(laundering_neighbors, 5.0) * 10.0
        + min(counterparties_24h, 20.0) * 1.0
        + min(sent_24h + received_24h, 30.0) * 0.5
        + min(rapid_pass_throughs, 5.0) * 6.0
        + min(cycles_3hop, 3.0) * 10.0
    )
    return int(max(0, min(100, round(score))))


def fetch_account_graph_risk(account_id: str | None) -> GraphRiskResult:
    if not account_id:
        return GraphRiskResult(False, None, None, {}, "account_id_not_provided")

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    if not uri or not user or not password:
        return GraphRiskResult(False, account_id, None, {}, "neo4j_not_configured")

    try:
        from neo4j import GraphDatabase

        query = """
        MATCH (a:Account {AccountID: $account_id})
        OPTIONAL MATCH (a)-[:SENT]->(out:AMLTransfer)
        WHERE out.Timestamp >= datetime() - duration('P1D')
        WITH a, count(DISTINCT out) AS sent_24h
        OPTIONAL MATCH (inn:AMLTransfer)-[:RECEIVED_BY]->(a)
        WHERE inn.Timestamp >= datetime() - duration('P1D')
        WITH a, sent_24h, count(DISTINCT inn) AS received_24h
        OPTIONAL MATCH (a)-[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(cp:Account)
        WITH a, sent_24h, received_24h, count(DISTINCT cp) AS counterparties_24h
        OPTIONAL MATCH (a)-[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(n:Account)
        WHERE EXISTS {
          MATCH (n)-[:SENT]->(t2:AMLTransfer)
          WHERE coalesce(t2.IsLaundering, false) = true
        }
        WITH a, sent_24h, received_24h, counterparties_24h,
             count(DISTINCT n) AS laundering_neighbors
        OPTIONAL MATCH (a)-[:SENT]->(t1:AMLTransfer)-[:RECEIVED_BY]->(b:Account)-[:SENT]->(t2:AMLTransfer)
        WHERE t2.Timestamp >= t1.Timestamp
          AND t2.Timestamp <= t1.Timestamp + duration('PT1H')
        WITH a, sent_24h, received_24h, counterparties_24h, laundering_neighbors,
             count(DISTINCT t2) AS rapid_pass_throughs
        OPTIONAL MATCH (a)-[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(b:Account)
                       -[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(c:Account)
                       -[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(a)
        RETURN sent_24h, received_24h, counterparties_24h, laundering_neighbors,
               rapid_pass_throughs, count(DISTINCT c) AS cycles_3hop
        """

        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            with driver.session(database=database) as session:
                record = session.run(query, account_id=account_id).single()
        finally:
            driver.close()

        if record is None:
            return GraphRiskResult(False, account_id, None, {}, "account_not_found")

        signals: dict[str, Any] = {key: (record[key] or 0) for key in record.keys()}
        return GraphRiskResult(
            True,
            account_id,
            score_graph_signals(signals),
            {key: float(value) if isinstance(value, float) else int(value) for key, value in signals.items()},
            None,
        )
    except Exception as exc:  # graph dependency must not take down base scoring
        return GraphRiskResult(False, account_id, None, {}, f"neo4j_error:{type(exc).__name__}")


def unified_risk_score(model_risk_score: int, graph_risk_score: int | None) -> int:
    """Blend model and network scores without representing the result as probability."""
    if graph_risk_score is None:
        return int(max(0, min(100, model_risk_score)))
    blended = 0.80 * float(model_risk_score) + 0.20 * float(graph_risk_score)
    return int(max(0, min(100, round(blended))))
