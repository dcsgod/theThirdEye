"""
scoring/correlate_signals.py
==============================
Third Eye — Signal Correlation Engine

Implements Section 7.1 of thirdeye_project.md:

1. Pull new rows from signal_index since last run.
2. For each model with a signal crossing its configured threshold, look up
   lineage_events for that model's upstream tables within ±correlation_window_hours.
3. If a qualifying combination exists (multi-signal co-occurrence), open a row
   in incidents with all contributing signals as evidence.
4. Single weak signals WITHOUT corroborating evidence → do NOT open an incident.
   This is the anti-alert-fatigue gate.

This is a straightforward time-window join — not a new statistical method.

Run as: Databricks Workflow task (after compute_health_score completes).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any

import yaml

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.scoring.correlate_signals")

REGISTRY_MAP_TABLE   = "governance.model_health.model_registry_map"
SIGNAL_INDEX_TABLE   = "governance.model_health.signal_index"
LINEAGE_EVENTS_TABLE = "governance.model_health.lineage_events"
INCIDENTS_TABLE      = "governance.model_health.incidents"
RISK_SCORES_TABLE    = "governance.model_health.risk_scores"
RISK_WEIGHTS_PATH    = os.path.join(os.path.dirname(__file__), "..", "config", "risk_weights.yaml")

LOOKBACK_HOURS = int(os.getenv("THIRD_EYE_SIGNAL_LOOKBACK_HOURS", "25"))


# ---------------------------------------------------------------------------
# Severity mapping
# ---------------------------------------------------------------------------
def _compute_severity(health_tier: str, criticality_tier: str) -> str:
    matrix = {
        ("critical", "tier_1_business"):     "critical",
        ("critical", "tier_2_operational"):  "high",
        ("critical", "tier_3_low"):          "medium",
        ("critical", "tier_0_experimental"): "low",
        ("at_risk",  "tier_1_business"):     "high",
        ("at_risk",  "tier_2_operational"):  "medium",
        ("at_risk",  "tier_3_low"):          "low",
        ("at_risk",  "tier_0_experimental"): "low",
        ("watch",    "tier_1_business"):     "medium",
        ("watch",    "tier_2_operational"):  "low",
        ("watch",    "tier_3_low"):          "low",
        ("watch",    "tier_0_experimental"): "low",
    }
    return matrix.get((health_tier, criticality_tier), "low")


def _recommended_action(signal_types: set[str], severity: str, lineage_correlated: bool) -> str:
    if severity in ("critical", "high") and lineage_correlated:
        return "remediate"
    if severity in ("critical", "high"):
        return "investigate"
    if severity == "medium":
        return "investigate"
    return "no_action"


# ---------------------------------------------------------------------------
# Incident data model
# ---------------------------------------------------------------------------
@dataclass
class Incident:
    incident_id:             str
    model_id:                str
    opened_at:               datetime
    closed_at:               datetime | None
    trigger_signals:         str   # JSON array of signal_ids
    trigger_signal_types:    str   # comma-separated
    lineage_context:         str | None  # JSON array of lineage event_ids
    correlation_window_hours: int
    root_cause_narrative:    str | None
    root_cause_confidence:   float | None
    root_cause_model:        str | None
    recommended_action:      str
    severity:                str
    status:                  str
    acknowledged_by:         str | None
    acknowledged_at:         datetime | None
    resolved_by:             str | None
    resolved_at:             datetime | None
    resolution_notes:        str | None
    auto_action_taken:       str | None
    auto_action_at:          datetime | None
    approval_required:       bool | None
    approved_by:             str | None
    approved_at:             datetime | None


# ---------------------------------------------------------------------------
# Mock data for correlation testing
# ---------------------------------------------------------------------------
def _generate_mock_correlation_data() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Returns (models, signals, lineage_events, existing_open_incidents).
    """
    from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
    mock_registry = _generate_mock_registry()
    now = datetime.now(tz=timezone.utc)

    models = [
        {
            "model_id": _make_model_id(e["name"], str(e["version"])),
            "model_name": e["name"],
            "criticality_tier": "tier_1_business" if e.get("business_domain") in ("finance", "fraud", "customer") else "tier_2_operational",
        }
        for e in mock_registry
    ]

    mid_credit = _make_model_id("main.finance.credit_risk_classifier", "12")
    mid_demand = _make_model_id("main.operations.demand_forecaster", "5")
    mid_churn  = _make_model_id("main.customer.churn_predictor", "3")
    mid_fraud  = _make_model_id("main.finance.fraud_detection_agent", "2")

    signals = [
        # credit risk: drift + cost + guardrail all breached → should open incident
        {"signal_id": "s001", "model_id": mid_credit, "signal_type": "drift",     "threshold_breached": True,  "computed_at": (now - timedelta(hours=2)).isoformat()},
        {"signal_id": "s003", "model_id": mid_credit, "signal_type": "cost",      "threshold_breached": True,  "computed_at": (now - timedelta(hours=1)).isoformat()},
        {"signal_id": "s004", "model_id": mid_credit, "signal_type": "guardrail", "threshold_breached": True,  "computed_at": (now - timedelta(hours=1.5)).isoformat()},
        {"signal_id": "s002", "model_id": mid_credit, "signal_type": "accuracy",  "threshold_breached": False, "computed_at": (now - timedelta(hours=2)).isoformat()},
        # demand: moderate drift only, NOT breaching threshold → should NOT open incident
        {"signal_id": "s006", "model_id": mid_demand, "signal_type": "drift",     "threshold_breached": False, "computed_at": (now - timedelta(hours=3)).isoformat()},
        # churn: drift breached but no lineage corroboration, single signal → NO incident yet
        {"signal_id": "s009", "model_id": mid_churn,  "signal_type": "drift",     "threshold_breached": True,  "computed_at": (now - timedelta(hours=0.5)).isoformat()},
        # fraud: guardrail breached (prompt injection) → single signal but Tier 1 → open incident
        {"signal_id": "s012", "model_id": mid_fraud,  "signal_type": "guardrail", "threshold_breached": True,  "computed_at": (now - timedelta(hours=0.3)).isoformat()},
    ]

    lineage_events = [
        # credit risk: upstream schema_change 26h ago correlates with drift
        {"event_id": "le001", "model_id": mid_credit, "upstream_table": "main.raw.transactions",
         "event_type": "schema_change", "event_time": (now - timedelta(hours=26)).isoformat()},
        # churn: upstream large_write 1h ago — within window of drift signal
        {"event_id": "le002", "model_id": mid_churn,  "upstream_table": "main.features.customer_features",
         "event_type": "large_write",   "event_time": (now - timedelta(hours=1.2)).isoformat()},
    ]

    return models, signals, lineage_events, []


# ---------------------------------------------------------------------------
# Core correlation logic
# ---------------------------------------------------------------------------
def correlate_and_open_incidents(
    models: list[dict],
    signals: list[dict],
    lineage_events: list[dict],
    existing_open_model_ids: set[str],
    config: dict,
    latest_health_scores: dict[str, dict],
) -> list[Incident]:
    """
    Main correlation loop.
    Returns list of new Incident objects to write.
    """
    now = datetime.now(tz=timezone.utc)

    # Index signals by model
    from collections import defaultdict
    signals_by_model: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        signals_by_model[s["model_id"]].append(s)

    # Index lineage events by model
    lineage_by_model: dict[str, list[dict]] = defaultdict(list)
    for le in lineage_events:
        lineage_by_model[le["model_id"]].append(le)

    new_incidents: list[Incident] = []

    for model in models:
        model_id = model["model_id"]
        crit_tier = model.get("criticality_tier", "tier_2_operational")
        tier_cfg  = config.get("tiers", {}).get(crit_tier, config["tiers"]["tier_2_operational"])
        window_h  = tier_cfg.get("correlation_window_hours", 2)
        min_sigs  = tier_cfg.get("incident_min_signals", 2)

        model_signals = signals_by_model.get(model_id, [])
        breached      = [s for s in model_signals if s.get("threshold_breached")]

        if not breached:
            continue

        # Count distinct signal types that breached
        breached_types = {s["signal_type"] for s in breached}

        # Find corroborating lineage events within the correlation window
        breach_times = []
        for s in breached:
            ts_str = s.get("computed_at", "")
            if ts_str:
                try:
                    bt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    breach_times.append(bt)
                except ValueError:
                    pass

        earliest_breach = min(breach_times) if breach_times else now
        latest_breach   = max(breach_times) if breach_times else now
        window_start = earliest_breach - timedelta(hours=window_h)
        window_end   = latest_breach   + timedelta(hours=window_h)

        corroborating_lineage = [
            le for le in lineage_by_model.get(model_id, [])
            if _parse_dt(le.get("event_time")) and window_start <= _parse_dt(le["event_time"]) <= window_end
        ]
        lineage_correlated = len(corroborating_lineage) > 0

        # --- Incident-opening logic (anti-alert-fatigue) ---
        # Rule: open an incident if:
        #   (a) ≥ min_sigs distinct signal types breached, OR
        #   (b) ANY signal breached AND lineage corroborates (strong evidence), OR
        #   (c) Tier 1 model AND any breach at all (strictest monitoring)
        open_incident = (
            len(breached_types) >= min_sigs
            or (len(breached_types) >= 1 and lineage_correlated)
            or (crit_tier == "tier_1_business" and len(breached_types) >= 1)
        )

        # Don't open a duplicate for the same model if there's already an open incident
        if model_id in existing_open_model_ids:
            logger.info("Skipping %s — already has an open incident.", model["model_name"])
            continue

        if not open_incident:
            logger.info(
                "Suppressing alert for %s — %d signal type(s) breached, no lineage correlation. Not enough evidence.",
                model.get("model_name", model_id), len(breached_types)
            )
            continue

        # Get current health score/tier for severity
        hs = latest_health_scores.get(model_id, {})
        health_tier = hs.get("health_tier", "watch")
        severity = _compute_severity(health_tier, crit_tier)
        action   = _recommended_action(breached_types, severity, lineage_correlated)

        incident = Incident(
            incident_id=str(uuid.uuid4()),
            model_id=model_id,
            opened_at=now,
            closed_at=None,
            trigger_signals=json.dumps([s["signal_id"] for s in breached]),
            trigger_signal_types=", ".join(sorted(breached_types)),
            lineage_context=json.dumps([le["event_id"] for le in corroborating_lineage]) if corroborating_lineage else None,
            correlation_window_hours=window_h,
            root_cause_narrative=None,   # filled by generate_root_cause.py
            root_cause_confidence=None,
            root_cause_model=None,
            recommended_action=action,
            severity=severity,
            status="open",
            acknowledged_by=None,
            acknowledged_at=None,
            resolved_by=None,
            resolved_at=None,
            resolution_notes=None,
            auto_action_taken=None,
            auto_action_at=None,
            approval_required=(crit_tier == "tier_1_business"),
            approved_by=None,
            approved_at=None,
        )
        new_incidents.append(incident)

        logger.info(
            "INCIDENT OPENED for %s | severity=%s | signals=%s | lineage=%s | action=%s",
            model.get("model_name"), severity,
            ", ".join(sorted(breached_types)),
            "yes" if lineage_correlated else "no",
            action,
        )

    return new_incidents


def _parse_dt(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_correlation(dry_run: bool = False) -> dict[str, Any]:
    config = yaml.safe_load(open(RISK_WEIGHTS_PATH))

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic correlation data.")
        models, signals, lineage_events, _ = _generate_mock_correlation_data()
        existing_open_model_ids: set[str] = set()
        latest_health_scores: dict[str, dict] = {}
        # Simulate getting latest health scores from scoring engine output
        from scoring.compute_health_score import run_scoring
        scoring_result = run_scoring(dry_run=True)
        for sc in scoring_result.get("scores", []):
            latest_health_scores[sc["model_id"]] = sc
    else:
        spark = SparkSession.builder.getOrCreate()
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()

        models_rows = spark.sql(f"""
            SELECT model_id, model_name, criticality_tier FROM {REGISTRY_MAP_TABLE} WHERE is_active = true
        """).collect()
        models = [r.asDict() for r in models_rows]

        signals_rows = spark.sql(f"""
            SELECT signal_id, model_id, signal_type, threshold_breached,
                   CAST(computed_at AS STRING) AS computed_at
            FROM {SIGNAL_INDEX_TABLE}
            WHERE ingested_at >= '{cutoff}' AND threshold_breached = true
        """).collect()
        signals = [r.asDict() for r in signals_rows]

        lineage_rows = spark.sql(f"""
            SELECT event_id, model_id, upstream_table, event_type,
                   CAST(event_time AS STRING) AS event_time
            FROM {LINEAGE_EVENTS_TABLE}
            WHERE event_time >= DATEADD(HOUR, -{LOOKBACK_HOURS + 4}, CURRENT_TIMESTAMP())
        """).collect()
        lineage_events = [r.asDict() for r in lineage_rows]

        open_rows = spark.sql(f"""
            SELECT DISTINCT model_id FROM {INCIDENTS_TABLE} WHERE status = 'open'
        """).collect()
        existing_open_model_ids = {r["model_id"] for r in open_rows}

        score_rows = spark.sql(f"""
            SELECT model_id, health_score, health_tier
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
                FROM {RISK_SCORES_TABLE}
            ) WHERE rn = 1
        """).collect()
        latest_health_scores = {r["model_id"]: r.asDict() for r in score_rows}

    new_incidents = correlate_and_open_incidents(
        models=models,
        signals=signals,
        lineage_events=lineage_events,
        existing_open_model_ids=existing_open_model_ids,
        config=config,
        latest_health_scores=latest_health_scores,
    )

    stats = {
        "new_incidents_opened": len(new_incidents),
        "incident_ids": [i.incident_id for i in new_incidents],
        "severities": {i.severity: 1 for i in new_incidents},
    }

    if not MOCK_MODE and not dry_run:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(i) for i in new_incidents]
        if rows:
            df = spark.createDataFrame(rows)
            df.write.mode("append").saveAsTable(INCIDENTS_TABLE)
            logger.info("Wrote %d incidents to %s.", len(rows), INCIDENTS_TABLE)

    logger.info("Correlation complete: %s", stats)
    return {"stats": stats, "incidents": [asdict(i) for i in new_incidents]}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    result = run_correlation(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
    print(f"\nNew incidents opened: {result['stats']['new_incidents_opened']}")
    for inc in result["incidents"]:
        print(f"  [{inc['incident_id'][:8]}] model={inc['model_id'][:8]} severity={inc['severity']} signals={inc['trigger_signal_types']} action={inc['recommended_action']}")
