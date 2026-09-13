"""
scoring/compute_health_score.py
=================================
Third Eye — Health Score & Confidence Computation Engine

This is the core computation that Databricks does NOT provide natively.
It computes a composite, criticality-weighted health score + confidence score
for each model in the fleet, using normalized signals from signal_index.

Formula (Section 7.2):
  health_score = 100 - (
      w1 * drift_component    +
      w2 * (1 - quality_component) +
      w3 * cost_component     +
      w4 * guardrail_component
  ) * criticality_weight

  confidence = f(evidence_volume, baseline_window_completeness, ground_truth_availability)

Output: governance.model_health.risk_scores

Run as: Databricks Workflow task (after all signal adapters complete).
"""

from __future__ import annotations

import json
import logging
import math
import os
import uuid
from dataclasses import dataclass, asdict, field
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
logger = logging.getLogger("third_eye.scoring.health_score")

REGISTRY_MAP_TABLE = "governance.model_health.model_registry_map"
SIGNAL_INDEX_TABLE = "governance.model_health.signal_index"
RISK_SCORES_TABLE  = "governance.model_health.risk_scores"
RISK_WEIGHTS_PATH  = os.path.join(os.path.dirname(__file__), "..", "config", "risk_weights.yaml")

# Look back this many hours for signals to include in the current scoring run
SIGNAL_WINDOW_HOURS = int(os.getenv("THIRD_EYE_SCORE_SIGNAL_WINDOW_HOURS", "25"))

HEALTH_TIER_THRESHOLDS = {
    "tier_0_experimental": {"healthy": 80, "watch": 60, "at_risk": 40},
    "tier_1_business":     {"healthy": 80, "watch": 65, "at_risk": 45},
    "tier_2_operational":  {"healthy": 80, "watch": 60, "at_risk": 40},
    "tier_3_low":          {"healthy": 80, "watch": 60, "at_risk": 40},
}


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def _load_weights(tier: str, config: dict) -> dict[str, float]:
    tier_cfg = config.get("tiers", {}).get(tier, config["tiers"]["tier_2_operational"])
    return {
        "w1_drift":          tier_cfg["w1_drift"],
        "w2_quality":        tier_cfg["w2_quality"],
        "w3_cost":           tier_cfg["w3_cost"],
        "w4_guardrail":      tier_cfg["w4_guardrail"],
        "criticality_weight": tier_cfg["criticality_weight"],
    }


def _health_tier(health_score: float, criticality_tier: str) -> str:
    thresholds = HEALTH_TIER_THRESHOLDS.get(criticality_tier, HEALTH_TIER_THRESHOLDS["tier_2_operational"])
    if health_score >= thresholds["healthy"]:
        return "healthy"
    if health_score >= thresholds["watch"]:
        return "watch"
    if health_score >= thresholds["at_risk"]:
        return "at_risk"
    return "critical"


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------
def _compute_confidence(
    ground_truth_available: bool,
    signal_types_present: set[str],
    traffic_volume: int,   # total requests in window
    baseline_days: int,    # how many days of baseline exist
    config: dict,
) -> tuple[float, dict]:
    """
    Confidence = weighted sum of four evidence-quality factors.
    Returns (confidence_score_0_to_1, factors_dict).
    """
    conf_cfg = config.get("confidence", {})

    # Factor 1: ground truth available
    w_gt = conf_cfg.get("ground_truth_available", {}).get("weight", 0.35)
    gt_score = 1.0 if ground_truth_available else 0.0

    # Factor 2: baseline window completeness
    w_bw = conf_cfg.get("baseline_window_completeness", {}).get("weight", 0.25)
    full_credit_days = conf_cfg.get("baseline_window_completeness", {}).get("full_credit_days", 14)
    bw_score = min(1.0, baseline_days / full_credit_days)

    # Factor 3: traffic volume
    w_tv = conf_cfg.get("traffic_volume", {}).get("weight", 0.25)
    full_credit_req = conf_cfg.get("traffic_volume", {}).get("full_credit_requests_per_day", 1000)
    min_req = conf_cfg.get("traffic_volume", {}).get("min_requests_per_day", 10)
    if traffic_volume < min_req:
        tv_score = 0.1
    else:
        tv_score = min(1.0, traffic_volume / full_credit_req)

    # Factor 4: signal count (how many distinct signal types present)
    w_sc = conf_cfg.get("signal_count", {}).get("weight", 0.15)
    full_credit_types = conf_cfg.get("signal_count", {}).get("full_credit_signal_count", 4)
    sc_score = min(1.0, len(signal_types_present) / full_credit_types)

    confidence = (w_gt * gt_score) + (w_bw * bw_score) + (w_tv * tv_score) + (w_sc * sc_score)
    confidence = round(max(0.0, min(1.0, confidence)), 4)

    factors = {
        "ground_truth_available": ground_truth_available,
        "gt_score": gt_score,
        "baseline_window_days": baseline_days,
        "bw_score": bw_score,
        "traffic_volume": traffic_volume,
        "tv_score": tv_score,
        "signal_types_present": list(signal_types_present),
        "sc_score": sc_score,
        "confidence": confidence,
    }
    return confidence, factors


# ---------------------------------------------------------------------------
# Health score computation per model
# ---------------------------------------------------------------------------
@dataclass
class RiskScore:
    score_id:            str
    model_id:            str
    computed_at:         datetime
    drift_component:     float | None
    quality_component:   float | None
    cost_component:      float | None
    guardrail_component: float | None
    latency_component:   float | None
    w1_drift:            float
    w2_quality:          float
    w3_cost:             float
    w4_guardrail:        float
    criticality_weight:  float
    raw_weighted_sum:    float
    health_score:        float
    health_tier:         str
    confidence:          float
    confidence_factors:  str   # JSON
    signal_count:        int
    signals_used:        str   # JSON array of signal_ids
    prev_health_score:   float | None
    prev_health_tier:    str | None
    tier_changed:        bool


def compute_model_health_score(
    model_id: str,
    criticality_tier: str,
    signals: list[dict[str, Any]],   # signal_index rows for this model
    prev_score: dict | None,          # previous risk_scores row
    config: dict,
) -> RiskScore:
    """
    Compute health_score and confidence for a single model.
    signals: list of signal_index dicts, already filtered for this model.
    """
    weights = _load_weights(criticality_tier, config)
    now = datetime.now(tz=timezone.utc)

    # --- Aggregate signals by type (take worst = max normalized value) ------
    drift_vals     = [s["latest_value"] for s in signals if s["signal_type"] == "drift"]
    quality_vals   = [s["latest_value"] for s in signals if s["signal_type"] == "accuracy"]
    cost_vals      = [s["latest_value"] for s in signals if s["signal_type"] in ("cost", "usage")]
    guardrail_vals = [s["latest_value"] for s in signals if s["signal_type"] == "guardrail"]
    latency_vals   = [s["latest_value"] for s in signals if s["signal_type"] == "latency"]

    drift_component     = max(drift_vals)     if drift_vals     else None
    quality_component   = min(quality_vals)   if quality_vals   else None   # min accuracy = worst
    cost_component      = max(cost_vals)      if cost_vals      else None
    guardrail_component = max(guardrail_vals) if guardrail_vals else None
    latency_component   = max(latency_vals)   if latency_vals   else None

    # Defaults when component is missing (assume healthy = 0 penalty)
    dc = drift_component     or 0.0
    qc = 1.0 - (quality_component or 1.0)   # invert: 1.0 accuracy = 0 penalty
    cc = cost_component      or 0.0
    gc = guardrail_component or 0.0

    # Weighted sum
    w  = weights
    raw_sum = (
        w["w1_drift"]     * dc +
        w["w2_quality"]   * qc +
        w["w3_cost"]      * cc +
        w["w4_guardrail"] * gc
    )
    raw_sum = max(0.0, min(1.0, raw_sum))   # clamp before criticality multiplier

    health_score = 100.0 - (raw_sum * w["criticality_weight"] * 100.0)
    health_score = round(max(0.0, min(100.0, health_score)), 2)
    tier = _health_tier(health_score, criticality_tier)

    # --- Confidence ---------------------------------------------------------
    signal_types = {s["signal_type"] for s in signals}
    has_accuracy = bool(quality_vals)
    total_traffic = sum(int(s.get("raw_value", 0)) for s in signals if s.get("raw_unit") == "requests_per_hour") * SIGNAL_WINDOW_HOURS
    baseline_days = 14  # assume 14-day baseline; could be enriched from signal metadata

    confidence, factors = _compute_confidence(
        ground_truth_available=has_accuracy,
        signal_types_present=signal_types,
        traffic_volume=max(total_traffic, 10),
        baseline_days=baseline_days,
        config=config,
    )

    # --- Prev score diff ----------------------------------------------------
    prev_hs   = prev_score["health_score"] if prev_score else None
    prev_tier = prev_score["health_tier"]  if prev_score else None
    tier_changed = (prev_tier is not None and prev_tier != tier)

    return RiskScore(
        score_id=str(uuid.uuid4()),
        model_id=model_id,
        computed_at=now,
        drift_component=drift_component,
        quality_component=quality_component,
        cost_component=cost_component,
        guardrail_component=guardrail_component,
        latency_component=latency_component,
        w1_drift=w["w1_drift"],
        w2_quality=w["w2_quality"],
        w3_cost=w["w3_cost"],
        w4_guardrail=w["w4_guardrail"],
        criticality_weight=w["criticality_weight"],
        raw_weighted_sum=round(raw_sum, 6),
        health_score=health_score,
        health_tier=tier,
        confidence=confidence,
        confidence_factors=json.dumps(factors),
        signal_count=len(signals),
        signals_used=json.dumps([s.get("signal_id", "") for s in signals]),
        prev_health_score=prev_hs,
        prev_health_tier=prev_tier,
        tier_changed=tier_changed,
    )


# ---------------------------------------------------------------------------
# Mock data for scoring
# ---------------------------------------------------------------------------
def _generate_mock_signals_for_scoring() -> list[dict[str, Any]]:
    """Returns a flat list of mock signal_index rows with model_id set."""
    from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
    mock_registry = _generate_mock_registry()
    mid_credit  = _make_model_id("main.finance.credit_risk_classifier", "12")
    mid_demand  = _make_model_id("main.operations.demand_forecaster", "5")
    mid_churn   = _make_model_id("main.customer.churn_predictor", "3")
    mid_fraud   = _make_model_id("main.finance.fraud_detection_agent", "2")

    now = datetime.now(tz=timezone.utc)
    return [
        # credit risk — high drift, degraded accuracy, cost spike, PII violations
        {"signal_id": "s001", "model_id": mid_credit, "signal_type": "drift",     "latest_value": 0.73, "raw_value": 0.73, "raw_unit": "drift_score", "ingested_at": now.isoformat()},
        {"signal_id": "s002", "model_id": mid_credit, "signal_type": "accuracy",  "latest_value": 0.831,"raw_value": 0.831,"raw_unit": "accuracy_score", "ingested_at": now.isoformat()},
        {"signal_id": "s003", "model_id": mid_credit, "signal_type": "cost",      "latest_value": 0.92, "raw_value": 47.8, "raw_unit": "usd", "ingested_at": now.isoformat()},
        {"signal_id": "s004", "model_id": mid_credit, "signal_type": "guardrail", "latest_value": 0.72, "raw_value": 45.0, "raw_unit": "violation_count", "ingested_at": now.isoformat()},
        {"signal_id": "s005", "model_id": mid_credit, "signal_type": "usage",     "latest_value": 0.12, "raw_value": 1250, "raw_unit": "requests_per_hour", "ingested_at": now.isoformat()},
        # demand forecaster — moderate drift only
        {"signal_id": "s006", "model_id": mid_demand, "signal_type": "drift",     "latest_value": 0.31, "raw_value": 0.31, "raw_unit": "drift_score", "ingested_at": now.isoformat()},
        {"signal_id": "s007", "model_id": mid_demand, "signal_type": "accuracy",  "latest_value": 0.942,"raw_value": 0.942,"raw_unit": "accuracy_score", "ingested_at": now.isoformat()},
        {"signal_id": "s008", "model_id": mid_demand, "signal_type": "usage",     "latest_value": 0.03, "raw_value": 310,  "raw_unit": "requests_per_hour", "ingested_at": now.isoformat()},
        # churn predictor — very high drift, no monitor, no accuracy signal
        {"signal_id": "s009", "model_id": mid_churn,  "signal_type": "drift",     "latest_value": 0.85, "raw_value": 0.85, "raw_unit": "drift_score", "ingested_at": now.isoformat()},
        {"signal_id": "s010", "model_id": mid_churn,  "signal_type": "usage",     "latest_value": 0.018,"raw_value": 185,  "raw_unit": "requests_per_hour", "ingested_at": now.isoformat()},
        # fraud agent — moderate overall, prompt injection attacks
        {"signal_id": "s011", "model_id": mid_fraud,  "signal_type": "accuracy",  "latest_value": 0.887,"raw_value": 0.887,"raw_unit": "accuracy_score", "ingested_at": now.isoformat()},
        {"signal_id": "s012", "model_id": mid_fraud,  "signal_type": "guardrail", "latest_value": 0.48, "raw_value": 12.0, "raw_unit": "violation_count", "ingested_at": now.isoformat()},
        {"signal_id": "s013", "model_id": mid_fraud,  "signal_type": "latency",   "latest_value": 0.1,  "raw_value": 0.05, "raw_unit": "breach_rate", "ingested_at": now.isoformat()},
        {"signal_id": "s014", "model_id": mid_fraud,  "signal_type": "usage",     "latest_value": 0.245,"raw_value": 2450, "raw_unit": "requests_per_hour", "ingested_at": now.isoformat()},
    ]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_scoring(dry_run: bool = False) -> dict[str, Any]:
    config = yaml.safe_load(open(RISK_WEIGHTS_PATH))

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic signals.")
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        models = [
            {
                "model_id": _make_model_id(e["name"], str(e["version"])),
                "model_name": e["name"],
                "criticality_tier":
                    "tier_1_business" if e.get("business_domain") in ("finance","fraud","customer") else "tier_2_operational",
            }
            for e in mock_registry
        ]
        all_signals = _generate_mock_signals_for_scoring()
        prev_scores: dict[str, dict] = {}
    else:
        spark = SparkSession.builder.getOrCreate()
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=SIGNAL_WINDOW_HOURS)).isoformat()
        models_rows = spark.sql(f"""
            SELECT model_id, model_name, criticality_tier
            FROM {REGISTRY_MAP_TABLE} WHERE is_active = true
        """).collect()
        models = [r.asDict() for r in models_rows]

        signals_rows = spark.sql(f"""
            SELECT * FROM {SIGNAL_INDEX_TABLE}
            WHERE ingested_at >= '{cutoff}'
        """).collect()
        all_signals = [r.asDict() for r in signals_rows]

        prev_rows = spark.sql(f"""
            SELECT model_id, health_score, health_tier
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
                FROM {RISK_SCORES_TABLE}
            ) WHERE rn = 1
        """).collect()
        prev_scores = {r["model_id"]: r.asDict() for r in prev_rows}

    # Group signals by model_id
    from collections import defaultdict
    signals_by_model: dict[str, list[dict]] = defaultdict(list)
    for s in all_signals:
        signals_by_model[s["model_id"]].append(s)

    # Compute score for each model
    computed_scores: list[RiskScore] = []
    for model in models:
        model_id = model["model_id"]
        tier     = model.get("criticality_tier", "tier_2_operational")
        signals  = signals_by_model.get(model_id, [])
        prev     = prev_scores.get(model_id)

        score = compute_model_health_score(model_id, tier, signals, prev, config)
        computed_scores.append(score)
        logger.info(
            "Scored %s | health=%.1f | tier=%s | confidence=%.2f | signals=%d | prev_tier=%s | changed=%s",
            model["model_name"], score.health_score, score.health_tier, score.confidence,
            score.signal_count, score.prev_health_tier, score.tier_changed,
        )

    stats = {
        "models_scored": len(computed_scores),
        "tier_changes": sum(1 for s in computed_scores if s.tier_changed),
        "critical_count": sum(1 for s in computed_scores if s.health_tier == "critical"),
        "at_risk_count":  sum(1 for s in computed_scores if s.health_tier == "at_risk"),
        "healthy_count":  sum(1 for s in computed_scores if s.health_tier == "healthy"),
        "avg_health_score": round(sum(s.health_score for s in computed_scores) / max(len(computed_scores), 1), 2),
    }

    if not MOCK_MODE and not dry_run:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(s) for s in computed_scores]
        df = spark.createDataFrame(rows)
        df.write.mode("append").saveAsTable(RISK_SCORES_TABLE)
        logger.info("Wrote %d risk scores to %s.", len(computed_scores), RISK_SCORES_TABLE)

    logger.info("Scoring complete: %s", stats)
    return {"stats": stats, "scores": [asdict(s) for s in computed_scores]}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    result = run_scoring(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
    print("\nModel scores:")
    for sc in result["scores"]:
        print(f"  {sc['model_id'][:8]}  health={sc['health_score']:.1f}  tier={sc['health_tier']:10s}  confidence={sc['confidence']:.2f}  signals={sc['signal_count']}")
