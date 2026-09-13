"""
adapters/read_lakehouse_monitoring.py
======================================
Third Eye — Lakehouse Monitoring Signal Adapter

Reads Databricks Lakehouse Monitoring output tables:
  - {monitor_schema}.{table}_profile_metrics  → accuracy/quality signals
  - {monitor_schema}.{table}_drift_metrics    → drift signals

IMPORTANT: This adapter does NOT recompute drift or accuracy statistics.
It reads Databricks' own computed output and normalizes the values into
governance.model_health.signal_index. (See Section 1, thirdeye_project.md)

Run as: Databricks Workflow task (after each Lakehouse Monitoring refresh cycle).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
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
logger = logging.getLogger("third_eye.adapters.lakehouse_monitoring")

REGISTRY_MAP_TABLE = "governance.model_health.model_registry_map"
SIGNAL_INDEX_TABLE = "governance.model_health.signal_index"
RISK_WEIGHTS_PATH  = os.path.join(os.path.dirname(__file__), "..", "config", "risk_weights.yaml")

LOOKBACK_HOURS = int(os.getenv("THIRD_EYE_SIGNAL_LOOKBACK_HOURS", "25"))  # slightly > 24 to handle timing variance


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def _load_risk_weights() -> dict[str, Any]:
    with open(RISK_WEIGHTS_PATH, "r") as f:
        return yaml.safe_load(f)


def _normalize_drift(raw_drift_score: float, config: dict) -> float:
    """
    Clamp and scale drift score to [0, 1].
    Raw drift score from Lakehouse Monitoring is already 0–1 (PSI/KS-based).
    Values > 1 are clamped.
    """
    min_raw = config["normalization"]["drift"]["min_raw"]
    max_raw = config["normalization"]["drift"]["max_raw"]
    clamped = max(min_raw, min(max_raw, raw_drift_score))
    return (clamped - min_raw) / (max_raw - min_raw) if max_raw > min_raw else 0.0


def _normalize_accuracy(raw_accuracy: float) -> float:
    """
    Accuracy is already 0–1 (higher = better). Stored as-is in quality_component.
    The scoring formula uses (1 - quality_component) so 1.0 accuracy = 0 penalty.
    """
    return max(0.0, min(1.0, raw_accuracy))


# ---------------------------------------------------------------------------
# Mock monitoring data
# ---------------------------------------------------------------------------
def _generate_mock_profile_metrics() -> list[dict[str, Any]]:
    """Synthetic profile_metrics table rows for testing."""
    now = datetime.now(tz=timezone.utc)
    return [
        # credit_risk_classifier — slightly degraded accuracy
        {
            "model_id_ref": "main.finance.credit_risk_classifier:12",
            "monitor_schema": "main.finance_monitoring",
            "inference_table": "main.finance.credit_risk_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "metric_type":  "accuracy_score",
            "metric_value": 0.831,   # was 0.91 — degraded
            "model_version": "12",
            "slice_key": None,
            "slice_value": None,
        },
        # demand_forecaster — good accuracy
        {
            "model_id_ref": "main.operations.demand_forecaster:5",
            "monitor_schema": "main.ops_monitoring",
            "inference_table": "main.operations.demand_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "metric_type":  "accuracy_score",
            "metric_value": 0.942,
            "model_version": "5",
            "slice_key": None,
            "slice_value": None,
        },
        # fraud_detection_agent — fairness/slice issue (silent failure scenario)
        {
            "model_id_ref": "main.finance.fraud_detection_agent:2",
            "monitor_schema": "main.fraud_monitoring",
            "inference_table": "system.serving.unified_trace_fraud",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "metric_type":  "accuracy_score",
            "metric_value": 0.887,   # aggregate looks fine ...
            "model_version": "2",
            "slice_key": None,
            "slice_value": None,
        },
        # fraud_detection_agent — but slice is bad
        {
            "model_id_ref": "main.finance.fraud_detection_agent:2",
            "monitor_schema": "main.fraud_monitoring",
            "inference_table": "system.serving.unified_trace_fraud",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "metric_type":  "accuracy_score",
            "metric_value": 0.512,   # ← slice has collapsed (silent failure)
            "model_version": "2",
            "slice_key": "customer_segment",
            "slice_value": "premium",
        },
    ]


def _generate_mock_drift_metrics() -> list[dict[str, Any]]:
    """Synthetic drift_metrics table rows for testing."""
    now = datetime.now(tz=timezone.utc)
    return [
        # credit_risk_classifier — high drift on income_level (upstream schema changed)
        {
            "model_id_ref": "main.finance.credit_risk_classifier:12",
            "monitor_schema": "main.finance_monitoring",
            "inference_table": "main.finance.credit_risk_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "column_name":  "income_level",
            "drift_type":   "consecutive",
            "drift_score":  0.73,   # ← HIGH drift — threshold typically 0.2
        },
        {
            "model_id_ref": "main.finance.credit_risk_classifier:12",
            "monitor_schema": "main.finance_monitoring",
            "inference_table": "main.finance.credit_risk_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "column_name":  "credit_score_band",
            "drift_type":   "consecutive",
            "drift_score":  0.18,   # below threshold
        },
        # demand_forecaster — moderate drift
        {
            "model_id_ref": "main.operations.demand_forecaster:5",
            "monitor_schema": "main.ops_monitoring",
            "inference_table": "main.operations.demand_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "column_name":  "product_category",
            "drift_type":   "consecutive",
            "drift_score":  0.31,   # moderate drift
        },
        # churn_predictor — extreme drift (large upstream write)
        {
            "model_id_ref": "main.customer.churn_predictor:3",
            "monitor_schema": None,  # no monitor yet — this won't fire in live mode
            "inference_table": "main.customer.churn_inference_log",
            "window_start": (now - timedelta(hours=24)).isoformat(),
            "window_end":   now.isoformat(),
            "column_name":  "tenure_months",
            "drift_type":   "consecutive",
            "drift_score":  0.85,   # very high
        },
    ]


# ---------------------------------------------------------------------------
# Signal normalization
# ---------------------------------------------------------------------------
@dataclass
class NormalizedSignal:
    signal_id: str
    model_id: str
    signal_type: str            # drift | accuracy
    signal_subtype: str | None  # consecutive | baseline | slice
    source_table: str
    computed_at: datetime
    ingested_at: datetime
    latest_value: float         # normalized 0–1 (higher = worse)
    raw_value: float
    raw_unit: str
    raw_reference: str          # JSON pointer back to native record
    column_name: str | None
    window_start: datetime | None
    window_end: datetime | None
    threshold_value: float
    threshold_breached: bool
    adapter_version: str = "1.0.0"


def _model_id_from_ref(model_id_ref: str, model_id_map: dict[str, str]) -> str | None:
    """Look up the governance model_id from the raw model ref string."""
    # Direct lookup first
    if model_id_ref in model_id_map:
        return model_id_map[model_id_ref]
    # Try fuzzy match by model name prefix
    model_name = model_id_ref.rsplit(":", 1)[0]
    for ref_key, mid in model_id_map.items():
        if ref_key.startswith(model_name):
            return mid
    return None


def process_lakehouse_monitoring(
    profile_rows: list[dict],
    drift_rows: list[dict],
    model_id_map: dict[str, str],  # inference_table → model_id
    config: dict,
    drift_threshold_by_tier: dict[str, float],
    accuracy_threshold_by_tier: dict[str, float],
    tier_by_model: dict[str, str],
) -> list[NormalizedSignal]:
    """
    Convert raw monitoring rows into NormalizedSignal objects.
    """
    now = datetime.now(tz=timezone.utc)
    signals: list[NormalizedSignal] = []

    # -- Accuracy / profile metrics -----------------------------------------
    for row in profile_rows:
        model_id = _model_id_from_ref(row["model_id_ref"], model_id_map)
        if not model_id:
            logger.warning("Could not match profile metric to model: %s", row.get("model_id_ref"))
            continue
        if row.get("metric_type") != "accuracy_score":
            continue

        raw_acc = float(row["metric_value"])
        normalized = _normalize_accuracy(raw_acc)
        tier = tier_by_model.get(model_id, "tier_2_operational")
        threshold = accuracy_threshold_by_tier.get(tier, 0.7)
        source = f"{row['monitor_schema']}.{row['inference_table'].split('.')[-1]}_profile_metrics"

        signals.append(NormalizedSignal(
            signal_id=str(uuid.uuid4()),
            model_id=model_id,
            signal_type="accuracy",
            signal_subtype=f"slice:{row['slice_key']}={row['slice_value']}" if row.get("slice_key") else None,
            source_table=source,
            computed_at=datetime.fromisoformat(row["window_end"].replace("Z", "+00:00")),
            ingested_at=now,
            latest_value=normalized,
            raw_value=raw_acc,
            raw_unit="accuracy_score",
            raw_reference=json.dumps({
                "monitor_schema": row["monitor_schema"],
                "inference_table": row["inference_table"],
                "window_end": row["window_end"],
                "metric_type": "accuracy_score",
                "slice_key": row.get("slice_key"),
            }),
            column_name=None,
            window_start=datetime.fromisoformat(row["window_start"].replace("Z", "+00:00")),
            window_end=datetime.fromisoformat(row["window_end"].replace("Z", "+00:00")),
            threshold_value=threshold,
            threshold_breached=(raw_acc < threshold),
        ))

    # -- Drift metrics -------------------------------------------------------
    for row in drift_rows:
        model_id = _model_id_from_ref(row["model_id_ref"], model_id_map)
        if not model_id:
            continue

        raw_drift = float(row["drift_score"])
        normalized = _normalize_drift(raw_drift, config)
        tier = tier_by_model.get(model_id, "tier_2_operational")
        threshold = drift_threshold_by_tier.get(tier, 0.2)
        source = f"{row['monitor_schema']}.{row['inference_table'].split('.')[-1]}_drift_metrics" if row.get("monitor_schema") else "lakehouse_monitoring.drift_metrics"

        signals.append(NormalizedSignal(
            signal_id=str(uuid.uuid4()),
            model_id=model_id,
            signal_type="drift",
            signal_subtype=row.get("drift_type", "consecutive"),
            source_table=source,
            computed_at=datetime.fromisoformat(row["window_end"].replace("Z", "+00:00")),
            ingested_at=now,
            latest_value=normalized,
            raw_value=raw_drift,
            raw_unit="drift_score",
            raw_reference=json.dumps({
                "monitor_schema": row.get("monitor_schema"),
                "inference_table": row["inference_table"],
                "column_name": row.get("column_name"),
                "drift_type": row.get("drift_type"),
                "window_end": row["window_end"],
            }),
            column_name=row.get("column_name"),
            window_start=datetime.fromisoformat(row["window_start"].replace("Z", "+00:00")),
            window_end=datetime.fromisoformat(row["window_end"].replace("Z", "+00:00")),
            threshold_value=threshold,
            threshold_breached=(raw_drift > threshold),
        ))

    return signals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_adapter(dry_run: bool = False) -> dict[str, int]:
    now = datetime.now(tz=timezone.utc)
    config = _load_risk_weights()

    # Tier-specific thresholds (could be extended per-model via config table)
    drift_thresholds    = {"tier_0_experimental": 0.3, "tier_1_business": 0.15, "tier_2_operational": 0.2, "tier_3_low": 0.25}
    accuracy_thresholds = {"tier_0_experimental": 0.5, "tier_1_business": 0.75, "tier_2_operational": 0.7, "tier_3_low": 0.6}

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic monitoring data.")
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        # Build model_id_map: "model_name:version" → model_id
        model_id_map = {
            f"{e['name']}:{e['version']}": _make_model_id(e["name"], str(e["version"]))
            for e in mock_registry
        }
        tier_by_model = {
            _make_model_id(e["name"], str(e["version"])): "tier_1_business"
            if e.get("business_domain") in ("finance", "fraud", "customer") else "tier_2_operational"
            for e in mock_registry
        }
        profile_rows = _generate_mock_profile_metrics()
        drift_rows   = _generate_mock_drift_metrics()
    else:
        spark = SparkSession.builder.getOrCreate()
        registry_rows = spark.sql(f"""
            SELECT model_id, model_name, model_version, criticality_tier,
                   inference_table, lakehouse_monitor_schema
            FROM {REGISTRY_MAP_TABLE}
            WHERE is_active = true AND lakehouse_monitor_configured = true
        """).collect()

        model_id_map = {f"{r['model_name']}:{r['model_version']}": r["model_id"] for r in registry_rows}
        tier_by_model = {r["model_id"]: r["criticality_tier"] for r in registry_rows}

        # Read each model's drift/profile tables from its monitor schema
        profile_rows_all, drift_rows_all = [], []
        for row in registry_rows:
            if not row["lakehouse_monitor_schema"] or not row["inference_table"]:
                continue
            table_base = row["inference_table"].split(".")[-1]
            schema     = row["lakehouse_monitor_schema"]
            try:
                prof_df = spark.sql(f"""
                    SELECT
                        '{row["model_name"]}:{row["model_version"]}' AS model_id_ref,
                        '{schema}' AS monitor_schema,
                        '{row["inference_table"]}' AS inference_table,
                        CAST(window_start AS STRING) AS window_start,
                        CAST(window_end AS STRING) AS window_end,
                        'accuracy_score' AS metric_type,
                        accuracy AS metric_value,
                        model_id AS model_version,
                        NULL AS slice_key, NULL AS slice_value
                    FROM {schema}.{table_base}_profile_metrics
                    WHERE window_end >= DATEADD(HOUR, -{LOOKBACK_HOURS}, CURRENT_TIMESTAMP())
                    ORDER BY window_end DESC LIMIT 50
                """)
                profile_rows_all.extend([r.asDict() for r in prof_df.collect()])
            except Exception as e:
                logger.warning("Could not read profile metrics for %s: %s", row["model_name"], e)

            try:
                drift_df = spark.sql(f"""
                    SELECT
                        '{row["model_name"]}:{row["model_version"]}' AS model_id_ref,
                        '{schema}' AS monitor_schema,
                        '{row["inference_table"]}' AS inference_table,
                        CAST(window_start AS STRING) AS window_start,
                        CAST(window_end AS STRING) AS window_end,
                        column_name,
                        'consecutive' AS drift_type,
                        drift_score
                    FROM {schema}.{table_base}_drift_metrics
                    WHERE window_end >= DATEADD(HOUR, -{LOOKBACK_HOURS}, CURRENT_TIMESTAMP())
                    ORDER BY window_end DESC LIMIT 500
                """)
                drift_rows_all.extend([r.asDict() for r in drift_df.collect()])
            except Exception as e:
                logger.warning("Could not read drift metrics for %s: %s", row["model_name"], e)

        profile_rows = profile_rows_all
        drift_rows   = drift_rows_all

    signals = process_lakehouse_monitoring(
        profile_rows=profile_rows,
        drift_rows=drift_rows,
        model_id_map=model_id_map,
        config=config,
        drift_threshold_by_tier=drift_thresholds,
        accuracy_threshold_by_tier=accuracy_thresholds,
        tier_by_model=tier_by_model,
    )

    breached = [s for s in signals if s.threshold_breached]
    stats = {"signals_processed": len(signals), "threshold_breached": len(breached)}
    logger.info("Lakehouse Monitoring adapter stats: %s", stats)

    if MOCK_MODE or dry_run:
        for s in signals:
            logger.info("  [%s] model=%s | %s/%s | raw=%.3f | norm=%.3f | breach=%s | col=%s",
                        s.signal_id[:8], s.model_id[:8], s.signal_type, s.signal_subtype,
                        s.raw_value, s.latest_value, s.threshold_breached, s.column_name)
    else:
        import pyspark.sql.types as T
        rows = [asdict(s) for s in signals]
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rows)
        df.write.mode("append").saveAsTable(SIGNAL_INDEX_TABLE)
        logger.info("Wrote %d signals to %s.", len(signals), SIGNAL_INDEX_TABLE)

    return stats


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    stats = run_adapter(dry_run=True)
    print(json.dumps(stats, indent=2))
