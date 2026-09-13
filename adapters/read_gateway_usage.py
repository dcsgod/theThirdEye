"""
adapters/read_gateway_usage.py
================================
Third Eye — Unity Gateway Usage & Cost Signal Adapter

Reads:
  - system.serving.endpoint_usage   → cost/usage signals
  - system.serving.served_entities  → endpoint metadata enrichment
  - Unified Trace Table (Unity Gateway, OpenTelemetry format) → latency/token signals

Normalizes values into governance.model_health.signal_index.
Does NOT recompute cost — reads Databricks' native system tables. (Section 1)

Run as: Databricks Workflow task (after signal adapter batch).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from typing import Any

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.adapters.gateway_usage")

REGISTRY_MAP_TABLE = "governance.model_health.model_registry_map"
SIGNAL_INDEX_TABLE = "governance.model_health.signal_index"

# Unity Gateway system tables
ENDPOINT_USAGE_TABLE  = "system.serving.endpoint_usage"
SERVED_ENTITIES_TABLE = "system.serving.served_entities"
UNIFIED_TRACE_TABLE   = "system.serving.served_entities"   # adjust to actual UC trace table path

LOOKBACK_HOURS       = int(os.getenv("THIRD_EYE_SIGNAL_LOOKBACK_HOURS", "25"))
ROLLING_BASELINE_DAYS = 14   # z-score baseline window
Z_SCORE_CAP           = 3.0  # normalized value = 1.0 when z ≥ this
P99_LATENCY_THRESHOLD = float(os.getenv("THIRD_EYE_P99_LATENCY_MS", "5000"))


# ---------------------------------------------------------------------------
# Mock data
# ---------------------------------------------------------------------------
def _generate_mock_endpoint_usage() -> list[dict[str, Any]]:
    """Synthetic endpoint_usage rows for the last 24h + 14-day baseline."""
    now = datetime.now(tz=timezone.utc)
    import random
    random.seed(42)

    rows = []
    # Historical baseline (14 days × 24 hours)
    for day in range(1, 15):
        for hour in range(24):
            ts = now - timedelta(days=day, hours=hour)
            rows.append({"endpoint_name": "prod-credit-risk-v2",  "window_start": ts.isoformat(), "total_token_cost": round(random.uniform(8.0, 12.0), 2),  "total_requests": random.randint(900, 1100),  "total_tokens": random.randint(500_000, 700_000)})
            rows.append({"endpoint_name": "prod-demand-forecast",  "window_start": ts.isoformat(), "total_token_cost": round(random.uniform(2.0, 4.0), 2),   "total_requests": random.randint(200, 400),   "total_tokens": random.randint(100_000, 200_000)})
            rows.append({"endpoint_name": "prod-churn-v1",         "window_start": ts.isoformat(), "total_token_cost": round(random.uniform(1.0, 3.0), 2),   "total_requests": random.randint(100, 300),   "total_tokens": random.randint(50_000, 150_000)})
            rows.append({"endpoint_name": "prod-fraud-gateway",    "window_start": ts.isoformat(), "total_token_cost": round(random.uniform(15.0, 20.0), 2), "total_requests": random.randint(2000, 3000), "total_tokens": random.randint(1_000_000, 1_500_000)})

    # Current window — credit risk endpoint has a cost spike (anomaly)
    rows.append({"endpoint_name": "prod-credit-risk-v2", "window_start": (now - timedelta(hours=1)).isoformat(), "total_token_cost": 47.8,   "total_requests": 1250, "total_tokens": 2_800_000})  # spike!
    rows.append({"endpoint_name": "prod-demand-forecast","window_start": (now - timedelta(hours=1)).isoformat(), "total_token_cost": 3.1,    "total_requests": 310,  "total_tokens": 155_000})
    rows.append({"endpoint_name": "prod-churn-v1",       "window_start": (now - timedelta(hours=1)).isoformat(), "total_token_cost": 2.2,    "total_requests": 185,  "total_tokens": 92_000})
    rows.append({"endpoint_name": "prod-fraud-gateway",  "window_start": (now - timedelta(hours=1)).isoformat(), "total_token_cost": 18.5,   "total_requests": 2450, "total_tokens": 1_225_000})
    return rows


def _generate_mock_trace_data() -> list[dict[str, Any]]:
    """Synthetic Unified Trace Table rows for latency analysis."""
    now = datetime.now(tz=timezone.utc)
    import random
    random.seed(99)

    rows = []
    for i in range(200):
        ts = now - timedelta(minutes=random.randint(1, 1440))
        # credit-risk endpoint has high latency on some requests
        latency = random.uniform(200, 8500) if i < 30 else random.uniform(100, 600)
        rows.append({
            "endpoint_name": "prod-credit-risk-v2",
            "request_time":  ts.isoformat(),
            "response_time_ms": round(latency, 1),
            "status_code": 200 if latency < 6000 else 500,
            "total_tokens": random.randint(300, 2000),
            "model_version": "12",
        })
    for i in range(500):
        ts = now - timedelta(minutes=random.randint(1, 1440))
        rows.append({
            "endpoint_name": "prod-demand-forecast",
            "request_time":  ts.isoformat(),
            "response_time_ms": round(random.uniform(80, 450), 1),
            "status_code": 200,
            "total_tokens": random.randint(100, 500),
            "model_version": "5",
        })
    return rows


# ---------------------------------------------------------------------------
# Z-score normalization for cost anomalies
# ---------------------------------------------------------------------------
def _z_score_normalize(current: float, baseline_values: list[float], cap: float = Z_SCORE_CAP) -> float:
    if len(baseline_values) < 2:
        return 0.0
    mu = mean(baseline_values)
    sigma = stdev(baseline_values)
    if sigma == 0:
        return 0.0
    z = (current - mu) / sigma
    return max(0.0, min(1.0, z / cap))  # clamp to [0, 1]


# ---------------------------------------------------------------------------
# Latency: fraction of requests exceeding P99 threshold
# ---------------------------------------------------------------------------
def _latency_breach_rate(latency_values: list[float], threshold_ms: float = P99_LATENCY_THRESHOLD) -> float:
    if not latency_values:
        return 0.0
    breaches = sum(1 for v in latency_values if v > threshold_ms)
    return breaches / len(latency_values)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
@dataclass
class GatewaySignal:
    signal_id: str
    model_id: str
    signal_type: str
    signal_subtype: str | None
    source_table: str
    computed_at: datetime
    ingested_at: datetime
    latest_value: float
    raw_value: float
    raw_unit: str
    raw_reference: str
    column_name: str | None
    window_start: datetime | None
    window_end: datetime | None
    threshold_value: float
    threshold_breached: bool
    adapter_version: str = "1.0.0"


def process_gateway_usage(
    usage_rows: list[dict],
    trace_rows: list[dict],
    endpoint_to_model: dict[str, str],  # endpoint_name → model_id
    cost_threshold_z: float = 2.0,      # z-score threshold for cost anomaly alert
    latency_threshold_ms: float = P99_LATENCY_THRESHOLD,
    latency_breach_rate_threshold: float = 0.05,
) -> list[GatewaySignal]:
    """Process gateway usage and trace rows into normalized signals."""
    now = datetime.now(tz=timezone.utc)
    signals: list[GatewaySignal] = []
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    baseline_cutoff = now - timedelta(days=ROLLING_BASELINE_DAYS)

    # Group usage by endpoint
    from collections import defaultdict
    endpoint_baseline: dict[str, list[float]] = defaultdict(list)
    endpoint_recent: dict[str, list[dict]]    = defaultdict(list)

    for row in usage_rows:
        ts_str = row["window_start"]
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        ep = row["endpoint_name"]
        if ts >= cutoff:
            endpoint_recent[ep].append(row)
        elif ts >= baseline_cutoff:
            endpoint_baseline[ep].append(float(row.get("total_token_cost", 0)))

    # Cost signals
    for ep, recent_rows in endpoint_recent.items():
        model_id = endpoint_to_model.get(ep)
        if not model_id:
            continue
        baseline = endpoint_baseline.get(ep, [])
        for row in recent_rows:
            cost = float(row.get("total_token_cost", 0))
            norm = _z_score_normalize(cost, baseline)
            ts = datetime.fromisoformat(row["window_start"].replace("Z", "+00:00"))
            signals.append(GatewaySignal(
                signal_id=str(uuid.uuid4()),
                model_id=model_id,
                signal_type="cost",
                signal_subtype="token_cost",
                source_table=ENDPOINT_USAGE_TABLE,
                computed_at=ts,
                ingested_at=now,
                latest_value=norm,
                raw_value=cost,
                raw_unit="usd",
                raw_reference=json.dumps({"endpoint_name": ep, "window_start": row["window_start"]}),
                column_name=None,
                window_start=ts,
                window_end=ts + timedelta(hours=1),
                threshold_value=cost_threshold_z / Z_SCORE_CAP,
                threshold_breached=(norm >= cost_threshold_z / Z_SCORE_CAP),
            ))

            # Request volume signal
            req_count = int(row.get("total_requests", 0))
            req_norm  = min(1.0, max(0.0, req_count / 10_000))  # normalize to 10k req/hr
            signals.append(GatewaySignal(
                signal_id=str(uuid.uuid4()),
                model_id=model_id,
                signal_type="usage",
                signal_subtype="request_volume",
                source_table=ENDPOINT_USAGE_TABLE,
                computed_at=ts,
                ingested_at=now,
                latest_value=req_norm,
                raw_value=float(req_count),
                raw_unit="requests_per_hour",
                raw_reference=json.dumps({"endpoint_name": ep, "window_start": row["window_start"]}),
                column_name=None,
                window_start=ts,
                window_end=ts + timedelta(hours=1),
                threshold_value=0.9,
                threshold_breached=(req_norm >= 0.9),
            ))

    # Latency signals from trace data
    endpoint_latencies: dict[str, list[float]] = defaultdict(list)
    for row in trace_rows:
        ts_str = row.get("request_time", "")
        if ts_str:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts >= cutoff:
                endpoint_latencies[row["endpoint_name"]].append(float(row.get("response_time_ms", 0)))

    for ep, latencies in endpoint_latencies.items():
        model_id = endpoint_to_model.get(ep)
        if not model_id or not latencies:
            continue
        breach_rate = _latency_breach_rate(latencies, latency_threshold_ms)
        norm = min(1.0, breach_rate / latency_breach_rate_threshold)
        signals.append(GatewaySignal(
            signal_id=str(uuid.uuid4()),
            model_id=model_id,
            signal_type="latency",
            signal_subtype="p99_breach_rate",
            source_table="unified_trace_table",
            computed_at=now,
            ingested_at=now,
            latest_value=norm,
            raw_value=breach_rate,
            raw_unit="breach_rate",
            raw_reference=json.dumps({"endpoint_name": ep, "sample_count": len(latencies), "threshold_ms": latency_threshold_ms}),
            column_name=None,
            window_start=cutoff,
            window_end=now,
            threshold_value=latency_breach_rate_threshold,
            threshold_breached=(breach_rate >= latency_breach_rate_threshold),
        ))

    return signals


def run_adapter(dry_run: bool = False) -> dict[str, int]:
    now = datetime.now(tz=timezone.utc)

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic gateway usage data.")
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        endpoint_to_model = {
            e["serving_endpoint"]: _make_model_id(e["name"], str(e["version"]))
            for e in mock_registry if e.get("serving_endpoint")
        }
        usage_rows = _generate_mock_endpoint_usage()
        trace_rows = _generate_mock_trace_data()
    else:
        spark = SparkSession.builder.getOrCreate()
        reg_rows = spark.sql(f"""
            SELECT model_id, serving_endpoint FROM {REGISTRY_MAP_TABLE}
            WHERE is_active = true AND serving_endpoint IS NOT NULL
        """).collect()
        endpoint_to_model = {r["serving_endpoint"]: r["model_id"] for r in reg_rows}
        endpoints_str = ", ".join(f"'{ep}'" for ep in endpoint_to_model)

        usage_rows_raw = spark.sql(f"""
            SELECT endpoint_name, CAST(window_start AS STRING) AS window_start,
                   total_token_cost, total_requests, total_tokens
            FROM {ENDPOINT_USAGE_TABLE}
            WHERE endpoint_name IN ({endpoints_str})
              AND window_start >= DATEADD(DAY, -{ROLLING_BASELINE_DAYS}, CURRENT_TIMESTAMP())
            ORDER BY window_start DESC
        """).collect()
        usage_rows = [r.asDict() for r in usage_rows_raw]

        try:
            trace_rows_raw = spark.sql(f"""
                SELECT endpoint_name, CAST(request_time AS STRING) AS request_time,
                       response_time_ms, status_code, total_tokens, model_version
                FROM system.serving.unified_trace
                WHERE endpoint_name IN ({endpoints_str})
                  AND request_time >= DATEADD(HOUR, -{LOOKBACK_HOURS}, CURRENT_TIMESTAMP())
                LIMIT 50000
            """).collect()
            trace_rows = [r.asDict() for r in trace_rows_raw]
        except Exception as e:
            logger.warning("Could not read Unified Trace Table (non-fatal): %s", e)
            trace_rows = []

    signals = process_gateway_usage(
        usage_rows=usage_rows,
        trace_rows=trace_rows,
        endpoint_to_model=endpoint_to_model,
    )

    breached = [s for s in signals if s.threshold_breached]
    stats = {"signals_processed": len(signals), "threshold_breached": len(breached)}
    logger.info("Gateway usage adapter stats: %s", stats)

    if MOCK_MODE or dry_run:
        for s in signals:
            logger.info("  [%s] model=%s | %s/%s | raw=%.3f | norm=%.3f | breach=%s",
                        s.signal_id[:8], s.model_id[:8], s.signal_type, s.signal_subtype,
                        s.raw_value, s.latest_value, s.threshold_breached)
    else:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(s) for s in signals]
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
