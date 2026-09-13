"""
adapters/read_guardrail_events.py
===================================
Third Eye — Unity Gateway AI Guardrails Signal Adapter

Reads Unity Gateway AI Guardrail violation events and normalizes them into
governance.model_health.signal_index with signal_type = 'guardrail'.

IMPORTANT: This adapter does NOT reimplement PII detection or content filtering.
It reads violation events that Unity Gateway has already produced. (Section 1)

This also removes scope overlap from a separate dbx-guardrails project —
reuse the guardrail events as a risk signal rather than rebuilding detection.

Run as: Databricks Workflow task (parallel with other adapters).
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

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.adapters.guardrail_events")

REGISTRY_MAP_TABLE = "governance.model_health.model_registry_map"
SIGNAL_INDEX_TABLE = "governance.model_health.signal_index"

# Unity Gateway guardrail violation events table
# The actual path depends on workspace configuration — adjust if using a custom output schema
GUARDRAIL_EVENTS_TABLE = os.getenv("THIRD_EYE_GUARDRAIL_TABLE", "system.serving.guardrail_events")

LOOKBACK_HOURS = int(os.getenv("THIRD_EYE_SIGNAL_LOOKBACK_HOURS", "25"))

# Guardrail violation rate cap: violations per 1000 requests → normalized to 1.0
# (from risk_weights.yaml: guardrail.violation_rate_cap = 50)
VIOLATION_RATE_CAP_PER_1000 = float(os.getenv("THIRD_EYE_GUARDRAIL_RATE_CAP", "50.0"))

# Types of guardrail violations tracked as risk signals
VIOLATION_SUBTYPES = [
    "pii_detected",
    "unsafe_content",
    "prompt_injection",
    "off_topic",
    "copyright_violation",
    "hallucination_flagged",
]


# ---------------------------------------------------------------------------
# Mock guardrail event data
# ---------------------------------------------------------------------------
def _generate_mock_guardrail_events() -> list[dict[str, Any]]:
    """Synthetic guardrail violation events for MOCK_MODE."""
    now = datetime.now(tz=timezone.utc)
    import random
    random.seed(77)

    events = []
    # credit-risk endpoint: some PII violations (input contained raw SSNs)
    for i in range(45):
        ts = now - timedelta(hours=random.uniform(0.1, 20))
        events.append({
            "endpoint_name":    "prod-credit-risk-v2",
            "event_time":       ts.isoformat(),
            "violation_type":   "pii_detected",
            "direction":        "input",       # input | output
            "action_taken":     "blocked",     # blocked | logged | redacted
            "severity":         "high",
            "model_version":    "12",
        })

    # fraud-detection agent: prompt injection attempts (attack signal)
    for i in range(12):
        ts = now - timedelta(hours=random.uniform(0.1, 5))
        events.append({
            "endpoint_name":    "prod-fraud-gateway",
            "event_time":       ts.isoformat(),
            "violation_type":   "prompt_injection",
            "direction":        "input",
            "action_taken":     "blocked",
            "severity":         "critical",
            "model_version":    "2",
        })

    # demand forecast: clean (no violations)
    # churn: a few off-topic responses
    for i in range(8):
        ts = now - timedelta(hours=random.uniform(0.5, 15))
        events.append({
            "endpoint_name":    "prod-churn-v1",
            "event_time":       ts.isoformat(),
            "violation_type":   "off_topic",
            "direction":        "output",
            "action_taken":     "logged",
            "severity":         "low",
            "model_version":    "3",
        })

    return events


def _generate_mock_request_counts() -> dict[str, int]:
    """Total request counts per endpoint in the lookback window (for rate calculation)."""
    return {
        "prod-credit-risk-v2": 1250,
        "prod-demand-forecast": 310,
        "prod-churn-v1":        185,
        "prod-fraud-gateway":   2450,
    }


# ---------------------------------------------------------------------------
# Signal normalization
# ---------------------------------------------------------------------------
@dataclass
class GuardrailSignal:
    signal_id: str
    model_id: str
    signal_type: str          # always 'guardrail'
    signal_subtype: str       # violation type (pii_detected, prompt_injection, etc.)
    source_table: str
    computed_at: datetime
    ingested_at: datetime
    latest_value: float       # normalized violation rate 0–1
    raw_value: float          # raw violation count
    raw_unit: str             # "violations_per_1000_requests"
    raw_reference: str
    column_name: str | None   # always None for guardrail
    window_start: datetime | None
    window_end: datetime | None
    threshold_value: float
    threshold_breached: bool
    adapter_version: str = "1.0.0"


def _normalize_violation_rate(violation_count: int, total_requests: int, cap_per_1000: float) -> tuple[float, float]:
    """
    Returns (normalized_0_to_1, rate_per_1000).
    Violation rate = violations / total_requests * 1000.
    Normalized: rate_per_1000 / cap → clamped to [0, 1].
    """
    if total_requests == 0:
        return 0.0, 0.0
    rate = (violation_count / total_requests) * 1000
    normalized = min(1.0, rate / cap_per_1000)
    return normalized, rate


def process_guardrail_events(
    events: list[dict],
    request_counts: dict[str, int],           # endpoint_name → total requests in window
    endpoint_to_model: dict[str, str],         # endpoint_name → model_id
    window_start: datetime,
    window_end: datetime,
    threshold_rate_per_1000: float = 5.0,     # alert threshold
) -> list[GuardrailSignal]:
    """Convert raw guardrail events into normalized signals per (endpoint, violation_type)."""
    now = datetime.now(tz=timezone.utc)
    signals: list[GuardrailSignal] = []

    # Aggregate: (endpoint_name, violation_type) → count
    agg: dict[tuple[str, str], int] = defaultdict(int)
    for ev in events:
        ep   = ev["endpoint_name"]
        vtype = ev.get("violation_type", "unknown")
        ev_time_str = ev.get("event_time", "")
        if ev_time_str:
            try:
                ev_time = datetime.fromisoformat(ev_time_str.replace("Z", "+00:00"))
                if not (window_start <= ev_time <= window_end):
                    continue
            except ValueError:
                pass
        agg[(ep, vtype)] += 1

    # Emit one signal per (endpoint, violation_type)
    for (ep, vtype), count in agg.items():
        model_id = endpoint_to_model.get(ep)
        if not model_id:
            continue
        total_reqs = request_counts.get(ep, 0)
        norm, rate_per_1000 = _normalize_violation_rate(count, total_reqs, VIOLATION_RATE_CAP_PER_1000)

        signals.append(GuardrailSignal(
            signal_id=str(uuid.uuid4()),
            model_id=model_id,
            signal_type="guardrail",
            signal_subtype=vtype,
            source_table=GUARDRAIL_EVENTS_TABLE,
            computed_at=window_end,
            ingested_at=now,
            latest_value=norm,
            raw_value=float(count),
            raw_unit="violation_count",
            raw_reference=json.dumps({
                "endpoint_name": ep,
                "violation_type": vtype,
                "total_requests": total_reqs,
                "rate_per_1000": round(rate_per_1000, 3),
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }),
            column_name=None,
            window_start=window_start,
            window_end=window_end,
            threshold_value=threshold_rate_per_1000 / VIOLATION_RATE_CAP_PER_1000,
            threshold_breached=(rate_per_1000 >= threshold_rate_per_1000),
        ))

    # Also emit an aggregate (all-violations) signal per endpoint
    for ep, total_reqs in request_counts.items():
        model_id = endpoint_to_model.get(ep)
        if not model_id:
            continue
        total_violations = sum(c for (e, _), c in agg.items() if e == ep)
        if total_violations == 0:
            continue
        norm, rate = _normalize_violation_rate(total_violations, total_reqs, VIOLATION_RATE_CAP_PER_1000)
        signals.append(GuardrailSignal(
            signal_id=str(uuid.uuid4()),
            model_id=model_id,
            signal_type="guardrail",
            signal_subtype="all_violations_aggregate",
            source_table=GUARDRAIL_EVENTS_TABLE,
            computed_at=window_end,
            ingested_at=now,
            latest_value=norm,
            raw_value=float(total_violations),
            raw_unit="violation_count_aggregate",
            raw_reference=json.dumps({"endpoint_name": ep, "total_requests": total_reqs, "rate_per_1000": round(rate, 3)}),
            column_name=None,
            window_start=window_start,
            window_end=window_end,
            threshold_value=threshold_rate_per_1000 / VIOLATION_RATE_CAP_PER_1000,
            threshold_breached=(rate >= threshold_rate_per_1000),
        ))

    return signals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_adapter(dry_run: bool = False) -> dict[str, int]:
    now = datetime.now(tz=timezone.utc)
    window_start = now - timedelta(hours=LOOKBACK_HOURS)

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic guardrail events.")
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        endpoint_to_model = {
            e["serving_endpoint"]: _make_model_id(e["name"], str(e["version"]))
            for e in mock_registry if e.get("serving_endpoint")
        }
        events          = _generate_mock_guardrail_events()
        request_counts  = _generate_mock_request_counts()
    else:
        spark = SparkSession.builder.getOrCreate()
        reg_rows = spark.sql(f"""
            SELECT model_id, serving_endpoint FROM {REGISTRY_MAP_TABLE}
            WHERE is_active = true AND serving_endpoint IS NOT NULL
        """).collect()
        endpoint_to_model = {r["serving_endpoint"]: r["model_id"] for r in reg_rows}
        endpoints_str = ", ".join(f"'{ep}'" for ep in endpoint_to_model)

        try:
            events_raw = spark.sql(f"""
                SELECT endpoint_name, CAST(event_time AS STRING) AS event_time,
                       violation_type, direction, action_taken, severity, model_version
                FROM {GUARDRAIL_EVENTS_TABLE}
                WHERE endpoint_name IN ({endpoints_str})
                  AND event_time >= DATEADD(HOUR, -{LOOKBACK_HOURS}, CURRENT_TIMESTAMP())
            """).collect()
            events = [r.asDict() for r in events_raw]
        except Exception as e:
            logger.warning("Could not read guardrail events table (non-fatal): %s", e)
            events = []

        # Get request counts from endpoint_usage table for rate calculation
        try:
            req_raw = spark.sql(f"""
                SELECT endpoint_name, SUM(total_requests) AS total_requests
                FROM system.serving.endpoint_usage
                WHERE endpoint_name IN ({endpoints_str})
                  AND window_start >= DATEADD(HOUR, -{LOOKBACK_HOURS}, CURRENT_TIMESTAMP())
                GROUP BY endpoint_name
            """).collect()
            request_counts = {r["endpoint_name"]: r["total_requests"] for r in req_raw}
        except Exception as e:
            logger.warning("Could not read endpoint_usage for request counts: %s", e)
            request_counts = {}

    signals = process_guardrail_events(
        events=events,
        request_counts=request_counts,
        endpoint_to_model=endpoint_to_model,
        window_start=window_start,
        window_end=now,
    )

    breached = [s for s in signals if s.threshold_breached]
    stats = {"guardrail_events_processed": len(events), "signals_emitted": len(signals), "threshold_breached": len(breached)}
    logger.info("Guardrail adapter stats: %s", stats)

    if MOCK_MODE or dry_run:
        for s in signals:
            logger.info("  [%s] model=%s | %s/%s | count=%.0f | rate_norm=%.3f | breach=%s",
                        s.signal_id[:8], s.model_id[:8], s.signal_type, s.signal_subtype,
                        s.raw_value, s.latest_value, s.threshold_breached)
    else:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(s) for s in signals]
        df = spark.createDataFrame(rows)
        df.write.mode("append").saveAsTable(SIGNAL_INDEX_TABLE)
        logger.info("Wrote %d guardrail signals to %s.", len(signals), SIGNAL_INDEX_TABLE)

    return stats


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    stats = run_adapter(dry_run=True)
    print(json.dumps(stats, indent=2))
