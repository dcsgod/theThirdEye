"""
discovery/lineage_walker.py
============================
Third Eye — Lineage Walker

Reads system.access.table_lineage and system.access.column_lineage to find
upstream table dependencies for each model's inference/serving table, then
populates governance.model_health.lineage_events.

Design notes (Section 11, thirdeye_project.md):
  - The Lineage REST API returns only one hop up/down per call — this module
    walks recursively for multi-hop graphs using the system tables directly.
  - Known limit: lineage is not preserved across catalog/schema/table renames.
  - Known limit: system tables retain a rolling 1-year window.
  - Lineage events are cached per run to avoid redundant queries.
  - Includes MOCK_MODE for development without a live workspace.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict, deque
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
logger = logging.getLogger("third_eye.discovery.lineage_walker")

REGISTRY_MAP_TABLE   = "governance.model_health.model_registry_map"
LINEAGE_EVENTS_TABLE = "governance.model_health.lineage_events"

# Lineage system tables (rolling 1-year retention)
TABLE_LINEAGE_SYS_TABLE  = "system.access.table_lineage"
COLUMN_LINEAGE_SYS_TABLE = "system.access.column_lineage"

# How far back to look for new lineage events per run
LOOKBACK_HOURS = int(os.getenv("THIRD_EYE_LINEAGE_LOOKBACK_HOURS", "24"))

# Max hop depth for recursive lineage walk
MAX_HOP_DEPTH = int(os.getenv("THIRD_EYE_LINEAGE_MAX_HOPS", "3"))


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class LineageEvent:
    event_id: str
    model_id: str
    upstream_table: str
    event_type: str           # schema_change | new_write | large_write | row_count_anomaly
    event_time: datetime
    entity_type: str | None   # JOB | NOTEBOOK | PIPELINE | DASHBOARD_V3 | DBSQL_QUERY
    entity_id: str | None
    entity_name: str | None
    workspace_id: str | None
    hop_distance: int
    row_count_before: int | None
    row_count_after: int | None
    ingested_at: datetime


# ---------------------------------------------------------------------------
# Mock lineage data
# ---------------------------------------------------------------------------
def _generate_mock_lineage() -> list[dict[str, Any]]:
    """Synthetic upstream lineage events for MOCK_MODE."""
    now = datetime.now(tz=timezone.utc)
    return [
        # credit_risk_classifier depends on: features.credit_features, raw.transactions
        {
            "upstream_table":  "main.features.credit_features",
            "target_table":    "main.finance.credit_risk_inference_log",
            "event_time":      (now - timedelta(hours=3)).isoformat(),
            "entity_type":     "JOB",
            "entity_id":       "job_001",
            "entity_name":     "daily_feature_pipeline",
            "event_type":      "new_write",
            "row_count_before": 2_500_000,
            "row_count_after":  2_510_500,   # ~10k new rows — normal
            "workspace_id":    "ws_001",
        },
        {
            "upstream_table":  "main.raw.transactions",
            "target_table":    "main.finance.credit_risk_inference_log",
            "event_time":      (now - timedelta(hours=26)).isoformat(),
            "entity_type":     "PIPELINE",
            "entity_id":       "pipeline_raw_ingest",
            "entity_name":     "raw_transaction_ingestion",
            "event_type":      "schema_change",   # ← this could cause drift!
            "row_count_before": 50_000_000,
            "row_count_after":  50_100_000,
            "workspace_id":    "ws_001",
        },
        # demand_forecaster depends on: features.demand_features
        {
            "upstream_table":  "main.features.demand_features",
            "target_table":    "main.operations.demand_inference_log",
            "event_time":      (now - timedelta(hours=6)).isoformat(),
            "entity_type":     "JOB",
            "entity_id":       "job_002",
            "entity_name":     "daily_demand_features",
            "event_type":      "new_write",
            "row_count_before": 1_200_000,
            "row_count_after":  1_215_000,
            "workspace_id":    "ws_001",
        },
        # churn_predictor depends on: features.customer_features (no monitor yet)
        {
            "upstream_table":  "main.features.customer_features",
            "target_table":    "main.customer.churn_inference_log",
            "event_time":      (now - timedelta(hours=1)).isoformat(),
            "entity_type":     "NOTEBOOK",
            "entity_id":       "nb_001",
            "entity_name":     "customer_feature_engineering",
            "event_type":      "large_write",    # unusually large write
            "row_count_before": 800_000,
            "row_count_after":  950_000,         # +18.75% — potential anomaly
            "workspace_id":    "ws_001",
        },
    ]


def _classify_event_type(row_count_before: int | None, row_count_after: int | None) -> str:
    """Infer event type from row count changes when not explicitly available."""
    if row_count_before is None or row_count_after is None:
        return "new_write"
    delta = row_count_after - row_count_before
    if delta == 0:
        return "no_change"
    pct_change = abs(delta) / max(row_count_before, 1) * 100
    if pct_change > 15:
        return "large_write"
    return "new_write"


# ---------------------------------------------------------------------------
# Live lineage reader
# ---------------------------------------------------------------------------
def _fetch_live_lineage_events(
    spark: "SparkSession",
    inference_tables: list[str],
    lookback_hours: int = LOOKBACK_HOURS,
    max_hops: int = MAX_HOP_DEPTH,
) -> list[dict[str, Any]]:
    """
    Reads system.access.table_lineage to find all upstream tables for the given
    inference/serving tables, walking up to max_hops deep.

    Uses breadth-first expansion: start with the inference tables, find their
    1-hop upstreams, then walk those upstreams, up to max_hops.
    """
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=lookback_hours)).isoformat()

    all_events = []
    visited_tables: set[str] = set(inference_tables)
    queue: deque[tuple[str, int]] = deque((t, 1) for t in inference_tables)

    while queue:
        target_table, hop = queue.popleft()
        if hop > max_hops:
            continue

        try:
            lineage_df = spark.sql(f"""
                SELECT
                    source_table_full_name  AS upstream_table,
                    target_table_full_name  AS target_table,
                    event_time,
                    entity_type,
                    entity_id,
                    workspace_id
                FROM {TABLE_LINEAGE_SYS_TABLE}
                WHERE target_table_full_name = '{target_table}'
                  AND event_time >= '{cutoff}'
                ORDER BY event_time DESC
                LIMIT 500
            """)
            rows = lineage_df.collect()
        except Exception as e:
            logger.error("Failed to read lineage for %s: %s", target_table, e)
            rows = []

        for row in rows:
            upstream = row["upstream_table"]
            all_events.append({
                "upstream_table":   upstream,
                "target_table":     target_table,
                "event_time":       row["event_time"],
                "entity_type":      row.get("entity_type"),
                "entity_id":        row.get("entity_id"),
                "entity_name":      None,  # enrichable from system.compute.clusters if needed
                "event_type":       "new_write",  # system table doesn't distinguish type
                "row_count_before": None,
                "row_count_after":  None,
                "workspace_id":     row.get("workspace_id"),
                "hop_distance":     hop,
            })
            # Enqueue upstream for deeper traversal
            if upstream not in visited_tables:
                visited_tables.add(upstream)
                queue.append((upstream, hop + 1))

    return all_events


# ---------------------------------------------------------------------------
# Main walk logic
# ---------------------------------------------------------------------------
def run_lineage_walk(dry_run: bool = False) -> dict[str, int]:
    """
    Entry point. Reads model_registry_map, walks lineage for each model's
    inference_table, and writes to lineage_events.
    """
    now = datetime.now(tz=timezone.utc)

    if MOCK_MODE:
        logger.info("MOCK_MODE — using synthetic lineage data.")
        # Build a fake model→inference_table mapping using mock discovery data
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        model_table_map = {
            _make_model_id(e["name"], str(e["version"])): e.get("inference_table")
            for e in mock_registry
            if e.get("inference_table")
        }
        # Reverse map: inference_table → model_id
        table_to_model: dict[str, str] = {v: k for k, v in model_table_map.items()}
        raw_events = _generate_mock_lineage()
        spark = None
    else:
        spark = SparkSession.builder.getOrCreate()
        # Read active models with an inference table
        registry_df = spark.sql(f"""
            SELECT model_id, inference_table
            FROM {REGISTRY_MAP_TABLE}
            WHERE is_active = true AND inference_table IS NOT NULL
        """).collect()

        inference_tables = [row["inference_table"] for row in registry_df]
        table_to_model = {row["inference_table"]: row["model_id"] for row in registry_df}
        raw_events = _fetch_live_lineage_events(spark, inference_tables)

    # -- Build LineageEvent objects ----------------------------------------
    events: list[LineageEvent] = []
    for raw in raw_events:
        model_id = table_to_model.get(raw["target_table"])
        if not model_id:
            # Try to find model by partial match
            for tbl, mid in table_to_model.items():
                if raw["target_table"] in tbl or tbl in raw["target_table"]:
                    model_id = mid
                    break
        if not model_id:
            logger.warning("Could not match lineage event to a model: %s", raw["upstream_table"])
            continue

        event_type = raw.get("event_type") or _classify_event_type(
            raw.get("row_count_before"), raw.get("row_count_after")
        )
        raw_time = raw["event_time"]
        if isinstance(raw_time, str):
            event_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        else:
            event_time = raw_time

        events.append(LineageEvent(
            event_id=str(uuid.uuid4()),
            model_id=model_id,
            upstream_table=raw["upstream_table"],
            event_type=event_type,
            event_time=event_time,
            entity_type=raw.get("entity_type"),
            entity_id=raw.get("entity_id"),
            entity_name=raw.get("entity_name"),
            workspace_id=raw.get("workspace_id"),
            hop_distance=raw.get("hop_distance", 1),
            row_count_before=raw.get("row_count_before"),
            row_count_after=raw.get("row_count_after"),
            ingested_at=now,
        ))

    stats = {"total_events": len(events), "models_covered": len({e.model_id for e in events})}
    logger.info("Lineage walk stats: %s", stats)

    # -- Write to lineage_events table ------------------------------------
    if MOCK_MODE or dry_run:
        logger.info("=== MOCK/DRY RUN — Lineage events ===")
        for evt in events:
            logger.info(
                "  [hop=%d] %s upstream of model %s — %s at %s [%s]",
                evt.hop_distance, evt.upstream_table, evt.model_id[:8],
                evt.event_type, evt.event_time.isoformat(), evt.entity_type
            )
    else:
        import pyspark.sql.types as T
        rows = [asdict(e) for e in events]
        new_df = spark.createDataFrame(rows)
        new_df.write.mode("append").saveAsTable(LINEAGE_EVENTS_TABLE)
        logger.info("Wrote %d lineage events to %s.", len(events), LINEAGE_EVENTS_TABLE)

    return stats


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    stats = run_lineage_walk(dry_run=True)
    print(json.dumps(stats, indent=2))
