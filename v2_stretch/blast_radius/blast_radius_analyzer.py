"""
v2_stretch/blast_radius/blast_radius_analyzer.py
=================================================
Third Eye V2 — Blast Radius Analysis

When an upstream table changes, compute which downstream models and dashboards
are affected — BEFORE those models show symptoms.

Design (Section 12.2):
  - Generalizes the lineage walk from sync_model_registry to multi-hop.
  - Reads from lineage_events (populated by lineage_walker.py).
  - Computes transitive closure: upstream table → affected model_ids (all hops).
  - Also identifies dashboards reading from affected models (via column_lineage).
  - Writes to governance.model_health.blast_radius_analysis.
  - Results surfaced in the dashboard and incident root_cause_narrative.
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
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.v2.blast_radius")

LINEAGE_EVENTS_TABLE  = "governance.model_health.lineage_events"
REGISTRY_MAP_TABLE    = "governance.model_health.model_registry_map"
BLAST_RADIUS_TABLE    = "governance.model_health.blast_radius_analysis"

MAX_HOP_DEPTH = int(os.getenv("THIRD_EYE_BLAST_MAX_HOPS", "4"))
LOOKBACK_HOURS = 48


@dataclass
class BlastRadiusResult:
    analysis_id: str
    upstream_table: str
    event_time: datetime
    affected_model_ids: str    # JSON array
    affected_dashboards: str   # JSON array (empty in V1 without dashboard lineage)
    hop_depths: str            # JSON map of model_id → hop_distance
    computed_at: datetime
    workspace_id: str | None


def _mock_lineage_graph() -> dict[str, list[str]]:
    """
    Returns a mock downstream graph: upstream_table → [downstream tables/models].
    In production this is derived from system.access.table_lineage.
    """
    return {
        "main.raw.transactions":          ["main.features.credit_features", "main.features.fraud_features"],
        "main.features.credit_features":  ["main.finance.credit_risk_inference_log"],
        "main.features.fraud_features":   ["system.serving.unified_trace_fraud"],
        "main.features.demand_features":  ["main.operations.demand_inference_log"],
        "main.features.customer_features":["main.customer.churn_inference_log"],
    }


def _compute_blast_radius(
    changed_table: str,
    downstream_graph: dict[str, list[str]],
    inference_table_to_model: dict[str, str],
    max_hops: int = MAX_HOP_DEPTH,
) -> tuple[dict[str, int], list[str]]:
    """
    BFS from changed_table through downstream_graph.
    Returns (model_id → hop_distance, list_of_dashboards).
    """
    hop_depths: dict[str, int] = {}
    visited_tables: set[str] = {changed_table}
    queue: deque[tuple[str, int]] = deque([(changed_table, 0)])

    while queue:
        table, hop = queue.popleft()
        if hop > max_hops:
            continue

        # Check if this table is a model's inference table
        model_id = inference_table_to_model.get(table)
        if model_id and model_id not in hop_depths:
            hop_depths[model_id] = hop

        # Expand downstream
        for downstream in downstream_graph.get(table, []):
            if downstream not in visited_tables:
                visited_tables.add(downstream)
                queue.append((downstream, hop + 1))

    return hop_depths, []  # dashboards: TBD in future via column_lineage


def run_blast_radius(dry_run: bool = False) -> dict[str, Any]:
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)

    if MOCK_MODE:
        logger.info("MOCK_MODE — V2 Blast Radius Analysis")
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()
        inference_table_to_model = {
            e["inference_table"]: _make_model_id(e["name"], str(e["version"]))
            for e in mock_registry if e.get("inference_table")
        }

        # Simulate: main.raw.transactions had a schema_change
        changed_tables = [
            {"upstream_table": "main.raw.transactions", "event_time": (now - timedelta(hours=26)).isoformat()}
        ]
        downstream_graph = _mock_lineage_graph()
    else:
        spark = SparkSession.builder.getOrCreate()
        # Find upstream tables that changed recently
        le_rows = spark.sql(f"""
            SELECT DISTINCT upstream_table, MIN(event_time) AS event_time
            FROM {LINEAGE_EVENTS_TABLE}
            WHERE event_time >= '{cutoff.isoformat()}'
              AND event_type IN ('schema_change', 'large_write', 'column_dropped', 'column_added')
            GROUP BY upstream_table
        """).collect()
        changed_tables = [r.asDict() for r in le_rows]

        # Build downstream graph from lineage_events
        all_lineage = spark.sql(f"""
            SELECT upstream_table, STRING_AGG(DISTINCT inference_table, ',') AS inference_tables
            FROM {LINEAGE_EVENTS_TABLE} le
            JOIN {REGISTRY_MAP_TABLE} mrm ON le.model_id = mrm.model_id
            GROUP BY upstream_table
        """).collect()
        downstream_graph = defaultdict(list)
        for r in all_lineage:
            for tbl in (r["inference_tables"] or "").split(","):
                if tbl:
                    downstream_graph[r["upstream_table"]].append(tbl.strip())

        reg_rows = spark.sql(f"SELECT model_id, inference_table FROM {REGISTRY_MAP_TABLE} WHERE is_active = true").collect()
        inference_table_to_model = {r["inference_table"]: r["model_id"] for r in reg_rows if r["inference_table"]}

    results: list[BlastRadiusResult] = []
    for ct in changed_tables:
        upstream  = ct["upstream_table"]
        evt_time_str = ct["event_time"]
        evt_time = datetime.fromisoformat(evt_time_str.replace("Z", "+00:00")) if isinstance(evt_time_str, str) else evt_time_str

        hop_depths, affected_dashboards = _compute_blast_radius(
            upstream, downstream_graph, inference_table_to_model
        )

        if not hop_depths:
            continue

        logger.info("Blast radius for %s: %d models affected (hops: %s)",
                    upstream, len(hop_depths), hop_depths)

        results.append(BlastRadiusResult(
            analysis_id=str(uuid.uuid4()),
            upstream_table=upstream,
            event_time=evt_time,
            affected_model_ids=json.dumps(list(hop_depths.keys())),
            affected_dashboards=json.dumps(affected_dashboards),
            hop_depths=json.dumps({k: v for k, v in hop_depths.items()}),
            computed_at=now,
            workspace_id=None,
        ))

    stats = {
        "changed_tables_analyzed": len(changed_tables),
        "blast_radius_computed": len(results),
        "total_models_affected": sum(len(json.loads(r.affected_model_ids)) for r in results),
    }

    if not MOCK_MODE and not dry_run:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(r) for r in results]
        if rows:
            df = spark.createDataFrame(rows)
            df.write.mode("append").saveAsTable(BLAST_RADIUS_TABLE)

    logger.info("[V2] Blast radius analysis complete: %s", stats)
    return {"stats": stats, "results": [asdict(r) for r in results]}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    result = run_blast_radius(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
    for r in result["results"]:
        hop_d = json.loads(r["hop_depths"])
        print(f"\nUpstream: {r['upstream_table']}")
        print(f"  Affected models ({len(hop_d)}): {hop_d}")
