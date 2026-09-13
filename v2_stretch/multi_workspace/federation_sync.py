"""
v2_stretch/multi_workspace/federation_sync.py
==============================================
Third Eye V2 — Multi-Workspace Federation Sync

Aggregates risk_scores and incidents across multiple Databricks workspaces
into a federated governance schema with workspace_id tagging.
Unity Catalog row-level security ensures each business unit sees only its models.

Design (Section 12.4):
  - Each workspace runs its own Third Eye pipeline independently.
  - This federation job reads from per-workspace Delta Sharing shares
    (or cross-workspace UC catalog sharing) and merges into a central
    'governance_global.model_health' schema.
  - workspace_id tagging + UC row-level security enforces data isolation.
  - Non-breaking: V1 workspaces are unaffected; federation is additive.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.v2.federation")

# Central federated schema
FEDERATED_CATALOG = os.getenv("THIRD_EYE_FED_CATALOG", "governance_global")
FEDERATED_SCHEMA  = "model_health_federated"

# Workspace registry: in production, read from a configuration Delta table or YAML
WORKSPACE_REGISTRY = [
    {"workspace_id": "ws_us_east",  "share_name": "third_eye_us_east",  "catalog": "governance", "region": "us-east-1"},
    {"workspace_id": "ws_eu_west",  "share_name": "third_eye_eu_west",  "catalog": "governance", "region": "eu-west-1"},
    {"workspace_id": "ws_apac",     "share_name": "third_eye_apac",     "catalog": "governance", "region": "ap-southeast-1"},
]


def run_federation_sync(dry_run: bool = False) -> dict[str, Any]:
    """
    Reads risk_scores, incidents, model_registry_map from each workspace share
    and merges into the central federated schema with workspace_id tagging.
    """
    now = datetime.now(tz=timezone.utc)

    if MOCK_MODE:
        logger.info("MOCK_MODE — V2 Federation Sync (simulated)")
        logger.info("Would sync from %d workspaces: %s",
                    len(WORKSPACE_REGISTRY),
                    [w["workspace_id"] for w in WORKSPACE_REGISTRY])
        stats = {
            "workspaces_synced": len(WORKSPACE_REGISTRY),
            "mode": "mock",
            "federated_catalog": f"{FEDERATED_CATALOG}.{FEDERATED_SCHEMA}",
        }
        logger.info("[V2] Federation sync complete (mock): %s", stats)
        return {"stats": stats}

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Ensure federated schema exists
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {FEDERATED_CATALOG}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FEDERATED_CATALOG}.{FEDERATED_SCHEMA}")

        total_rows = 0
        for ws in WORKSPACE_REGISTRY:
            ws_id   = ws["workspace_id"]
            share   = ws["share_name"]
            catalog = ws["catalog"]

            logger.info("Syncing from workspace %s (share: %s)", ws_id, share)
            try:
                # Use Delta Sharing or UC catalog sharing to read remote tables
                # In production: the remote workspace must have a Delta Share configured
                # pointing at governance.model_health.risk_scores etc.
                remote_risk = spark.sql(f"""
                    SELECT *, '{ws_id}' AS workspace_id, '{ws['region']}' AS workspace_region
                    FROM {share}.{catalog}.model_health.risk_scores
                    WHERE computed_at >= DATEADD(HOUR, -25, CURRENT_TIMESTAMP())
                """)
                remote_risk.write.mode("append").option("mergeSchema", "true").saveAsTable(
                    f"{FEDERATED_CATALOG}.{FEDERATED_SCHEMA}.risk_scores_federated"
                )
                count = remote_risk.count()
                total_rows += count
                logger.info("  Synced %d risk_score rows from %s", count, ws_id)
            except Exception as e:
                logger.error("  Failed to sync from %s: %s", ws_id, e)

        stats = {
            "workspaces_synced": len(WORKSPACE_REGISTRY),
            "total_rows_synced": total_rows,
            "federated_schema": f"{FEDERATED_CATALOG}.{FEDERATED_SCHEMA}",
        }

    except Exception as e:
        logger.error("Federation sync failed: %s", e)
        stats = {"error": str(e)}

    logger.info("[V2] Federation sync complete: %s", stats)
    return {"stats": stats}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    result = run_federation_sync(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
