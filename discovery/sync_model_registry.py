"""
discovery/sync_model_registry.py
=================================
Third Eye — Discovery & Sync Job

Diffs MLflow Model Registry + Unity Catalog model objects + Unity Gateway
AI Asset Registry against governance.model_health.model_registry_map and
keeps the asset inventory up to date.

Design principles (from Section 3, 11 of thirdeye_project.md):
  - Zero-touch: registering a model normally (mlflow.register_model or UC UI)
    is sufficient to activate Third Eye discovery.
  - Read-first: this job reads Databricks sources; it does not recompute metrics.
  - Optionally provisions Lakehouse Monitors for models that lack one.
  - Includes a MOCK_MODE for development without a live Databricks workspace.

Run as: Databricks Workflow task (hourly cadence).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Conditional imports — falls back to mock stubs in MOCK_MODE
# ---------------------------------------------------------------------------
MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.catalog import (
            ListModelsRequest,
        )
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, BooleanType, TimestampType
        import mlflow
        import mlflow.tracking
    except ImportError as e:
        logging.warning("Databricks SDK/MLflow not available — falling back to MOCK_MODE. %s", e)
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.discovery.sync")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "criticality_defaults.yaml")
RISK_WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "risk_weights.yaml")

REGISTRY_MAP_TABLE = "governance.model_health.model_registry_map"
TARGET_CATALOG = os.getenv("THIRD_EYE_CATALOG", "governance")
TARGET_SCHEMA  = os.getenv("THIRD_EYE_SCHEMA", "model_health")

# Foundation model used for root-cause (referenced here for monitor auto-provisioning notes)
FOUNDATION_MODEL_ENDPOINT = os.getenv("THIRD_EYE_FOUNDATION_MODEL", "databricks-meta-llama-3-3-70b-instruct")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass
class DiscoveredModel:
    model_id: str
    model_name: str
    model_version: str
    serving_endpoint: str | None
    gateway_registered: bool
    asset_type: str                 # model | agent | tool | mcp_server
    owning_team: str | None
    business_domain: str | None
    criticality_tier: str
    lakehouse_monitor_configured: bool
    lakehouse_monitor_schema: str | None
    inference_table: str | None
    mlflow_run_id: str | None
    mlflow_experiment_id: str | None
    uc_model_uri: str | None
    created_at: datetime
    updated_at: datetime
    discovered_by: str
    is_active: bool = True
    autonomy_level: int = 1
    retrain_trigger_threshold: float = 40.0


def _make_model_id(model_name: str, model_version: str) -> str:
    """Stable, deterministic ID = SHA-256 of (model_name + ':' + model_version)."""
    raw = f"{model_name}:{model_version}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Criticality tier resolution
# ---------------------------------------------------------------------------
class CriticalityResolver:
    """Applies criticality_defaults.yaml rules to assign a tier to a model."""

    def __init__(self, config_path: str = CONFIG_PATH):
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        # Sort rules ascending by priority (lower = higher priority)
        self.rules = sorted(cfg.get("rules", []), key=lambda r: r["priority"])
        self.autonomy_defaults = cfg.get("autonomy_defaults", {})

    def resolve(
        self,
        domain: str | None,
        tags: dict[str, str],
        model_name: str,
        serving_endpoint: str | None,
    ) -> str:
        for rule in self.rules:
            m_type = rule["match_type"]
            m_val  = rule["match_value"].lower()

            if m_type == "default":
                return rule["assigned_tier"]

            if m_type == "domain" and domain:
                if domain.lower().startswith(m_val):
                    return rule["assigned_tier"]

            if m_type == "tag_key":
                if any(k.lower() == m_val for k in tags):
                    return rule["assigned_tier"]

            if m_type == "tag_value":
                key = rule.get("match_key", "")
                if tags.get(key, "").lower() == m_val:
                    return rule["assigned_tier"]

            if m_type == "model_name_prefix":
                short_name = model_name.split(".")[-1].lower()
                if short_name.startswith(m_val):
                    return rule["assigned_tier"]

            if m_type == "serving_endpoint_prefix" and serving_endpoint:
                if serving_endpoint.lower().startswith(m_val):
                    return rule["assigned_tier"]

        return "tier_2_operational"  # fallback

    def autonomy_for_tier(self, tier: str) -> int:
        return self.autonomy_defaults.get(tier, 1)


# ---------------------------------------------------------------------------
# Mock registry — used in MOCK_MODE / development
# ---------------------------------------------------------------------------
def _generate_mock_registry() -> list[dict[str, Any]]:
    """Returns synthetic model registry entries for testing without a live workspace."""
    now = datetime.now(tz=timezone.utc)
    return [
        {
            "name": "main.finance.credit_risk_classifier",
            "version": "12",
            "run_id": "run_abc123",
            "experiment_id": "exp_001",
            "tags": {"pii": "true", "compliance": "sox"},
            "serving_endpoint": "prod-credit-risk-v2",
            "business_domain": "finance",
            "owning_team": "risk_analytics",
            "gateway_registered": True,
            "asset_type": "model",
            "inference_table": "main.finance.credit_risk_inference_log",
            "lakehouse_monitor_configured": True,
            "lakehouse_monitor_schema": "main.finance_monitoring",
        },
        {
            "name": "main.operations.demand_forecaster",
            "version": "5",
            "run_id": "run_def456",
            "experiment_id": "exp_002",
            "tags": {},
            "serving_endpoint": "prod-demand-forecast",
            "business_domain": "operations",
            "owning_team": "supply_chain",
            "gateway_registered": True,
            "asset_type": "model",
            "inference_table": "main.operations.demand_inference_log",
            "lakehouse_monitor_configured": True,
            "lakehouse_monitor_schema": "main.ops_monitoring",
        },
        {
            "name": "main.customer.churn_predictor",
            "version": "3",
            "run_id": "run_ghi789",
            "experiment_id": "exp_003",
            "tags": {},
            "serving_endpoint": "prod-churn-v1",
            "business_domain": "customer",
            "owning_team": "cust_success",
            "gateway_registered": False,
            "asset_type": "model",
            "inference_table": "main.customer.churn_inference_log",
            "lakehouse_monitor_configured": False,  # → will trigger auto-provision
            "lakehouse_monitor_schema": None,
        },
        {
            "name": "main.internal_analytics.exp_revenue_estimator",
            "version": "1",
            "run_id": "run_jkl012",
            "experiment_id": "exp_004",
            "tags": {},
            "serving_endpoint": None,
            "business_domain": "internal_analytics",
            "owning_team": "data_science",
            "gateway_registered": False,
            "asset_type": "model",
            "inference_table": None,
            "lakehouse_monitor_configured": False,
            "lakehouse_monitor_schema": None,
        },
        {
            "name": "main.finance.fraud_detection_agent",
            "version": "2",
            "run_id": None,
            "experiment_id": None,
            "tags": {"pii": "true"},
            "serving_endpoint": "prod-fraud-gateway",
            "business_domain": "fraud",
            "owning_team": "fraud_ops",
            "gateway_registered": True,
            "asset_type": "agent",
            "inference_table": "system.serving.unified_trace_fraud",
            "lakehouse_monitor_configured": True,
            "lakehouse_monitor_schema": "main.fraud_monitoring",
        },
    ]


# ---------------------------------------------------------------------------
# Live Databricks registry reader
# ---------------------------------------------------------------------------
def _fetch_live_registry(w: "WorkspaceClient") -> list[dict[str, Any]]:
    """
    Reads MLflow Model Registry + Unity Catalog model objects + Unity Gateway
    AI Asset Registry.

    Returns a normalised list of dicts matching the mock schema.
    """
    results = []
    now = datetime.now(tz=timezone.utc)

    # -- 1. MLflow / Unity Catalog registered models -----------------------
    mlflow_client = mlflow.tracking.MlflowClient()

    try:
        for rm in mlflow_client.search_registered_models(max_results=1000):
            for mv in mlflow_client.get_latest_versions(rm.name):
                tags = {t.key: t.value for t in rm.tags}
                domain = tags.get("business_domain") or tags.get("domain") or None
                team   = tags.get("owning_team") or tags.get("team") or None
                endpoint = tags.get("serving_endpoint") or None

                results.append({
                    "name": rm.name,
                    "version": mv.version,
                    "run_id": mv.run_id,
                    "experiment_id": None,
                    "tags": tags,
                    "serving_endpoint": endpoint,
                    "business_domain": domain,
                    "owning_team": team,
                    "gateway_registered": False,   # enriched below from Gateway
                    "asset_type": "model",
                    "inference_table": tags.get("inference_table"),
                    "lakehouse_monitor_configured": False,   # checked below
                    "lakehouse_monitor_schema": tags.get("lakehouse_monitor_schema"),
                })
    except Exception as e:
        logger.error("MLflow registry read failed: %s", e)

    # -- 2. Unity Gateway AI Asset Registry --------------------------------
    try:
        # The AI Asset Registry is accessed via the Unity Gateway REST API.
        # The SDK surface may evolve — adapt path as needed.
        # Documented endpoint: GET /api/2.0/gateway/ai-assets
        resp = w.api_client.do("GET", "/api/2.0/gateway/ai-assets")
        gateway_assets = resp.get("ai_assets", [])
        gateway_names = set()
        for asset in gateway_assets:
            asset_name = asset.get("name", "")
            gateway_names.add(asset_name)
            asset_type = asset.get("asset_type", "model")
            # Check if already in results
            existing = next((r for r in results if r["name"] == asset_name), None)
            if existing:
                existing["gateway_registered"] = True
                existing["asset_type"] = asset_type
            else:
                # Gateway-only asset (e.g., external model, MCP server)
                results.append({
                    "name": asset_name,
                    "version": asset.get("version", "1"),
                    "run_id": None,
                    "experiment_id": None,
                    "tags": asset.get("tags", {}),
                    "serving_endpoint": asset.get("endpoint_name"),
                    "business_domain": asset.get("domain"),
                    "owning_team": asset.get("owner"),
                    "gateway_registered": True,
                    "asset_type": asset_type,
                    "inference_table": asset.get("trace_table"),
                    "lakehouse_monitor_configured": False,
                    "lakehouse_monitor_schema": None,
                })
    except Exception as e:
        logger.warning("Unity Gateway AI Asset Registry read failed (non-fatal): %s", e)

    # -- 3. Check Lakehouse Monitor status for each model ------------------
    for entry in results:
        if not entry.get("inference_table"):
            continue
        try:
            monitor_info = w.quality_monitors.get(table_name=entry["inference_table"])
            entry["lakehouse_monitor_configured"] = True
            entry["lakehouse_monitor_schema"] = monitor_info.output_schema_name
        except Exception:
            entry["lakehouse_monitor_configured"] = False

    return results


# ---------------------------------------------------------------------------
# Auto-provisioning (optional)
# ---------------------------------------------------------------------------
def _auto_provision_monitor(
    w: "WorkspaceClient",
    model: DiscoveredModel,
    dry_run: bool = False,
) -> bool:
    """
    Provisions a Lakehouse Monitor for models that don't have one.
    This is orchestration — not reimplementing Databricks' monitoring algorithms.
    Section 11, step 4 of thirdeye_project.md.
    """
    if not model.inference_table:
        logger.info("Skipping auto-provision for %s — no inference_table configured.", model.model_name)
        return False

    logger.info(
        "Auto-provisioning Lakehouse Monitor for %s on table %s%s",
        model.model_name,
        model.inference_table,
        " [DRY RUN]" if dry_run else "",
    )

    if dry_run or MOCK_MODE:
        logger.info("[DRY RUN / MOCK] Would call w.quality_monitors.create(...)")
        return True

    try:
        output_schema = f"{TARGET_CATALOG}.{TARGET_SCHEMA}_monitoring"
        w.quality_monitors.create(
            table_name=model.inference_table,
            assets_dir=f"/Shared/third_eye/monitors/{model.model_name.replace('.', '_')}",
            output_schema_name=output_schema,
            inference_log={"granularities": ["1 day"], "problem_type": "PROBLEM_TYPE_REGRESSION"},
        )
        logger.info("Monitor created successfully for %s", model.model_name)
        return True
    except Exception as e:
        logger.error("Failed to auto-provision monitor for %s: %s", model.model_name, e)
        return False


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------
def run_sync(
    auto_provision_monitors: bool = True,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Main entry point. Returns a summary dict with counts of new/updated/deactivated models.
    """
    resolver = CriticalityResolver(CONFIG_PATH)
    now = datetime.now(tz=timezone.utc)

    # -- Step 1: Fetch source registry ------------------------------------
    if MOCK_MODE:
        logger.info("MOCK_MODE enabled — using synthetic registry.")
        raw_entries = _generate_mock_registry()
        w = None
    else:
        spark = SparkSession.builder.getOrCreate()
        w = WorkspaceClient()
        raw_entries = _fetch_live_registry(w)

    logger.info("Fetched %d model entries from registry.", len(raw_entries))

    # -- Step 2: Resolve criticality tier and build DiscoveredModel list --
    discovered: list[DiscoveredModel] = []
    for entry in raw_entries:
        tier = resolver.resolve(
            domain=entry.get("business_domain"),
            tags=entry.get("tags", {}),
            model_name=entry["name"],
            serving_endpoint=entry.get("serving_endpoint"),
        )
        model_id = _make_model_id(entry["name"], str(entry["version"]))
        uc_uri = f"models:/{entry['name']}/{entry['version']}" if not entry.get("gateway_registered") else None

        discovered.append(DiscoveredModel(
            model_id=model_id,
            model_name=entry["name"],
            model_version=str(entry["version"]),
            serving_endpoint=entry.get("serving_endpoint"),
            gateway_registered=entry.get("gateway_registered", False),
            asset_type=entry.get("asset_type", "model"),
            owning_team=entry.get("owning_team"),
            business_domain=entry.get("business_domain"),
            criticality_tier=tier,
            lakehouse_monitor_configured=entry.get("lakehouse_monitor_configured", False),
            lakehouse_monitor_schema=entry.get("lakehouse_monitor_schema"),
            inference_table=entry.get("inference_table"),
            mlflow_run_id=entry.get("run_id"),
            mlflow_experiment_id=entry.get("experiment_id"),
            uc_model_uri=uc_uri,
            created_at=now,
            updated_at=now,
            discovered_by="sync_job",
            autonomy_level=resolver.autonomy_for_tier(tier),
        ))

    # -- Step 3: Read existing registry_map --------------------------------
    if MOCK_MODE:
        existing_ids: set[str] = set()
    else:
        spark = SparkSession.builder.getOrCreate()
        try:
            existing_df = spark.table(REGISTRY_MAP_TABLE).filter("is_active = true")
            existing_ids = {row["model_id"] for row in existing_df.select("model_id").collect()}
        except Exception:
            logger.warning("registry_map table not readable yet — treating as empty.")
            existing_ids = set()

    discovered_ids = {m.model_id for m in discovered}
    new_ids = discovered_ids - existing_ids
    to_deactivate = existing_ids - discovered_ids

    stats = {
        "total_discovered": len(discovered),
        "new": len(new_ids),
        "unchanged": len(discovered_ids & existing_ids),
        "deactivated": len(to_deactivate),
        "monitors_provisioned": 0,
    }

    logger.info("Sync stats: %s", stats)

    # -- Step 4: Write to registry_map (MERGE) ----------------------------
    if not MOCK_MODE and not dry_run:
        import pyspark.sql.types as T
        rows = [asdict(m) for m in discovered]
        schema_fields = [
            ("model_id", T.StringType()), ("model_name", T.StringType()), ("model_version", T.StringType()),
            ("serving_endpoint", T.StringType()), ("gateway_registered", T.BooleanType()),
            ("asset_type", T.StringType()), ("owning_team", T.StringType()),
            ("business_domain", T.StringType()), ("criticality_tier", T.StringType()),
            ("lakehouse_monitor_configured", T.BooleanType()), ("lakehouse_monitor_schema", T.StringType()),
            ("inference_table", T.StringType()), ("mlflow_run_id", T.StringType()),
            ("mlflow_experiment_id", T.StringType()), ("uc_model_uri", T.StringType()),
            ("created_at", T.TimestampType()), ("updated_at", T.TimestampType()),
            ("discovered_by", T.StringType()), ("is_active", T.BooleanType()),
            ("autonomy_level", T.IntegerType()), ("retrain_trigger_threshold", T.DoubleType()),
        ]
        schema = T.StructType([T.StructField(n, t, True) for n, t in schema_fields])
        new_df = spark.createDataFrame(rows, schema=schema)
        new_df.createOrReplaceTempView("_sync_staging")

        spark.sql(f"""
            MERGE INTO {REGISTRY_MAP_TABLE} AS target
            USING _sync_staging AS source
            ON target.model_id = source.model_id
            WHEN MATCHED THEN UPDATE SET
                target.model_version            = source.model_version,
                target.serving_endpoint         = source.serving_endpoint,
                target.gateway_registered       = source.gateway_registered,
                target.asset_type               = source.asset_type,
                target.owning_team              = source.owning_team,
                target.business_domain          = source.business_domain,
                target.criticality_tier         = source.criticality_tier,
                target.lakehouse_monitor_configured = source.lakehouse_monitor_configured,
                target.lakehouse_monitor_schema = source.lakehouse_monitor_schema,
                target.inference_table          = source.inference_table,
                target.updated_at               = source.updated_at,
                target.is_active                = true
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Deactivate removed models
        if to_deactivate:
            ids_str = ", ".join(f"'{i}'" for i in to_deactivate)
            spark.sql(f"""
                UPDATE {REGISTRY_MAP_TABLE}
                SET is_active = false, deactivated_at = current_timestamp()
                WHERE model_id IN ({ids_str})
            """)

    # -- Step 5: Auto-provision monitors where missing --------------------
    if auto_provision_monitors and w:
        for model in discovered:
            if not model.lakehouse_monitor_configured and model.inference_table:
                provisioned = _auto_provision_monitor(w, model, dry_run=dry_run)
                if provisioned:
                    stats["monitors_provisioned"] += 1

    # Mock mode output
    if MOCK_MODE:
        logger.info("=== MOCK MODE — Discovered models ===")
        for m in discovered:
            logger.info("  [%s] %s v%s | tier=%s | monitor=%s | gateway=%s",
                        m.model_id[:8], m.model_name, m.model_version,
                        m.criticality_tier, m.lakehouse_monitor_configured, m.gateway_registered)

    logger.info("Sync complete. Stats: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Run in MOCK_MODE by default when executed directly
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    stats = run_sync(auto_provision_monitors=True, dry_run=True)
    print(json.dumps(stats, indent=2))
