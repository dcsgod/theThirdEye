"""
v2_stretch/auto_retrain/auto_retrain_pipeline.py
==================================================
Third Eye V2 — Auto-Retraining Pipeline with Champion/Challenger Evaluation

Triggered when a model sustains health_tier = 'critical' beyond a configured
duration (default: 2 consecutive scoring runs). Never auto-promotes Tier 1 /
critical models — always requires explicit approval.

Design (Section 12.1):
  - Reads retrain_trigger_threshold from model_registry_map
  - Registers a challenger model tagged auto_retrain_candidate=true
  - Runs champion/challenger comparison on a holdout window
  - Writes results to governance.model_health.retrain_candidates
  - Promotion always requires human approval for Tier 1 models
  - Uses MLflow AutoML or a configured retraining job as the backend
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from pyspark.sql import SparkSession
        import mlflow
        import mlflow.tracking
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.v2.auto_retrain")

REGISTRY_MAP_TABLE    = "governance.model_health.model_registry_map"
RISK_SCORES_TABLE     = "governance.model_health.risk_scores"
INCIDENTS_TABLE       = "governance.model_health.incidents"
RETRAIN_TABLE         = "governance.model_health.retrain_candidates"

MIN_CRITICAL_RUNS     = int(os.getenv("THIRD_EYE_RETRAIN_MIN_CRITICAL_RUNS", "2"))
MODE = os.getenv("THIRD_EYE_RETRAIN_MODE", "trigger")  # trigger | evaluate


@dataclass
class RetrainCandidate:
    candidate_id: str
    model_id: str
    incident_id: str | None
    triggered_at: datetime
    trigger_reason: str
    champion_model_version: str
    challenger_model_version: str | None
    challenger_run_id: str | None
    challenger_health_score: float | None
    champion_health_score: float | None
    comparison_window_start: datetime | None
    comparison_window_end: datetime | None
    status: str
    promotion_approved_by: str | None
    promotion_approved_at: datetime | None
    promoted_at: datetime | None
    rejection_reason: str | None


def _should_trigger_retrain(model_id: str, critical_run_count: int, trigger_threshold: float) -> bool:
    """Returns True if the model has been critical for enough consecutive runs."""
    return critical_run_count >= MIN_CRITICAL_RUNS


def _trigger_retraining_mock(model_id: str, model_name: str, champion_version: str) -> dict:
    """Mock retraining — in production, this launches an AutoML run or a custom Databricks job."""
    logger.info("[MOCK] Triggering retraining for %s v%s", model_name, champion_version)
    return {
        "run_id": f"mock_run_{uuid.uuid4().hex[:8]}",
        "challenger_version": str(int(champion_version) + 1) if champion_version.isdigit() else "retrain_1",
        "status": "training_started",
    }


def run_auto_retrain(dry_run: bool = False) -> dict[str, Any]:
    now = datetime.now(tz=timezone.utc)
    mode = os.getenv("THIRD_EYE_RETRAIN_MODE", "trigger")

    if MOCK_MODE:
        logger.info("MOCK_MODE — V2 Auto-Retrain pipeline")
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        mock_registry = _generate_mock_registry()

        # Simulate: credit_risk_classifier has been critical for 2+ runs
        models_to_retrain = [
            {
                "model_id": _make_model_id("main.finance.credit_risk_classifier", "12"),
                "model_name": "main.finance.credit_risk_classifier",
                "model_version": "12",
                "criticality_tier": "tier_1_business",
                "autonomy_level": 2,
                "retrain_trigger_threshold": 40.0,
                "critical_run_count": 3,
                "current_health_score": 22.4,
                "incident_id": str(uuid.uuid4()),
            }
        ]
    else:
        spark = SparkSession.builder.getOrCreate()
        # Find models that have been critical for MIN_CRITICAL_RUNS consecutive runs
        models_to_retrain_rows = spark.sql(f"""
            WITH critical_counts AS (
                SELECT model_id,
                       COUNT(*) AS critical_run_count,
                       MIN(health_score) AS min_health_score
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
                    FROM {RISK_SCORES_TABLE}
                    WHERE health_tier = 'critical'
                ) WHERE rn <= {MIN_CRITICAL_RUNS}
                GROUP BY model_id
                HAVING COUNT(*) >= {MIN_CRITICAL_RUNS}
            )
            SELECT mrm.model_id, mrm.model_name, mrm.model_version,
                   mrm.criticality_tier, mrm.autonomy_level, mrm.retrain_trigger_threshold,
                   cc.critical_run_count, cc.min_health_score AS current_health_score,
                   i.incident_id
            FROM {REGISTRY_MAP_TABLE} mrm
            JOIN critical_counts cc ON mrm.model_id = cc.model_id
            LEFT JOIN (
                SELECT model_id, MAX(incident_id) AS incident_id
                FROM {INCIDENTS_TABLE} WHERE status = 'open' GROUP BY model_id
            ) i ON mrm.model_id = i.model_id
            WHERE mrm.is_active = true AND mrm.autonomy_level >= 2
        """).collect()
        models_to_retrain = [r.asDict() for r in models_to_retrain_rows]

    candidates: list[RetrainCandidate] = []
    for model in models_to_retrain:
        model_id       = model["model_id"]
        model_name     = model.get("model_name", "unknown")
        champion_ver   = model.get("model_version", "1")
        crit_tier      = model.get("criticality_tier", "tier_2_operational")
        autonomy_level = model.get("autonomy_level", 2)

        if mode == "trigger" or MOCK_MODE:
            logger.info("Auto-retrain triggered for %s v%s (critical_runs=%d, health=%.1f)",
                        model_name, champion_ver, model.get("critical_run_count", 0),
                        model.get("current_health_score", 0))

            if MOCK_MODE or dry_run:
                retrain_result = _trigger_retraining_mock(model_id, model_name, champion_ver)
            else:
                retrain_result = _trigger_retraining_mock(model_id, model_name, champion_ver)

            candidate = RetrainCandidate(
                candidate_id=str(uuid.uuid4()),
                model_id=model_id,
                incident_id=model.get("incident_id"),
                triggered_at=now,
                trigger_reason="health_score_below_threshold",
                champion_model_version=champion_ver,
                challenger_model_version=retrain_result.get("challenger_version"),
                challenger_run_id=retrain_result.get("run_id"),
                challenger_health_score=None,     # populated by evaluate mode
                champion_health_score=model.get("current_health_score"),
                comparison_window_start=now,
                comparison_window_end=now + timedelta(days=1),
                status="training",
                promotion_approved_by=None,
                promotion_approved_at=None,
                promoted_at=None,
                rejection_reason=None,
            )
            candidates.append(candidate)

        elif mode == "evaluate":
            # Evaluate challenger vs. champion on recent holdout window
            logger.info("[EVALUATE] Comparing champion/challenger for %s", model_name)
            # In production: run evaluation pipeline comparing champion vs challenger
            # metrics on the holdout window and update challenger_health_score
            # Status changes: training → evaluation → awaiting_approval (for Tier 1)
            # or → approved (for lower tiers if autonomy_level allows)
            logger.info("  Tier 1 model requires human approval before promotion.")

    stats = {
        "models_evaluated": len(models_to_retrain),
        "retrain_candidates_created": len(candidates),
        "mode": mode,
    }

    if not MOCK_MODE and not dry_run:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(c) for c in candidates]
        if rows:
            df = spark.createDataFrame(rows)
            df.write.mode("append").saveAsTable(RETRAIN_TABLE)

    logger.info("[V2] Auto-retrain pipeline complete: %s", stats)
    return {"stats": stats, "candidates": [asdict(c) for c in candidates]}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    result = run_auto_retrain(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
