"""
remediation/generate_root_cause.py
=====================================
Third Eye — Root Cause Narrative Generator

For each new open incident, calls a Databricks Foundation Model endpoint
via ai_query() with the incident's evidence bundle and asks for:
  1. Plain-language explanation of what happened and why
  2. Ranked remediation actions
  3. Urgency assessment

Results are written to governance.model_health.remediation_suggestions.
The incidents table's root_cause_narrative column is also updated.

Design: LLM calls stay INSIDE Databricks (ai_query).
Dashboard and Copilot Studio read from remediation_suggestions — they
never call the model directly. (Section 7.3, thirdeye_project.md)

Run as: Databricks Workflow task (after correlate_signals completes,
only if new incidents were opened).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

MOCK_MODE = os.getenv("THIRD_EYE_MOCK_MODE", "false").lower() == "true"

if not MOCK_MODE:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.remediation.generate_root_cause")

REGISTRY_MAP_TABLE        = "governance.model_health.model_registry_map"
INCIDENTS_TABLE           = "governance.model_health.incidents"
SIGNAL_INDEX_TABLE        = "governance.model_health.signal_index"
LINEAGE_EVENTS_TABLE      = "governance.model_health.lineage_events"
RISK_SCORES_TABLE         = "governance.model_health.risk_scores"
REMEDIATION_TABLE         = "governance.model_health.remediation_suggestions"

FOUNDATION_MODEL_ENDPOINT = os.getenv(
    "THIRD_EYE_FOUNDATION_MODEL",
    "databricks-meta-llama-3-3-70b-instruct"
)

# Max tokens for LLM call (keep evidence bundle concise)
MAX_EVIDENCE_CHARS = 4000


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are an expert MLOps engineer and AI model health analyst.
You analyze model health incidents in a Databricks ModelOps control plane.
You will be given a structured evidence bundle about a model health incident
and must respond in valid JSON with exactly three fields:
  - "explanation": a clear, specific, plain-language explanation (3-6 sentences)
    of what happened and the likely root cause based on the evidence.
  - "suggested_actions": a JSON array of action objects, each with:
    { "rank": int, "action": str, "effort": "low|medium|high", "impact": "low|medium|high", "command": str or null }
    Rank from most important to least important (rank 1 = do first).
    Include 3-5 actions. Be specific and actionable.
  - "urgency": "urgent" | "can_wait" | "monitor_only"
    Base urgency on criticality tier and severity of the incident.

Always ground your explanation in the specific evidence provided.
Do not hallucinate metric values not present in the evidence.
"""

def _build_evidence_prompt(incident: dict, signals: list[dict], lineage: list[dict],
                            model_info: dict, health_score: dict | None) -> str:
    """Builds the evidence bundle prompt sent to the Foundation Model."""
    ev = {
        "model_name": model_info.get("model_name", "unknown"),
        "model_version": model_info.get("model_version", "unknown"),
        "criticality_tier": model_info.get("criticality_tier", "unknown"),
        "business_domain": model_info.get("business_domain", "unknown"),
        "incident_severity": incident.get("severity", "unknown"),
        "incident_opened_at": str(incident.get("opened_at", "")),
        "trigger_signal_types": incident.get("trigger_signal_types", ""),
        "recommended_action": incident.get("recommended_action", ""),
        "current_health_score": health_score.get("health_score", "N/A") if health_score else "N/A",
        "current_health_tier": health_score.get("health_tier", "N/A") if health_score else "N/A",
        "signals": [
            {
                "signal_type": s.get("signal_type"),
                "signal_subtype": s.get("signal_subtype"),
                "raw_value": s.get("raw_value"),
                "raw_unit": s.get("raw_unit"),
                "normalized_value": round(float(s.get("latest_value", 0)), 3),
                "threshold_breached": s.get("threshold_breached"),
                "column_name": s.get("column_name"),
                "computed_at": str(s.get("computed_at", "")),
                "source_table": s.get("source_table"),
            }
            for s in signals[:20]  # cap at 20 signals in prompt
        ],
        "lineage_events": [
            {
                "upstream_table": le.get("upstream_table"),
                "event_type": le.get("event_type"),
                "event_time": str(le.get("event_time", "")),
                "entity_type": le.get("entity_type"),
                "entity_name": le.get("entity_name"),
                "row_count_delta": (
                    le.get("row_count_after", 0) - le.get("row_count_before", 0)
                    if le.get("row_count_after") and le.get("row_count_before") else None
                ),
            }
            for le in lineage[:5]  # cap at 5 lineage events
        ],
    }

    prompt_text = f"""Analyze this AI model health incident and provide a root-cause explanation and remediation plan.

EVIDENCE BUNDLE:
{json.dumps(ev, indent=2, default=str)}

Respond only with a valid JSON object containing "explanation", "suggested_actions", and "urgency".
"""
    # Truncate if too long
    if len(prompt_text) > MAX_EVIDENCE_CHARS:
        prompt_text = prompt_text[:MAX_EVIDENCE_CHARS] + "\n...[truncated]\n\nRespond only with valid JSON."
    return prompt_text


# ---------------------------------------------------------------------------
# Mock LLM responses
# ---------------------------------------------------------------------------
MOCK_RESPONSES: dict[str, dict] = {
    "drift_cost_guardrail": {
        "explanation": (
            "The credit risk classifier (v12) experienced simultaneous data drift on the 'income_level' feature "
            "(drift score 0.73, well above the 0.15 threshold), a significant cost spike ($47.80 vs. baseline ~$10/hr), "
            "and elevated PII violation rates (45/1000 requests). The upstream 'raw.transactions' table had a schema "
            "change 26 hours before the drift was detected, which is the most likely root cause — the income_level "
            "field distribution changed when the upstream ETL pipeline was modified. The PII violations indicate that "
            "raw SSN values are now flowing through without the expected masking applied upstream."
        ),
        "suggested_actions": [
            {"rank": 1, "action": "Inspect the schema change in main.raw.transactions and confirm income_level encoding changed", "effort": "low", "impact": "high", "command": "DESCRIBE HISTORY main.raw.transactions LIMIT 10"},
            {"rank": 2, "action": "Re-run the feature engineering pipeline with the corrected upstream schema to produce aligned features", "effort": "medium", "impact": "high", "command": "dbutils.notebook.run('/pipelines/feature_engineering/credit_features', timeout_seconds=3600)"},
            {"rank": 3, "action": "Enable PII masking on the transactions pipeline output before it reaches feature engineering", "effort": "medium", "impact": "high", "command": None},
            {"rank": 4, "action": "Trigger a champion/challenger evaluation of credit_risk_classifier v12 vs. a retrained candidate on corrected features", "effort": "high", "impact": "high", "command": "third_eye retrain --model main.finance.credit_risk_classifier --version 12"},
            {"rank": 5, "action": "Update the Lakehouse Monitor baseline to the corrected feature distribution once upstream is fixed", "effort": "low", "impact": "medium", "command": None},
        ],
        "urgency": "urgent",
    },
    "guardrail_only": {
        "explanation": (
            "The fraud detection agent (v2) is experiencing a spike in prompt injection attempts "
            "(12 attempts in the past 5 hours, blocked by Unity Gateway AI Guardrails). "
            "While the guardrails successfully blocked these attempts, the attack pattern suggests "
            "a coordinated probe of the endpoint. The aggregate accuracy (88.7%) remains acceptable, "
            "but continued injection attempts could reveal model behavior patterns to adversaries."
        ),
        "suggested_actions": [
            {"rank": 1, "action": "Review Unity Gateway guardrail logs to identify the originating IP range and consider rate-limiting", "effort": "low", "impact": "high", "command": None},
            {"rank": 2, "action": "Enable stricter input validation guardrail rules for the prod-fraud-gateway endpoint", "effort": "low", "impact": "medium", "command": None},
            {"rank": 3, "action": "Notify the security team of the coordinated probe pattern for further investigation", "effort": "low", "impact": "high", "command": None},
        ],
        "urgency": "urgent",
    },
    "default": {
        "explanation": (
            "The model has experienced threshold-breaching signals that indicate potential health degradation. "
            "The combination of signals suggests data distribution changes may be affecting model performance. "
            "Further investigation of upstream data sources and recent pipeline changes is recommended."
        ),
        "suggested_actions": [
            {"rank": 1, "action": "Review upstream data sources for recent changes or anomalies", "effort": "low", "impact": "medium", "command": None},
            {"rank": 2, "action": "Compare current feature distributions against the training baseline", "effort": "medium", "impact": "high", "command": None},
            {"rank": 3, "action": "Monitor health score over the next 24 hours; escalate if it continues to degrade", "effort": "low", "impact": "low", "command": None},
        ],
        "urgency": "can_wait",
    },
}

def _mock_llm_call(prompt: str) -> tuple[dict, int]:
    """Returns a mock LLM response based on signal types in the prompt."""
    if "drift" in prompt and "cost" in prompt and "guardrail" in prompt:
        return MOCK_RESPONSES["drift_cost_guardrail"], 2800
    if "prompt_injection" in prompt or ("guardrail" in prompt and "fraud" in prompt):
        return MOCK_RESPONSES["guardrail_only"], 1400
    return MOCK_RESPONSES["default"], 1200


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class RemediationSuggestion:
    suggestion_id:      str
    incident_id:        str
    model_id:           str
    generated_at:       datetime
    explanation:        str
    suggested_actions:  str   # JSON array
    urgency:            str
    prompt_context:     str   # JSON of evidence bundle sent to LLM
    model_endpoint:     str
    token_count:        int | None
    blast_radius_summary: str | None
    cost_impact_estimate: float | None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_remediation(
    incident_ids: list[str] | None = None,  # None = process all open incidents without narrative
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Generates root-cause narratives for open incidents.
    incident_ids: explicit list of incident_ids to process (optional filter).
    """
    now = datetime.now(tz=timezone.utc)

    if MOCK_MODE:
        logger.info("MOCK_MODE — using mock correlation data and mock LLM responses.")
        # Run correlation to get incidents
        import sys, os as _os
        sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
        from scoring.correlate_signals import run_correlation
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id

        corr_result = run_correlation(dry_run=True)
        mock_incidents = corr_result.get("incidents", [])
        mock_registry  = _generate_mock_registry()

        model_info_map = {
            _make_model_id(e["name"], str(e["version"])): {
                "model_name": e["name"], "model_version": str(e["version"]),
                "criticality_tier": "tier_1_business" if e.get("business_domain") in ("finance","fraud","customer") else "tier_2_operational",
                "business_domain": e.get("business_domain"),
            }
            for e in mock_registry
        }
        # Build mock signals/lineage lookup
        from scoring.compute_health_score import _generate_mock_signals_for_scoring
        all_signals = _generate_mock_signals_for_scoring()
        from discovery.lineage_walker import _generate_mock_lineage
        all_lineage = _generate_mock_lineage()
        health_score_map: dict[str, dict] = {}
        from scoring.compute_health_score import run_scoring
        for sc in run_scoring(dry_run=True).get("scores", []):
            health_score_map[sc["model_id"]] = sc

        incidents_to_process = mock_incidents
    else:
        spark = SparkSession.builder.getOrCreate()
        if incident_ids:
            ids_str = ", ".join(f"'{i}'" for i in incident_ids)
            filter_clause = f"AND incident_id IN ({ids_str})"
        else:
            filter_clause = "AND root_cause_narrative IS NULL"

        inc_rows = spark.sql(f"""
            SELECT * FROM {INCIDENTS_TABLE}
            WHERE status = 'open' {filter_clause}
        """).collect()
        incidents_to_process = [r.asDict() for r in inc_rows]

        # Enrich with signals and lineage
        all_signals_rows = spark.sql(f"SELECT * FROM {SIGNAL_INDEX_TABLE}").collect()
        all_signals = [r.asDict() for r in all_signals_rows]
        all_lineage_rows = spark.sql(f"SELECT * FROM {LINEAGE_EVENTS_TABLE}").collect()
        all_lineage = [r.asDict() for r in all_lineage_rows]

        model_rows = spark.sql(f"SELECT * FROM {REGISTRY_MAP_TABLE}").collect()
        model_info_map = {r["model_id"]: r.asDict() for r in model_rows}
        score_rows = spark.sql(f"""
            SELECT model_id, health_score, health_tier
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn FROM {RISK_SCORES_TABLE}) WHERE rn = 1
        """).collect()
        health_score_map = {r["model_id"]: r.asDict() for r in score_rows}

    suggestions: list[RemediationSuggestion] = []
    updated_incidents: list[str] = []

    for incident in incidents_to_process:
        incident_id = incident.get("incident_id", str(uuid.uuid4()))
        model_id    = incident.get("model_id", "")
        model_info  = model_info_map.get(model_id, {})
        hs          = health_score_map.get(model_id)

        # Gather contributing signals
        trigger_signal_ids = set()
        ts_raw = incident.get("trigger_signals", "[]")
        try:
            trigger_signal_ids = set(json.loads(ts_raw) if isinstance(ts_raw, str) else ts_raw)
        except Exception:
            pass
        model_signals = [s for s in all_signals if s.get("model_id") == model_id and s.get("signal_id") in trigger_signal_ids]
        if not model_signals:
            model_signals = [s for s in all_signals if s.get("model_id") == model_id]

        # Gather corroborating lineage
        lineage_ids_raw = incident.get("lineage_context") or "[]"
        try:
            lineage_ids = set(json.loads(lineage_ids_raw) if isinstance(lineage_ids_raw, str) else lineage_ids_raw)
        except Exception:
            lineage_ids = set()
        corr_lineage = [le for le in all_lineage if le.get("event_id") in lineage_ids or le.get("model_id") == model_id]

        # Build prompt
        prompt_text = _build_evidence_prompt(incident, model_signals, corr_lineage, model_info, hs)

        # LLM call
        if MOCK_MODE or dry_run:
            response_dict, token_count = _mock_llm_call(prompt_text)
            logger.info("MOCK LLM response for incident %s (model=%s)", incident_id[:8], model_info.get("model_name", "?"))
        else:
            try:
                # Use Databricks ai_query() via Spark SQL
                prompt_escaped = prompt_text.replace("'", "\\'")
                result_row = spark.sql(f"""
                    SELECT ai_query(
                        '{FOUNDATION_MODEL_ENDPOINT}',
                        '{prompt_escaped}',
                        STRUCT(
                            '{{"type": "json_object"}}' AS response_format
                        )
                    ) AS response
                """).collect()[0]["response"]
                response_dict = json.loads(result_row)
                token_count = None  # token count not directly returned by ai_query
            except Exception as e:
                logger.error("ai_query() failed for incident %s: %s", incident_id, e)
                response_dict = MOCK_RESPONSES["default"]
                token_count = None

        explanation = response_dict.get("explanation", "Analysis pending.")
        actions     = response_dict.get("suggested_actions", [])
        urgency     = response_dict.get("urgency", "can_wait")

        logger.info(
            "Incident %s | model=%s | urgency=%s | actions=%d",
            incident_id[:8], model_info.get("model_name", model_id[:8]), urgency, len(actions)
        )
        logger.info("  Explanation: %s", explanation[:200] + "..." if len(explanation) > 200 else explanation)

        suggestion = RemediationSuggestion(
            suggestion_id=str(uuid.uuid4()),
            incident_id=incident_id,
            model_id=model_id,
            generated_at=now,
            explanation=explanation,
            suggested_actions=json.dumps(actions),
            urgency=urgency,
            prompt_context=json.dumps({"model_name": model_info.get("model_name"), "signal_count": len(model_signals), "lineage_count": len(corr_lineage)}),
            model_endpoint=FOUNDATION_MODEL_ENDPOINT,
            token_count=token_count,
            blast_radius_summary=None,     # V2
            cost_impact_estimate=None,     # V2
        )
        suggestions.append(suggestion)
        updated_incidents.append(incident_id)

    stats = {
        "incidents_processed": len(suggestions),
        "incident_ids": updated_incidents,
        "foundation_model": FOUNDATION_MODEL_ENDPOINT,
    }

    if not MOCK_MODE and not dry_run:
        spark = SparkSession.builder.getOrCreate()
        # Write remediation suggestions
        rows = [asdict(s) for s in suggestions]
        if rows:
            df = spark.createDataFrame(rows)
            df.write.mode("append").saveAsTable(REMEDIATION_TABLE)

        # Update incidents with narrative
        for s, inc in zip(suggestions, incidents_to_process):
            spark.sql(f"""
                UPDATE {INCIDENTS_TABLE}
                SET root_cause_narrative = '{s.explanation[:2000].replace("'", "\\'")}',
                    root_cause_model = '{FOUNDATION_MODEL_ENDPOINT}'
                WHERE incident_id = '{s.incident_id}'
            """)

    logger.info("Remediation complete: %s", stats)
    return {"stats": stats, "suggestions": [asdict(s) for s in suggestions]}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    result = run_remediation(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
    for s in result["suggestions"]:
        print(f"\n--- Incident {s['incident_id'][:8]} ---")
        print(f"Urgency: {s['urgency']}")
        print(f"Explanation: {s['explanation'][:300]}...")
        actions = json.loads(s["suggested_actions"])
        for a in actions[:3]:
            print(f"  [{a['rank']}] {a['action']} (effort={a['effort']}, impact={a['impact']})")
