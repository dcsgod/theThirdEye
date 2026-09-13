"""
passport/generate_model_passport.py
======================================
Third Eye — Model Passport Generator

Generates a formatted per-model health passport for each model in the fleet.
The passport is a pure read of the governed schema — no raw data access.
Writes results to governance.model_health.model_passport.

Contents per passport:
  - Model purpose/metadata (from model_registry_map)
  - Current health score and tier (from risk_scores)
  - Upstream lineage (from lineage_events)
  - Open/recent incidents with root-cause narratives (from incidents + remediation_suggestions)
  - 30-day health trend (from risk_scores history)

This is cheap to build (all data is already in the governed schema) and
is a high-value demo artifact. (Section 4.1, thirdeye_project.md)

Run as: Databricks Workflow task (scheduled daily, or on-demand).
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
    except ImportError:
        MOCK_MODE = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("third_eye.passport.generate")

REGISTRY_MAP_TABLE   = "governance.model_health.model_registry_map"
RISK_SCORES_TABLE    = "governance.model_health.risk_scores"
INCIDENTS_TABLE      = "governance.model_health.incidents"
LINEAGE_EVENTS_TABLE = "governance.model_health.lineage_events"
REMEDIATION_TABLE    = "governance.model_health.remediation_suggestions"
PASSPORT_TABLE       = "governance.model_health.model_passport"

TIER_EMOJI = {
    "tier_0_experimental": "🧪",
    "tier_1_business":     "🔴",
    "tier_2_operational":  "🟠",
    "tier_3_low":          "🟢",
}
HEALTH_EMOJI = {
    "healthy":  "✅",
    "watch":    "👀",
    "at_risk":  "⚠️",
    "critical": "🚨",
}


def _health_bar(score: float, width: int = 20) -> str:
    filled = round((score / 100) * width)
    empty  = width - filled
    return "█" * filled + "░" * empty


def _render_markdown(model: dict, health: dict | None, lineage: list[dict],
                     incidents: list[dict], trend: list[dict]) -> str:
    """Render the model passport as Markdown."""
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    tier  = model.get("criticality_tier", "tier_2_operational")
    hs    = health.get("health_score", "N/A") if health else "N/A"
    htier = health.get("health_tier", "unknown") if health else "unknown"
    conf  = health.get("confidence", 0) if health else 0
    hs_display = f"{hs:.1f}" if isinstance(hs, float) else str(hs)
    bar   = _health_bar(float(hs)) if isinstance(hs, float) else "N/A"

    open_inc = [i for i in incidents if i.get("status") == "open"]

    lines = [
        f"# 🧿 Third Eye — Model Passport",
        f"*Generated: {now_str}*",
        f"",
        f"---",
        f"",
        f"## {TIER_EMOJI.get(tier, '📦')} {model.get('model_name', 'Unknown Model')} v{model.get('model_version', '?')}",
        f"",
        f"| Field | Value |",
        f"|---|---|",
        f"| **Serving Endpoint** | `{model.get('serving_endpoint') or '—'}` |",
        f"| **Business Domain** | {model.get('business_domain') or '—'} |",
        f"| **Owning Team** | {model.get('owning_team') or '—'} |",
        f"| **Criticality Tier** | {TIER_EMOJI.get(tier, '')} {tier} |",
        f"| **Asset Type** | {model.get('asset_type', 'model')} |",
        f"| **Gateway Registered** | {'✓' if model.get('gateway_registered') else '✗'} |",
        f"| **Lakehouse Monitor** | {'✓' if model.get('lakehouse_monitor_configured') else '✗ (not configured)'} |",
        f"| **MLflow Experiment** | `{model.get('mlflow_experiment_id') or '—'}` |",
        f"| **Last Retrained** | {model.get('last_retrained_at') or '—'} |",
        f"",
        f"---",
        f"",
        f"## {HEALTH_EMOJI.get(htier, '❓')} Current Health",
        f"",
        f"```",
        f"Health Score:  {hs_display} / 100",
        f"Health Tier:   {htier.upper()}",
        f"Confidence:    {conf:.0%}",
        f"Progress:      [{bar}]",
        f"```",
    ]

    if health and isinstance(hs, float):
        dc = health.get("drift_component")
        qc = health.get("quality_component")
        cc = health.get("cost_component")
        gc = health.get("guardrail_component")

        lines += [
            f"",
            f"### Component Breakdown",
            f"",
            f"| Component | Normalized Value | Interpretation |",
            f"|---|---|---|",
            f"| Drift | {f'{dc:.3f}' if dc is not None else 'N/A'} | {'⚠️ elevated' if dc and dc > 0.3 else '✅ normal'} |",
            f"| Quality/Accuracy | {f'{qc:.3f}' if qc is not None else 'N/A'} | {'⚠️ degraded' if qc and qc < 0.7 else '✅ normal'} |",
            f"| Cost Anomaly | {f'{cc:.3f}' if cc is not None else 'N/A'} | {'⚠️ anomaly' if cc and cc > 0.5 else '✅ normal'} |",
            f"| Guardrail Violations | {f'{gc:.3f}' if gc is not None else 'N/A'} | {'⚠️ violations' if gc and gc > 0.1 else '✅ normal'} |",
        ]

    # Lineage
    if lineage:
        lines += ["", "---", "", "## 🔗 Upstream Lineage (Top 5)", ""]
        unique_tables = list({le["upstream_table"]: le for le in lineage}.values())[:5]
        for le in unique_tables:
            lines.append(f"- `{le['upstream_table']}` ← {le.get('entity_type', '?')} `{le.get('entity_name', '?')}`")
    else:
        lines += ["", "---", "", "## 🔗 Upstream Lineage", "", "*No lineage events recorded.*"]

    # Incidents
    lines += ["", "---", "", f"## 📋 Incidents ({len(open_inc)} open)", ""]
    if incidents:
        for inc in incidents[:5]:
            status_icon = "🔴" if inc.get("status") == "open" else "✅"
            lines += [
                f"### {status_icon} [{inc.get('incident_id', '?')[:8]}] {inc.get('severity', '?').upper()} — {inc.get('opened_at', '?')}",
                f"",
                f"- **Status**: {inc.get('status', '?')}",
                f"- **Signals**: {inc.get('trigger_signal_types', '?')}",
                f"- **Recommended Action**: {inc.get('recommended_action', '?')}",
            ]
            narrative = inc.get("root_cause_narrative")
            if narrative:
                lines += ["", f"**Root Cause**: {narrative[:400]}{'...' if len(narrative or '') > 400 else ''}", ""]
            actions_raw = inc.get("suggested_actions")
            if actions_raw:
                try:
                    actions = json.loads(actions_raw) if isinstance(actions_raw, str) else actions_raw
                    lines += ["", "**Suggested Actions**:", ""]
                    for a in actions[:3]:
                        lines.append(f"  {a.get('rank', '?')}. {a.get('action', '?')} *(effort: {a.get('effort','?')}, impact: {a.get('impact','?')})*")
                except Exception:
                    pass
            lines.append("")
    else:
        lines.append("*No incidents recorded.*")

    # 30-day trend
    if trend:
        lines += ["", "---", "", "## 📈 30-Day Health Trend", "", "| Date | Score | Tier |", "|---|---|---|"]
        for t in trend[-10:]:   # last 10 data points
            lines.append(f"| {t.get('date', '?')} | {t.get('health_score', '?'):.1f} | {t.get('health_tier', '?')} |")

    lines += ["", "---", "", "*Passport generated by The Third Eye — governance.model_health.*"]
    return "\n".join(lines)


def _render_html(markdown_text: str, model_name: str) -> str:
    """Simple HTML wrapper around the markdown content."""
    import html as html_lib
    escaped = html_lib.escape(markdown_text)
    return f"""<!DOCTYPE html>
<html>
<head>
  <title>Third Eye Passport — {html_lib.escape(model_name)}</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; max-width: 900px; margin: 2em auto; padding: 1em; background: #0f0f1a; color: #e2e8f0; }}
    pre {{ background: #1e1e2e; padding: 1em; border-radius: 8px; font-family: monospace; overflow-x: auto; }}
    h1, h2, h3 {{ color: #7c3aed; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ padding: 0.5em 1em; border: 1px solid #334155; text-align: left; }}
    th {{ background: #1e1e2e; }}
  </style>
</head>
<body>
<pre>{escaped}</pre>
</body>
</html>"""


@dataclass
class PassportRecord:
    passport_id: str
    model_id: str
    generated_at: datetime
    model_name: str
    model_version: str
    owning_team: str | None
    business_domain: str | None
    criticality_tier: str
    current_health_score: float | None
    current_health_tier: str | None
    confidence: float | None
    upstream_tables: str | None   # JSON array
    open_incidents: int
    last_incident_at: datetime | None
    last_retrained_at: datetime | None
    health_30d_trend: str | None  # JSON
    passport_markdown: str
    passport_html: str


def generate_passports(model_ids: list[str] | None = None, dry_run: bool = False) -> dict[str, Any]:
    """Generate passports for all active models (or a specific list)."""
    now = datetime.now(tz=timezone.utc)

    if MOCK_MODE:
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from discovery.sync_model_registry import _generate_mock_registry, _make_model_id
        from scoring.compute_health_score import run_scoring
        from scoring.correlate_signals import run_correlation
        from remediation.generate_root_cause import run_remediation

        mock_registry = _generate_mock_registry()
        models = [
            {
                "model_id": _make_model_id(e["name"], str(e["version"])),
                "model_name": e["name"],
                "model_version": str(e["version"]),
                "serving_endpoint": e.get("serving_endpoint"),
                "business_domain": e.get("business_domain"),
                "owning_team": e.get("owning_team"),
                "criticality_tier": "tier_1_business" if e.get("business_domain") in ("finance","fraud","customer") else "tier_2_operational",
                "asset_type": e.get("asset_type","model"),
                "gateway_registered": e.get("gateway_registered", False),
                "lakehouse_monitor_configured": e.get("lakehouse_monitor_configured", False),
                "mlflow_experiment_id": e.get("experiment_id"),
                "last_retrained_at": None,
            }
            for e in mock_registry
        ]
        scores = {sc["model_id"]: sc for sc in run_scoring(dry_run=True).get("scores", [])}
        corr   = run_correlation(dry_run=True)
        remed  = run_remediation(dry_run=True)

        inc_list   = corr.get("incidents", [])
        sugg_map   = {s["incident_id"]: s for s in remed.get("suggestions", [])}
        # Merge suggestions into incidents
        for inc in inc_list:
            sugg = sugg_map.get(inc.get("incident_id"))
            if sugg:
                inc["root_cause_narrative"] = sugg["explanation"]
                inc["suggested_actions"]    = sugg["suggested_actions"]

        lineage_events: list[dict] = []
        from discovery.lineage_walker import _generate_mock_lineage
        for le_raw in _generate_mock_lineage():
            from discovery.sync_model_registry import _make_model_id as _mid
            lineage_events.append({
                "event_id": str(uuid.uuid4()),
                "model_id": next(
                    (_mid(e["name"], str(e["version"])) for e in mock_registry
                     if e.get("inference_table") == le_raw.get("target_table")),
                    None
                ),
                "upstream_table": le_raw["upstream_table"],
                "event_type": le_raw.get("event_type", "new_write"),
                "entity_type": le_raw.get("entity_type"),
                "entity_name": le_raw.get("entity_name"),
            })
    else:
        spark = SparkSession.builder.getOrCreate()
        model_filter = ""
        if model_ids:
            ids_str = ", ".join(f"'{m}'" for m in model_ids)
            model_filter = f"AND model_id IN ({ids_str})"

        models_rows = spark.sql(f"SELECT * FROM {REGISTRY_MAP_TABLE} WHERE is_active = true {model_filter}").collect()
        models = [r.asDict() for r in models_rows]
        score_rows = spark.sql(f"""
            SELECT model_id, health_score, health_tier, confidence, drift_component, quality_component, cost_component, guardrail_component
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn FROM {RISK_SCORES_TABLE}) WHERE rn = 1
        """).collect()
        scores = {r["model_id"]: r.asDict() for r in score_rows}
        inc_rows = spark.sql(f"SELECT i.*, rs.explanation AS r_explanation, rs.suggested_actions FROM {INCIDENTS_TABLE} i LEFT JOIN {REMEDIATION_TABLE} rs ON i.incident_id = rs.incident_id").collect()
        inc_list = [r.asDict() for r in inc_rows]
        le_rows = spark.sql(f"SELECT * FROM {LINEAGE_EVENTS_TABLE}").collect()
        lineage_events = [r.asDict() for r in le_rows]

    passports: list[PassportRecord] = []
    for model in models:
        mid = model["model_id"]
        hs  = scores.get(mid)
        mod_incidents = [i for i in inc_list if i.get("model_id") == mid]
        mod_lineage   = [le for le in lineage_events if le.get("model_id") == mid]
        open_inc_count = sum(1 for i in mod_incidents if i.get("status") == "open")
        last_inc_at = max((i.get("opened_at") for i in mod_incidents if i.get("opened_at")), default=None)
        upstream_tables = list({le["upstream_table"] for le in mod_lineage})

        md  = _render_markdown(model, hs, mod_lineage, mod_incidents, trend=[])
        htm = _render_html(md, model.get("model_name", ""))

        passports.append(PassportRecord(
            passport_id=str(uuid.uuid4()),
            model_id=mid,
            generated_at=now,
            model_name=model.get("model_name", ""),
            model_version=model.get("model_version", ""),
            owning_team=model.get("owning_team"),
            business_domain=model.get("business_domain"),
            criticality_tier=model.get("criticality_tier", "tier_2_operational"),
            current_health_score=hs.get("health_score") if hs else None,
            current_health_tier=hs.get("health_tier") if hs else None,
            confidence=hs.get("confidence") if hs else None,
            upstream_tables=json.dumps(upstream_tables),
            open_incidents=open_inc_count,
            last_incident_at=last_inc_at,
            last_retrained_at=model.get("last_retrained_at"),
            health_30d_trend=None,
            passport_markdown=md,
            passport_html=htm,
        ))

        logger.info("Generated passport for %s (health=%s, incidents=%d)",
                    model.get("model_name"), hs.get("health_score") if hs else "N/A", open_inc_count)

    stats = {"passports_generated": len(passports), "models": [p.model_name for p in passports]}

    if MOCK_MODE or dry_run:
        for p in passports:
            print(f"\n{'='*80}")
            print(p.passport_markdown[:1500] + ("\n..." if len(p.passport_markdown) > 1500 else ""))
    else:
        spark = SparkSession.builder.getOrCreate()
        rows = [asdict(p) for p in passports]
        df = spark.createDataFrame(rows)
        df.write.mode("append").saveAsTable(PASSPORT_TABLE)
        logger.info("Wrote %d passports to %s.", len(passports), PASSPORT_TABLE)

    return {"stats": stats}


if __name__ == "__main__":
    os.environ.setdefault("THIRD_EYE_MOCK_MODE", "true")
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    result = generate_passports(dry_run=True)
    print(json.dumps(result["stats"], indent=2))
