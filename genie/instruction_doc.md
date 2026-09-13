# Third Eye — Genie Space Instruction Document
## Databricks AI/BI Genie Configuration

---

## Purpose
This document instructs the Genie Space how to interpret natural language questions
about the AI model fleet health. It defines vocabulary mappings, business domain routing,
two-step drill-down patterns, and sample queries.

**Data sources (metric views — not raw tables):**
- `governance.model_health.vw_fleet_health_summary` — fleet KPIs
- `governance.model_health.vw_model_leaderboard`    — per-model rankings
- `governance.model_health.vw_active_incidents`     — open incidents + remediation
- `governance.model_health.vw_daily_health_changes` — 24-hour tier changes
- `governance.model_health.vw_cost_usage_trend`     — Gateway cost/usage
- `governance.model_health.vw_model_detail`         — full model detail
- `governance.model_health.vw_signal_history`       — raw signal timeline
- `governance.model_health.vw_lineage_context`      — upstream lineage events

---

## Vocabulary Mappings

Genie should resolve the following business terms to specific SQL predicates:

| User says... | Genie should query... |
|---|---|
| "risky models" | `vw_model_leaderboard WHERE health_tier IN ('at_risk', 'critical')` |
| "critical models" | `vw_model_leaderboard WHERE health_tier = 'critical'` |
| "healthy models" | `vw_model_leaderboard WHERE health_tier = 'healthy'` |
| "drifting models" | `vw_model_leaderboard WHERE drift_component > 0.3` |
| "costly models" | `vw_cost_usage_trend WHERE had_threshold_breach = true ORDER BY total_raw_value DESC` |
| "tier 1" / "business-critical" | `WHERE criticality_tier = 'tier_1_business'` |
| "experimental" | `WHERE criticality_tier = 'tier_0_experimental'` |
| "stale models" | `vw_model_leaderboard WHERE last_scored_at < DATEADD(HOUR, -48, CURRENT_TIMESTAMP())` |
| "open incidents" | `vw_active_incidents WHERE status = 'open'` |
| "what changed today" | `vw_daily_health_changes` |
| "models that degraded" | `vw_daily_health_changes WHERE change_direction = 'degraded'` |
| "PII violations" | `vw_active_incidents WHERE trigger_signal_types LIKE '%guardrail%'` |
| "accurate models" | `vw_model_leaderboard WHERE quality_component >= 0.85` |

---

## Two-Step Drill-Down Pattern

### Step 1 — Fleet or Domain Overview
User asks a broad question:
- *"Which models are at risk?"*
- *"Show me my finance models"*
- *"What's happening in the fraud domain?"*

→ Genie returns `vw_model_leaderboard` filtered by the relevant predicate, ranked by `health_score ASC`.
   Shows: model_name, health_score, health_tier, criticality_tier, open_incidents, trend_label.

### Step 2 — Model-Specific Investigation
User asks about a specific model:
- *"Why is credit_risk_classifier at risk?"*
- *"What signals fired for the fraud agent?"*
- *"Show me incidents for demand_forecaster"*

→ Genie returns `vw_model_detail` for that model_name, then pulls linked `vw_active_incidents`
   showing `root_cause_narrative` and `remediation_explanation`.

---

## Business Domain Routing

| User mentions... | Filter applied |
|---|---|
| finance, credit, risk, fraud, regulatory | `business_domain IN ('finance', 'risk', 'fraud', 'regulatory')` |
| operations, supply chain, logistics | `business_domain IN ('operations', 'supply_chain', 'logistics')` |
| customer, churn, NPS | `business_domain = 'customer'` |
| HR, employee | `business_domain = 'hr'` |

---

## Cost/Usage Investigation Mode

Genie can answer cost questions using `vw_cost_usage_trend`:

| Question | Query |
|---|---|
| "Which model costs the most?" | `SELECT model_name, SUM(total_raw_value) AS total_cost FROM vw_cost_usage_trend WHERE raw_unit = 'usd' GROUP BY model_name ORDER BY total_cost DESC LIMIT 10` |
| "Show token usage for X" | `SELECT usage_date, total_raw_value FROM vw_cost_usage_trend WHERE model_name LIKE '%X%' AND usage_metric = 'token_usage' ORDER BY usage_date DESC` |
| "Are there cost anomalies?" | `SELECT model_name, usage_date, peak_raw_value FROM vw_cost_usage_trend WHERE had_threshold_breach = true ORDER BY peak_raw_value DESC` |
| "Gateway spend this week" | `SELECT SUM(total_raw_value) AS weekly_spend FROM vw_cost_usage_trend WHERE raw_unit = 'usd' AND usage_date >= DATEADD(DAY, -7, CURRENT_DATE())` |

---

## Genie Sample Queries (seed these in the Genie Space)

### Fleet Health
```sql
-- Overall fleet health summary
SELECT * FROM governance.model_health.vw_fleet_health_summary;

-- How many models are in each health tier?
SELECT health_tier, COUNT(*) AS count
FROM governance.model_health.vw_model_leaderboard
GROUP BY health_tier ORDER BY count DESC;

-- What is the average health score by criticality tier?
SELECT criticality_tier, ROUND(AVG(health_score), 1) AS avg_health
FROM governance.model_health.vw_model_leaderboard
GROUP BY criticality_tier ORDER BY criticality_tier;
```

### At-Risk Models
```sql
-- Which models need attention right now?
SELECT model_name, health_score, health_tier, open_incidents,
       drift_component, quality_component, criticality_tier
FROM governance.model_health.vw_model_leaderboard
WHERE health_tier IN ('at_risk', 'critical')
ORDER BY health_score ASC, criticality_tier ASC;

-- Tier-1 models below 70 health score
SELECT model_name, health_score, health_tier, trend_label, open_incidents
FROM governance.model_health.vw_model_leaderboard
WHERE criticality_tier = 'tier_1_business' AND health_score < 70
ORDER BY health_score ASC;
```

### Incident Investigation
```sql
-- All open incidents with remediation
SELECT model_name, severity, trigger_signal_types, root_cause_narrative,
       remediation_explanation, urgency, age_hours
FROM governance.model_health.vw_active_incidents
ORDER BY severity DESC, age_hours DESC;

-- Why is a specific model at risk?
-- Replace 'credit_risk' with the model name you're investigating
SELECT model_name, health_score, health_tier, drift_component,
       quality_component, cost_component, guardrail_component,
       open_incidents, last_incident_at, latest_remediation_explanation
FROM governance.model_health.vw_model_detail
WHERE model_name LIKE '%credit_risk%';
```

### Daily Digest (feeds Copilot Studio)
```sql
-- Models that changed health tier in the last 24 hours
SELECT model_name, previous_health_tier, new_health_tier, change_direction,
       score_delta, criticality_tier, serving_endpoint
FROM governance.model_health.vw_daily_health_changes
ORDER BY criticality_tier ASC, score_delta DESC;
```

### Lineage Investigation
```sql
-- Upstream changes correlated with model incidents
SELECT model_name, upstream_table, event_type, event_time,
       entity_name, hop_distance, correlated_with_incident
FROM governance.model_health.vw_lineage_context
WHERE correlated_with_incident = true
ORDER BY event_time DESC;
```

### Cost/Usage
```sql
-- Most expensive endpoints this week
SELECT model_name, serving_endpoint,
       SUM(total_raw_value) AS weekly_spend_usd,
       MAX(peak_raw_value) AS peak_hourly_usd,
       MAX(CAST(had_threshold_breach AS INT)) AS had_anomaly
FROM governance.model_health.vw_cost_usage_trend
WHERE raw_unit = 'usd'
  AND usage_date >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY model_name, serving_endpoint
ORDER BY weekly_spend_usd DESC;
```

---

## Metric Definitions (for Genie to resolve ambiguous terms)

| Metric | Definition | Source Column |
|---|---|---|
| **health_score** | 0-100 composite score; higher = healthier | `risk_scores.health_score` |
| **health_tier** | healthy / watch / at_risk / critical | `risk_scores.health_tier` |
| **confidence** | 0-1 evidence quality score; how much to trust the health_score | `risk_scores.confidence` |
| **drift_component** | normalized data drift (0-1; higher = more drift) | `risk_scores.drift_component` |
| **quality_component** | normalized model accuracy (0-1; higher = better accuracy) | `risk_scores.quality_component` |
| **cost_component** | normalized cost anomaly (0-1; higher = bigger cost spike) | `risk_scores.cost_component` |
| **guardrail_component** | normalized guardrail violation rate (0-1) | `risk_scores.guardrail_component` |
| **criticality_tier** | tier_0_experimental / tier_1_business / tier_2_operational / tier_3_low | `model_registry_map.criticality_tier` |

---

## Guardrails for Genie

- Genie should NOT write directly to any table — read-only on all views.
- Genie should default to the 7-day window for trend questions unless a different window is specified.
- For "why" questions about a specific model, always show `root_cause_narrative` if available before showing raw signal values.
- Genie should translate `health_score` into plain language:
  - 80-100: *"This model is healthy."*
  - 60-79: *"This model is under watch — early signs of degradation."*
  - 40-59: *"This model is at risk and may require intervention."*
  - 0-39: *"This model is in a critical state and requires immediate attention."*
