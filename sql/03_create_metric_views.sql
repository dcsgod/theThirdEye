-- =============================================================================
-- The Third Eye — Step 3: Metric Views (Genie-Ready)
-- =============================================================================
-- These views are the Genie Space's query surface and the AI/BI dashboard's
-- primary data source. They perform the joins and computations so that Genie
-- can answer business questions without raw SQL knowledge.
--
-- Run after: 02_create_tables.sql
-- =============================================================================

USE CATALOG governance;
USE SCHEMA  model_health;

-- ---------------------------------------------------------------------------
-- 1. vw_fleet_health_summary
-- ---------------------------------------------------------------------------
-- Purpose  : Fleet-level KPI tiles on the dashboard + Genie fleet questions
-- Answers  : "What % of my models are healthy?", "How many open incidents?",
--            "What is the average health score across the fleet today?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_fleet_health_summary AS
WITH latest_scores AS (
  -- Take the most recent risk_score per model
  SELECT
    rs.model_id,
    rs.health_score,
    rs.health_tier,
    rs.confidence,
    rs.computed_at,
    rs.tier_changed,
    rs.drift_component,
    rs.quality_component,
    rs.cost_component,
    rs.guardrail_component,
    ROW_NUMBER() OVER (PARTITION BY rs.model_id ORDER BY rs.computed_at DESC) AS rn
  FROM governance.model_health.risk_scores rs
),
current_scores AS (
  SELECT * FROM latest_scores WHERE rn = 1
),
incident_counts AS (
  SELECT
    model_id,
    COUNT(*) FILTER (WHERE status = 'open')         AS open_incidents,
    COUNT(*) FILTER (WHERE status != 'false_positive') AS total_incidents_ever
  FROM governance.model_health.incidents
  GROUP BY model_id
)
SELECT
  -- Counts by tier
  COUNT(*)                                                         AS total_models,
  COUNT(*) FILTER (WHERE cs.health_tier = 'healthy')              AS healthy_count,
  COUNT(*) FILTER (WHERE cs.health_tier = 'watch')                AS watch_count,
  COUNT(*) FILTER (WHERE cs.health_tier = 'at_risk')              AS at_risk_count,
  COUNT(*) FILTER (WHERE cs.health_tier = 'critical')             AS critical_count,
  -- Percentages
  ROUND(100.0 * COUNT(*) FILTER (WHERE cs.health_tier = 'healthy') / COUNT(*), 1)  AS pct_healthy,
  ROUND(100.0 * COUNT(*) FILTER (WHERE cs.health_tier = 'watch')   / COUNT(*), 1)  AS pct_watch,
  ROUND(100.0 * COUNT(*) FILTER (WHERE cs.health_tier = 'at_risk') / COUNT(*), 1)  AS pct_at_risk,
  ROUND(100.0 * COUNT(*) FILTER (WHERE cs.health_tier = 'critical')/ COUNT(*), 1)  AS pct_critical,
  -- Aggregate scores
  ROUND(AVG(cs.health_score), 2)                                  AS avg_health_score,
  ROUND(MIN(cs.health_score), 2)                                  AS min_health_score,
  ROUND(AVG(cs.confidence), 3)                                    AS avg_confidence,
  -- Incidents
  COALESCE(SUM(ic.open_incidents), 0)                             AS total_open_incidents,
  COUNT(*) FILTER (WHERE cs.tier_changed = true)                  AS tier_changes_today,
  -- Timestamps
  MAX(cs.computed_at)                                             AS last_scoring_run,
  current_timestamp()                                             AS view_generated_at
FROM current_scores cs
JOIN governance.model_health.model_registry_map mrm ON cs.model_id = mrm.model_id AND mrm.is_active = true
LEFT JOIN incident_counts ic ON cs.model_id = ic.model_id;

-- ---------------------------------------------------------------------------
-- 2. vw_model_leaderboard
-- ---------------------------------------------------------------------------
-- Purpose  : Model-level table on dashboard, sortable. Genie "worst models" queries.
-- Answers  : "Which models are most at risk?", "Show me Tier-1 models below 60 health"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_model_leaderboard AS
WITH latest_scores AS (
  SELECT
    model_id, health_score, health_tier, confidence, drift_component,
    quality_component, cost_component, guardrail_component,
    tier_changed, computed_at,
    ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
  FROM governance.model_health.risk_scores
),
open_incidents AS (
  SELECT model_id, COUNT(*) AS open_incident_count
  FROM governance.model_health.incidents
  WHERE status = 'open'
  GROUP BY model_id
),
signal_summary AS (
  SELECT
    model_id,
    MAX(ingested_at) FILTER (WHERE signal_type = 'drift')      AS last_drift_signal,
    MAX(ingested_at) FILTER (WHERE signal_type = 'accuracy')   AS last_accuracy_signal,
    MAX(ingested_at) FILTER (WHERE signal_type = 'cost')       AS last_cost_signal,
    MAX(latest_value) FILTER (WHERE signal_type = 'drift'
                              AND threshold_breached = true)   AS max_drift_breach_value
  FROM governance.model_health.signal_index
  WHERE ingested_at >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY model_id
)
SELECT
  mrm.model_id,
  mrm.model_name,
  mrm.model_version,
  mrm.serving_endpoint,
  mrm.business_domain,
  mrm.owning_team,
  mrm.criticality_tier,
  mrm.asset_type,
  mrm.gateway_registered,
  mrm.lakehouse_monitor_configured,
  -- Health
  ls.health_score,
  ls.health_tier,
  ls.confidence,
  CASE
    WHEN ls.tier_changed = true THEN '↑↓ changed'
    WHEN ls.health_score >= 80  THEN '→ stable'
    WHEN ls.health_score >= 60  THEN '↓ declining'
    ELSE '↓↓ deteriorating'
  END AS trend_label,
  -- Components
  ls.drift_component,
  ls.quality_component,
  ls.cost_component,
  ls.guardrail_component,
  -- Signals
  ss.last_drift_signal,
  ss.last_accuracy_signal,
  ss.last_cost_signal,
  ss.max_drift_breach_value,
  -- Incidents
  COALESCE(oi.open_incident_count, 0)  AS open_incidents,
  -- Timestamps
  ls.computed_at                        AS last_scored_at,
  mrm.last_retrained_at,
  mrm.updated_at                        AS registry_updated_at
FROM governance.model_health.model_registry_map mrm
JOIN latest_scores ls ON mrm.model_id = ls.model_id AND ls.rn = 1
LEFT JOIN open_incidents oi ON mrm.model_id = oi.model_id
LEFT JOIN signal_summary  ss ON mrm.model_id = ss.model_id
WHERE mrm.is_active = true
ORDER BY ls.health_score ASC, mrm.criticality_tier ASC;

-- ---------------------------------------------------------------------------
-- 3. vw_active_incidents
-- ---------------------------------------------------------------------------
-- Purpose  : Breach notification panel + Genie incident investigation
-- Answers  : "What's wrong with model X?", "Which incidents are still open?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_active_incidents AS
SELECT
  i.incident_id,
  i.model_id,
  mrm.model_name,
  mrm.serving_endpoint,
  mrm.business_domain,
  mrm.owning_team,
  mrm.criticality_tier,
  i.opened_at,
  i.severity,
  i.status,
  i.recommended_action,
  i.trigger_signal_types,
  i.root_cause_narrative,
  i.root_cause_confidence,
  -- Linked remediation
  rs_rem.explanation       AS remediation_explanation,
  rs_rem.urgency           AS remediation_urgency,
  rs_rem.suggested_actions AS remediation_actions,
  -- Current health context
  risk.health_score        AS current_health_score,
  risk.health_tier         AS current_health_tier,
  -- Timestamps
  i.acknowledged_by,
  i.acknowledged_at,
  ROUND(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(i.opened_at)) / 3600.0 AS age_hours
FROM governance.model_health.incidents i
JOIN governance.model_health.model_registry_map mrm ON i.model_id = mrm.model_id
LEFT JOIN governance.model_health.remediation_suggestions rs_rem
  ON i.incident_id = rs_rem.incident_id
LEFT JOIN (
  SELECT model_id, health_score, health_tier,
    ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
  FROM governance.model_health.risk_scores
) risk ON i.model_id = risk.model_id AND risk.rn = 1
WHERE i.status IN ('open', 'acknowledged', 'in_progress')
ORDER BY i.severity DESC, i.opened_at ASC;

-- ---------------------------------------------------------------------------
-- 4. vw_daily_health_changes
-- ---------------------------------------------------------------------------
-- Purpose  : Feeds the Copilot Studio scheduled digest.
--            "Which models changed health tier in the last 24 hours?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_daily_health_changes AS
SELECT
  rs.model_id,
  mrm.model_name,
  mrm.business_domain,
  mrm.owning_team,
  mrm.criticality_tier,
  mrm.serving_endpoint,
  rs.health_score,
  rs.health_tier         AS new_health_tier,
  rs.prev_health_tier    AS previous_health_tier,
  rs.computed_at,
  rs.confidence,
  -- Direction of change
  CASE
    WHEN rs.health_score > rs.prev_health_score THEN 'improved'
    ELSE 'degraded'
  END AS change_direction,
  ROUND(ABS(rs.health_score - COALESCE(rs.prev_health_score, rs.health_score)), 2) AS score_delta
FROM governance.model_health.risk_scores rs
JOIN governance.model_health.model_registry_map mrm ON rs.model_id = mrm.model_id
WHERE rs.tier_changed = true
  AND rs.computed_at >= current_timestamp() - INTERVAL 24 HOURS
  AND mrm.is_active = true
ORDER BY mrm.criticality_tier ASC, ABS(rs.health_score - COALESCE(rs.prev_health_score, rs.health_score)) DESC;

-- ---------------------------------------------------------------------------
-- 5. vw_cost_usage_trend
-- ---------------------------------------------------------------------------
-- Purpose  : Gateway cost/usage panel on dashboard + Genie cost questions
-- Answers  : "Which model costs the most?", "Show me token usage trend for X"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_cost_usage_trend AS
SELECT
  si.model_id,
  mrm.model_name,
  mrm.serving_endpoint,
  mrm.business_domain,
  mrm.owning_team,
  mrm.criticality_tier,
  si.signal_subtype                  AS usage_metric,
  DATE(si.computed_at)               AS usage_date,
  SUM(si.raw_value)                  AS total_raw_value,
  AVG(si.raw_value)                  AS avg_raw_value,
  MAX(si.raw_value)                  AS peak_raw_value,
  si.raw_unit,
  COUNT(*)                           AS data_points,
  -- Anomaly flag
  MAX(CAST(si.threshold_breached AS INT)) AS had_threshold_breach
FROM governance.model_health.signal_index si
JOIN governance.model_health.model_registry_map mrm ON si.model_id = mrm.model_id
WHERE si.signal_type IN ('cost', 'usage', 'token_usage', 'latency')
  AND si.computed_at >= current_timestamp() - INTERVAL 30 DAYS
  AND mrm.is_active = true
GROUP BY
  si.model_id, mrm.model_name, mrm.serving_endpoint,
  mrm.business_domain, mrm.owning_team, mrm.criticality_tier,
  si.signal_subtype, DATE(si.computed_at), si.raw_unit
ORDER BY si.model_id, usage_date DESC;

-- ---------------------------------------------------------------------------
-- 6. vw_model_detail
-- ---------------------------------------------------------------------------
-- Purpose  : Model drill-through page on dashboard + Genie "why" questions
-- Answers  : "Why is model X at risk?", "What signals fired for X today?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_model_detail AS
SELECT
  mrm.model_id,
  mrm.model_name,
  mrm.model_version,
  mrm.serving_endpoint,
  mrm.business_domain,
  mrm.owning_team,
  mrm.criticality_tier,
  mrm.asset_type,
  mrm.gateway_registered,
  mrm.lakehouse_monitor_configured,
  mrm.lakehouse_monitor_schema,
  mrm.inference_table,
  mrm.mlflow_experiment_id,
  -- Latest health
  rs.health_score,
  rs.health_tier,
  rs.confidence,
  rs.drift_component,
  rs.quality_component,
  rs.cost_component,
  rs.guardrail_component,
  rs.computed_at                  AS last_scored_at,
  -- Signal summary (last 7 days)
  sig_agg.drift_signal_count,
  sig_agg.accuracy_signal_count,
  sig_agg.guardrail_signal_count,
  sig_agg.cost_signal_count,
  sig_agg.breach_count,
  -- Lineage
  lin_agg.upstream_table_count,
  lin_agg.recent_lineage_events,
  -- Incidents
  inc_agg.total_incidents,
  inc_agg.open_incidents,
  inc_agg.last_incident_at,
  -- Latest remediation
  rem.explanation                 AS latest_remediation_explanation,
  rem.suggested_actions           AS latest_suggested_actions,
  rem.urgency                     AS latest_urgency,
  -- Registry metadata
  mrm.created_at,
  mrm.updated_at                  AS registry_updated_at,
  mrm.last_retrained_at,
  mrm.autonomy_level              -- V2
FROM governance.model_health.model_registry_map mrm
LEFT JOIN (
  SELECT model_id, health_score, health_tier, confidence,
         drift_component, quality_component, cost_component, guardrail_component, computed_at,
         ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
  FROM governance.model_health.risk_scores
) rs ON mrm.model_id = rs.model_id AND rs.rn = 1
LEFT JOIN (
  SELECT
    model_id,
    COUNT(*) FILTER (WHERE signal_type = 'drift')    AS drift_signal_count,
    COUNT(*) FILTER (WHERE signal_type = 'accuracy') AS accuracy_signal_count,
    COUNT(*) FILTER (WHERE signal_type = 'guardrail')AS guardrail_signal_count,
    COUNT(*) FILTER (WHERE signal_type = 'cost')     AS cost_signal_count,
    COUNT(*) FILTER (WHERE threshold_breached = true)AS breach_count
  FROM governance.model_health.signal_index
  WHERE ingested_at >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY model_id
) sig_agg ON mrm.model_id = sig_agg.model_id
LEFT JOIN (
  SELECT
    model_id,
    COUNT(DISTINCT upstream_table)                               AS upstream_table_count,
    COUNT(*) FILTER (WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS) AS recent_lineage_events
  FROM governance.model_health.lineage_events
  GROUP BY model_id
) lin_agg ON mrm.model_id = lin_agg.model_id
LEFT JOIN (
  SELECT
    model_id,
    COUNT(*)                       AS total_incidents,
    COUNT(*) FILTER (WHERE status = 'open') AS open_incidents,
    MAX(opened_at)                 AS last_incident_at
  FROM governance.model_health.incidents
  GROUP BY model_id
) inc_agg ON mrm.model_id = inc_agg.model_id
LEFT JOIN (
  SELECT rs2.model_id, rs2.explanation, rs2.suggested_actions, rs2.urgency,
         ROW_NUMBER() OVER (PARTITION BY rs2.model_id ORDER BY rs2.generated_at DESC) AS rn
  FROM governance.model_health.remediation_suggestions rs2
  JOIN governance.model_health.incidents i2 ON rs2.incident_id = i2.incident_id
) rem ON mrm.model_id = rem.model_id AND rem.rn = 1
WHERE mrm.is_active = true;

-- ---------------------------------------------------------------------------
-- 7. vw_lineage_context
-- ---------------------------------------------------------------------------
-- Purpose  : Lineage panel on model detail page + Genie lineage questions
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_lineage_context AS
SELECT
  le.event_id,
  le.model_id,
  mrm.model_name,
  mrm.criticality_tier,
  le.upstream_table,
  le.event_type,
  le.event_time,
  le.entity_type,
  le.entity_name,
  le.hop_distance,
  le.row_count_before,
  le.row_count_after,
  CASE WHEN le.row_count_before IS NOT NULL AND le.row_count_after IS NOT NULL
    THEN le.row_count_after - le.row_count_before
    ELSE NULL
  END AS row_count_delta,
  -- Correlation: was there a health degradation near this event?
  CASE WHEN corr.model_id IS NOT NULL THEN true ELSE false END AS correlated_with_incident
FROM governance.model_health.lineage_events le
JOIN governance.model_health.model_registry_map mrm ON le.model_id = mrm.model_id
LEFT JOIN (
  SELECT DISTINCT model_id FROM governance.model_health.incidents
  WHERE status != 'false_positive'
) corr ON le.model_id = corr.model_id
WHERE le.event_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY le.model_id, le.event_time DESC;

-- ---------------------------------------------------------------------------
-- 8. vw_signal_history (last 30 days per model)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW governance.model_health.vw_signal_history AS
SELECT
  si.signal_id,
  si.model_id,
  mrm.model_name,
  mrm.criticality_tier,
  mrm.business_domain,
  si.signal_type,
  si.signal_subtype,
  si.computed_at,
  si.latest_value        AS normalized_value,
  si.raw_value,
  si.raw_unit,
  si.column_name,
  si.threshold_value,
  si.threshold_breached,
  si.source_table,
  si.window_start,
  si.window_end
FROM governance.model_health.signal_index si
JOIN governance.model_health.model_registry_map mrm ON si.model_id = mrm.model_id
WHERE si.computed_at >= current_timestamp() - INTERVAL 30 DAYS
  AND mrm.is_active = true
ORDER BY si.model_id, si.computed_at DESC;

-- ---------------------------------------------------------------------------
-- Done
-- ---------------------------------------------------------------------------
SELECT 'Step 03 complete: 8 metric views created in governance.model_health.' AS status;
