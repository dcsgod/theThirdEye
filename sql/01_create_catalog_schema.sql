-- =============================================================================
-- The Third Eye — Step 1: Catalog, Schema, and Config Tables
-- =============================================================================
-- Catalog  : governance
-- Schema   : model_health
-- Purpose  : Bootstrap the governed namespace and configuration tables that
--            drive the health-scoring engine (risk weights, criticality tiers).
--
-- Run order: 01 → 02 → 03
-- Idempotent: all statements use IF NOT EXISTS / CREATE OR REPLACE
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Catalog
-- ---------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS governance
  COMMENT 'Third Eye governed ModelOps control plane. Read by AI/BI dashboard, Genie Space, and Copilot Studio.';

USE CATALOG governance;

-- ---------------------------------------------------------------------------
-- 2. Schema
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS model_health
  COMMENT 'Core schema for The Third Eye. Contains asset inventory, signal index, lineage events, risk scores, incidents, and remediation suggestions.';

USE SCHEMA model_health;

-- ---------------------------------------------------------------------------
-- 3. risk_weights_config
-- ---------------------------------------------------------------------------
-- Stores per-criticality-tier weights for the health score formula:
--   health_score = 100 - (w1*drift + w2*(1-quality) + w3*cost + w4*guardrail)
--                       * criticality_weight
-- Values here are the defaults; operators override per-model via UC tags or
-- by inserting rows for a specific (model_id, tier) combination.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.risk_weights_config (
  tier                STRING    NOT NULL  COMMENT 'Criticality tier: tier_0_experimental | tier_1_business | tier_2_operational | tier_3_low',
  w1_drift            DOUBLE    NOT NULL  COMMENT 'Weight for drift component (0–1)',
  w2_quality          DOUBLE    NOT NULL  COMMENT 'Weight for quality/accuracy component (0–1)',
  w3_cost             DOUBLE    NOT NULL  COMMENT 'Weight for cost/usage anomaly component (0–1)',
  w4_guardrail        DOUBLE    NOT NULL  COMMENT 'Weight for guardrail violation component (0–1)',
  criticality_weight  DOUBLE    NOT NULL  COMMENT 'Overall multiplier applied after weighted sum (0–2). Higher = harsher penalty for the same signal.',
  correlation_window_hours INT  NOT NULL  COMMENT 'Time window (hours) for signal correlation (Section 7.1)',
  incident_min_signals INT      NOT NULL  COMMENT 'Minimum distinct signal types required to open an incident',
  updated_at          TIMESTAMP           COMMENT 'Last config update',
  updated_by          STRING              COMMENT 'Identity that last changed this row'
)
USING DELTA
COMMENT 'Configurable risk weights per criticality tier. Drives the composite health score formula.'
TBLPROPERTIES ('third_eye.version' = '1.0', 'third_eye.component' = 'config');

-- Seed defaults (idempotent via MERGE)
MERGE INTO governance.model_health.risk_weights_config AS t
USING (
  SELECT * FROM VALUES
    ('tier_0_experimental', 0.20, 0.20, 0.10, 0.10, 0.50,  4, 1, current_timestamp(), 'system'),
    ('tier_1_business',     0.35, 0.35, 0.15, 0.15, 1.50,  2, 2, current_timestamp(), 'system'),
    ('tier_2_operational',  0.30, 0.30, 0.20, 0.20, 1.00,  2, 2, current_timestamp(), 'system'),
    ('tier_3_low',          0.25, 0.25, 0.25, 0.25, 0.75,  6, 2, current_timestamp(), 'system')
  AS s(tier, w1_drift, w2_quality, w3_cost, w4_guardrail, criticality_weight,
       correlation_window_hours, incident_min_signals, updated_at, updated_by)
) AS s ON t.tier = s.tier
WHEN NOT MATCHED THEN INSERT *;

-- ---------------------------------------------------------------------------
-- 4. criticality_tier_config
-- ---------------------------------------------------------------------------
-- Maps business domain / UC tag patterns to a default criticality tier.
-- The discovery/sync job applies these rules when a model has no explicit
-- criticality_tier tag. Operators add rows to extend the ruleset.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.criticality_tier_config (
  rule_id           STRING    NOT NULL  COMMENT 'Unique rule identifier',
  match_type        STRING    NOT NULL  COMMENT 'domain | tag_key | tag_value | model_name_prefix | serving_endpoint_prefix',
  match_value       STRING    NOT NULL  COMMENT 'Value to match against (case-insensitive prefix match)',
  assigned_tier     STRING    NOT NULL  COMMENT 'Tier assigned when match fires',
  priority          INT       NOT NULL  COMMENT 'Lower number = higher priority when multiple rules match',
  description       STRING              COMMENT 'Human-readable rationale',
  created_at        TIMESTAMP,
  created_by        STRING
)
USING DELTA
COMMENT 'Rule-based criticality tier assignment for the discovery/sync job.'
TBLPROPERTIES ('third_eye.version' = '1.0', 'third_eye.component' = 'config');

-- Seed defaults
MERGE INTO governance.model_health.criticality_tier_config AS t
USING (
  SELECT * FROM VALUES
    ('rule_fin',       'domain',            'finance',         'tier_1_business',     10, 'Finance domain → business-critical',    current_timestamp(), 'system'),
    ('rule_reg',       'domain',            'regulatory',      'tier_1_business',     10, 'Regulatory domain → business-critical',  current_timestamp(), 'system'),
    ('rule_ops',       'domain',            'operations',      'tier_2_operational',  20, 'Ops domain → operational',               current_timestamp(), 'system'),
    ('rule_risk',      'tag_key',           'pii',             'tier_1_business',      5, 'PII-tagged models are high-priority',    current_timestamp(), 'system'),
    ('rule_exp',       'model_name_prefix', 'exp_',            'tier_0_experimental', 50, 'exp_ prefix → experimental',             current_timestamp(), 'system'),
    ('rule_prod',      'serving_endpoint_prefix', 'prod-',     'tier_1_business',     15, 'prod- endpoint → business-critical',    current_timestamp(), 'system'),
    ('rule_default',   'domain',            '*',               'tier_2_operational',  99, 'Default fallback tier',                  current_timestamp(), 'system')
  AS s(rule_id, match_type, match_value, assigned_tier, priority, description, created_at, created_by)
) AS s ON t.rule_id = s.rule_id
WHEN NOT MATCHED THEN INSERT *;

-- ---------------------------------------------------------------------------
-- 5. alert_routing_config
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.alert_routing_config (
  route_id          STRING    NOT NULL,
  channel_type      STRING    NOT NULL  COMMENT 'slack | teams | google_chat | pagerduty | email',
  webhook_url       STRING              COMMENT 'Stored as a Databricks Secret reference: {{secrets/third_eye/slack_webhook}}',
  trigger_condition STRING    NOT NULL  COMMENT 'incident_opened | health_tier_changed | daily_digest',
  min_tier          STRING    NOT NULL  COMMENT 'Minimum criticality_tier to trigger this route',
  is_active         BOOLEAN   NOT NULL  DEFAULT true,
  created_at        TIMESTAMP,
  created_by        STRING
)
USING DELTA
COMMENT 'Alert routing configuration. Webhook URLs should reference Databricks Secrets, not hardcoded values.'
TBLPROPERTIES ('third_eye.version' = '1.0', 'third_eye.component' = 'config');

MERGE INTO governance.model_health.alert_routing_config AS t
USING (
  SELECT * FROM VALUES
    ('route_teams_breach',  'teams',      '{{secrets/third_eye/teams_webhook}}',  'incident_opened',     'tier_2_operational', true,  current_timestamp(), 'system'),
    ('route_slack_breach',  'slack',      '{{secrets/third_eye/slack_webhook}}',  'incident_opened',     'tier_2_operational', true,  current_timestamp(), 'system'),
    ('route_teams_digest',  'teams',      '{{secrets/third_eye/teams_webhook}}',  'daily_digest',        'tier_0_experimental',true, current_timestamp(), 'system'),
    ('route_pagerduty',     'pagerduty',  '{{secrets/third_eye/pd_api_key}}',     'incident_opened',     'tier_1_business',    true,  current_timestamp(), 'system')
  AS s(route_id, channel_type, webhook_url, trigger_condition, min_tier, is_active, created_at, created_by)
) AS s ON t.route_id = s.route_id
WHEN NOT MATCHED THEN INSERT *;

-- ---------------------------------------------------------------------------
-- Done
-- ---------------------------------------------------------------------------
SELECT 'Step 01 complete: catalog, schema, and config tables created.' AS status;
