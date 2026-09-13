-- =============================================================================
-- The Third Eye — Step 2: Core Delta Tables
-- =============================================================================
-- Catalog  : governance
-- Schema   : model_health
-- Tables   : model_registry_map, signal_index, lineage_events, risk_scores,
--            incidents, remediation_suggestions
--
-- IMPORTANT: These tables are the governed "source of truth" for the dashboard,
--            Genie Space, and Copilot Studio. They do NOT copy raw Databricks
--            metric data — they store pointers and normalized snapshots that
--            reference the native Databricks output tables.
--
-- Run after: 01_create_catalog_schema.sql
-- =============================================================================

USE CATALOG governance;
USE SCHEMA  model_health;

-- ---------------------------------------------------------------------------
-- 1. model_registry_map
-- ---------------------------------------------------------------------------
-- Asset inventory. Populated by discovery/sync_model_registry.py.
-- Not manually maintained — the sync job diffs MLflow + UC + Gateway against
-- this table and inserts/updates automatically.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.model_registry_map (
  model_id                     STRING    NOT NULL  COMMENT 'Synthetic PK: generated as SHA-256 of (model_name + version)',
  model_name                   STRING    NOT NULL  COMMENT 'Fully qualified UC model name: catalog.schema.model',
  model_version                STRING    NOT NULL  COMMENT 'MLflow / UC version string',
  serving_endpoint             STRING              COMMENT 'Associated Model Serving or Unity Gateway endpoint name',
  gateway_registered           BOOLEAN             COMMENT 'True if this model is registered in Unity Gateway AI Asset Registry',
  asset_type                   STRING              COMMENT 'model | agent | tool | mcp_server (from Gateway AI Asset Registry)',
  owning_team                  STRING              COMMENT 'Team tag from UC; populated by sync job',
  business_domain              STRING              COMMENT 'Domain tag from UC (finance, operations, etc.)',
  criticality_tier             STRING    NOT NULL  COMMENT 'tier_0_experimental|tier_1_business|tier_2_operational|tier_3_low',
  lakehouse_monitor_configured BOOLEAN   NOT NULL  DEFAULT false COMMENT 'True if a Lakehouse Monitor is configured on the serving/inference table',
  lakehouse_monitor_schema     STRING              COMMENT 'Output schema of the associated Lakehouse Monitor (where drift/profile tables are written)',
  inference_table              STRING              COMMENT 'Inference/serving table or Unified Trace Table name (fully qualified)',
  mlflow_run_id                STRING              COMMENT 'Associated MLflow run ID',
  mlflow_experiment_id         STRING              COMMENT 'Associated MLflow experiment ID',
  uc_model_uri                 STRING              COMMENT 'Unity Catalog model URI for this version',
  created_at                   TIMESTAMP NOT NULL,
  updated_at                   TIMESTAMP NOT NULL,
  discovered_by                STRING    NOT NULL  COMMENT 'sync_job | manual | gateway_auto',
  is_active                    BOOLEAN   NOT NULL  DEFAULT true,
  deactivated_at               TIMESTAMP           COMMENT 'Set when model is removed from MLflow/UC/Gateway',
  -- V2 columns (nullable, populated when V2 is enabled)
  autonomy_level               INT                 COMMENT '[V2] 1=notify_only 2=auto_retrain_candidate 3=auto_champion 4=auto_promote_non_tier1 5=fully_autonomous',
  retrain_trigger_threshold    DOUBLE              COMMENT '[V2] health_score threshold below which retraining is auto-triggered',
  last_retrained_at            TIMESTAMP           COMMENT '[V2] Last successful retraining timestamp'
)
USING DELTA
PARTITIONED BY (business_domain)
COMMENT 'Governed asset inventory for all AI models, agents, tools, and MCP servers discovered from MLflow, Unity Catalog, and Unity Gateway AI Asset Registry.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'discovery',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 2. signal_index
-- ---------------------------------------------------------------------------
-- Normalized pointer table. NOT a copy of Databricks' native metric tables.
-- Stores the latest snapshot value + a pointer back to the native record
-- so the source tables remain the system of record (Section 6.2).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.signal_index (
  signal_id       STRING    NOT NULL  COMMENT 'Generated UUID per signal observation',
  model_id        STRING    NOT NULL  COMMENT 'FK → model_registry_map.model_id',
  signal_type     STRING    NOT NULL  COMMENT 'drift | accuracy | cost | usage | guardrail | lineage_change | latency | token_usage',
  signal_subtype  STRING              COMMENT 'For drift: consecutive | baseline. For guardrail: pii | unsafe_content | etc.',
  source_table    STRING    NOT NULL  COMMENT 'Fully qualified name of the native Databricks table this snapshot was read from',
  computed_at     TIMESTAMP NOT NULL  COMMENT 'Timestamp of the native Databricks computation (from source table, not ingestion time)',
  ingested_at     TIMESTAMP NOT NULL  DEFAULT current_timestamp() COMMENT 'When Third Eye read this value',
  latest_value    DOUBLE    NOT NULL  COMMENT 'Normalized numeric snapshot for scoring (0–1 scale; higher = worse)',
  raw_value       DOUBLE              COMMENT 'Actual raw value from the source table (before normalization)',
  raw_unit        STRING              COMMENT 'Unit of raw_value (e.g., psi, accuracy_score, dollars, count)',
  raw_reference   STRING              COMMENT 'Pointer back to the full native record: JSON of row key / filter query',
  column_name     STRING              COMMENT 'For drift signals: which column drifted',
  window_start    TIMESTAMP           COMMENT 'Start of the window this signal covers',
  window_end      TIMESTAMP           COMMENT 'End of the window this signal covers',
  threshold_value DOUBLE              COMMENT 'Configured alert threshold for this signal type (from risk_weights_config)',
  threshold_breached BOOLEAN NOT NULL DEFAULT false COMMENT 'True if latest_value > threshold_value',
  -- Metadata
  adapter_version STRING              COMMENT 'Version of the adapter that wrote this row'
)
USING DELTA
PARTITIONED BY (signal_type, date(computed_at))
COMMENT 'Normalized signal index. Pointer table referencing native Databricks metric tables — not a data copy. Source of truth for health scoring and correlation.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'signal_normalization',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 3. lineage_events
-- ---------------------------------------------------------------------------
-- Derived from system.access.table_lineage and column_lineage.
-- Populated by discovery/lineage_walker.py.
-- Correlated against signal_index by the scoring engine (Section 7.1).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.lineage_events (
  event_id         STRING    NOT NULL  COMMENT 'Generated UUID',
  model_id         STRING    NOT NULL  COMMENT 'FK → model_registry_map (matched via upstream table dependency)',
  upstream_table   STRING    NOT NULL  COMMENT 'Fully qualified upstream table name',
  event_type       STRING    NOT NULL  COMMENT 'schema_change | new_write | large_write | row_count_anomaly | column_added | column_dropped | partition_change',
  event_time       TIMESTAMP NOT NULL  COMMENT 'From system.access.table_lineage.event_time',
  entity_type      STRING              COMMENT 'JOB | NOTEBOOK | PIPELINE | DASHBOARD_V3 | DBSQL_QUERY (from lineage system table)',
  entity_id        STRING              COMMENT 'Job/notebook/pipeline ID that caused the write',
  entity_name      STRING              COMMENT 'Human-readable entity name',
  workspace_id     STRING              COMMENT 'Databricks workspace ID (for multi-workspace V2)',
  hop_distance     INT       NOT NULL  DEFAULT 1 COMMENT 'Lineage graph distance: 1 = direct dependency, 2+ = transitive',
  row_count_before BIGINT              COMMENT 'Upstream table row count at previous write (snapshotted)',
  row_count_after  BIGINT              COMMENT 'Upstream table row count at this write',
  ingested_at      TIMESTAMP NOT NULL  DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(event_time))
COMMENT 'Upstream lineage change events derived from system.access.table_lineage. Used to correlate data changes with model health degradation.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'lineage',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 4. risk_scores
-- ---------------------------------------------------------------------------
-- THE core computation output. This is what Third Eye produces that Databricks
-- does not: a composite, criticality-weighted, confidence-tagged health score
-- across the fleet (Section 6.4, Section 7.2).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.risk_scores (
  score_id              STRING    NOT NULL  COMMENT 'UUID per computation run',
  model_id              STRING    NOT NULL  COMMENT 'FK → model_registry_map.model_id',
  computed_at           TIMESTAMP NOT NULL,
  -- Component scores (all 0–1, higher = worse/more concerning)
  drift_component       DOUBLE              COMMENT 'Normalized drift signal (from signal_index)',
  quality_component     DOUBLE              COMMENT 'Normalized quality/accuracy (1.0 = perfect; stored inverted in formula)',
  cost_component        DOUBLE              COMMENT 'Normalized cost anomaly (from Gateway usage tables)',
  guardrail_component   DOUBLE              COMMENT 'Normalized guardrail violation rate',
  latency_component     DOUBLE              COMMENT 'Normalized latency anomaly',
  -- Weights applied (snapshot of risk_weights_config at compute time)
  w1_drift              DOUBLE,
  w2_quality            DOUBLE,
  w3_cost               DOUBLE,
  w4_guardrail          DOUBLE,
  criticality_weight    DOUBLE,
  -- Output
  raw_weighted_sum      DOUBLE              COMMENT 'Pre-scaled weighted sum before criticality_weight',
  health_score          DOUBLE    NOT NULL  COMMENT '0–100. Higher = healthier. Formula: 100 - raw_weighted_sum * criticality_weight',
  health_tier           STRING    NOT NULL  COMMENT 'healthy (80–100) | watch (60–79) | at_risk (40–59) | critical (0–39)',
  -- Confidence scoring
  confidence            DOUBLE    NOT NULL  COMMENT '0–1. How much to trust this health_score given evidence completeness.',
  confidence_factors    STRING              COMMENT 'JSON: {ground_truth_available, baseline_window_days, traffic_volume, signal_count}',
  -- Evidence metadata
  signal_count          INT                 COMMENT 'Number of signal_index rows contributing to this score',
  signals_used          STRING              COMMENT 'JSON array of signal_ids used',
  -- Trend
  prev_health_score     DOUBLE              COMMENT 'Previous period health_score for trend computation',
  prev_health_tier      STRING              COMMENT 'Previous period health_tier',
  tier_changed          BOOLEAN NOT NULL    DEFAULT false COMMENT 'True if health_tier changed from previous period'
)
USING DELTA
PARTITIONED BY (health_tier, date(computed_at))
COMMENT 'Composite health scores per model. The core Third Eye computation — not produced by any Databricks native system.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'scoring',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 5. incidents
-- ---------------------------------------------------------------------------
-- Output of the correlation engine (Section 7.1). An incident is opened only
-- when a qualifying combination of signals co-occur within the correlation
-- window — single weak signals are suppressed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.incidents (
  incident_id           STRING    NOT NULL  COMMENT 'UUID PK',
  model_id              STRING    NOT NULL  COMMENT 'FK → model_registry_map.model_id',
  opened_at             TIMESTAMP NOT NULL,
  closed_at             TIMESTAMP           COMMENT 'Set when status moves to resolved',
  -- Correlated evidence
  trigger_signals       STRING    NOT NULL  COMMENT 'JSON array of signal_index.signal_id values that co-occurred within the correlation window',
  trigger_signal_types  STRING              COMMENT 'Comma-separated list of distinct signal_types for quick filtering',
  lineage_context       STRING              COMMENT 'JSON array of lineage_events.event_id values within the correlation window (may be null)',
  correlation_window_hours INT              COMMENT 'Window used for this incident (from risk_weights_config at open time)',
  -- Root cause (LLM-generated, Section 7.3)
  root_cause_narrative  STRING              COMMENT 'LLM-generated plain-language explanation of the incident',
  root_cause_confidence DOUBLE              COMMENT '0–1. LLM self-reported confidence in the narrative.',
  root_cause_model      STRING              COMMENT 'Foundation model endpoint used to generate the narrative',
  -- Classification
  recommended_action    STRING    NOT NULL  COMMENT 'investigate | remediate | no_action | escalate',
  severity              STRING    NOT NULL  COMMENT 'low | medium | high | critical (derived from health_tier + criticality_tier)',
  status                STRING    NOT NULL  DEFAULT 'open' COMMENT 'open | acknowledged | in_progress | resolved | false_positive',
  -- Lifecycle
  acknowledged_by       STRING,
  acknowledged_at       TIMESTAMP,
  resolved_by           STRING,
  resolved_at           TIMESTAMP,
  resolution_notes      STRING,
  -- V2 action tracking
  auto_action_taken     STRING              COMMENT '[V2] retrain_triggered | rollback_triggered | champion_promoted | none',
  auto_action_at        TIMESTAMP           COMMENT '[V2] When the automated action was taken',
  approval_required     BOOLEAN             COMMENT '[V2] True if the recommended action needs human approval per autonomy_level',
  approved_by           STRING              COMMENT '[V2] Who approved the action',
  approved_at           TIMESTAMP
)
USING DELTA
PARTITIONED BY (status, date(opened_at))
COMMENT 'Correlated incident bundles. Opened only when multiple signal types co-occur within the correlation window. Source for breach notifications and Genie investigation.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'correlation',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 6. remediation_suggestions
-- ---------------------------------------------------------------------------
-- Written by remediation/generate_root_cause.py via ai_query().
-- Read by the dashboard and Copilot Studio — they never call the LLM directly.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.remediation_suggestions (
  suggestion_id    STRING    NOT NULL  COMMENT 'UUID PK',
  incident_id      STRING    NOT NULL  COMMENT 'FK → incidents.incident_id',
  model_id         STRING    NOT NULL  COMMENT 'FK → model_registry_map.model_id (denormalized for query convenience)',
  generated_at     TIMESTAMP NOT NULL,
  -- LLM outputs
  explanation      STRING    NOT NULL  COMMENT 'Plain-language explanation of what happened and why',
  suggested_actions STRING   NOT NULL  COMMENT 'JSON array of {rank, action, effort, impact, command} objects',
  urgency          STRING    NOT NULL  COMMENT 'urgent | can_wait | monitor_only',
  -- Evidence context sent to LLM (stored for auditability)
  prompt_context   STRING              COMMENT 'JSON of the evidence bundle sent to the foundation model',
  model_endpoint   STRING    NOT NULL  COMMENT 'Foundation model endpoint used (e.g., databricks-meta-llama-3-3-70b-instruct)',
  token_count      INT                 COMMENT 'Total tokens consumed by this call',
  -- V2 enrichment
  blast_radius_summary STRING          COMMENT '[V2] Which downstream models/dashboards are affected',
  cost_impact_estimate DOUBLE          COMMENT '[V2] Estimated dollar cost of continued drift'
)
USING DELTA
COMMENT 'LLM-generated remediation suggestions keyed to incidents. Dashboard and Copilot Studio read from here — no LLM calls in the presentation layer.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'remediation',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- 7. model_passport (V1 snapshot, V2 versioned history)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.model_passport (
  passport_id      STRING    NOT NULL  COMMENT 'UUID PK',
  model_id         STRING    NOT NULL  COMMENT 'FK → model_registry_map.model_id',
  generated_at     TIMESTAMP NOT NULL,
  -- Passport content (all derived from the governed schema — no recomputation)
  model_name       STRING    NOT NULL,
  model_version    STRING    NOT NULL,
  owning_team      STRING,
  business_domain  STRING,
  criticality_tier STRING    NOT NULL,
  current_health_score DOUBLE,
  current_health_tier  STRING,
  confidence       DOUBLE,
  upstream_tables  STRING              COMMENT 'JSON array of upstream table names (from lineage_events)',
  open_incidents   INT                 COMMENT 'Count of currently open incidents',
  last_incident_at TIMESTAMP,
  last_retrained_at TIMESTAMP,
  health_30d_trend STRING              COMMENT 'JSON array of {date, health_score} for last 30 days',
  -- Formatted output
  passport_markdown STRING             COMMENT 'Full passport as Markdown text',
  passport_html    STRING              COMMENT 'Full passport as HTML (for notebook rendering)'
)
USING DELTA
COMMENT 'Auto-generated per-model health passport. Derived from the governed schema — no raw data access.'
TBLPROPERTIES (
  'third_eye.version'   = '1.0',
  'third_eye.component' = 'passport'
);

-- ---------------------------------------------------------------------------
-- 8. blast_radius_analysis (V2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.blast_radius_analysis (
  analysis_id         STRING    NOT NULL  COMMENT 'UUID PK',
  upstream_table      STRING    NOT NULL  COMMENT 'The upstream table that changed',
  event_time          TIMESTAMP NOT NULL  COMMENT 'When the upstream change was detected',
  affected_model_ids  STRING    NOT NULL  COMMENT 'JSON array of model_ids that depend on this upstream table (multi-hop)',
  affected_dashboards STRING              COMMENT 'JSON array of dashboard IDs reading from affected models',
  hop_depths          STRING              COMMENT 'JSON map of model_id → hop_distance from upstream table',
  computed_at         TIMESTAMP NOT NULL,
  workspace_id        STRING              COMMENT '[V2 multi-workspace] Source workspace'
)
USING DELTA
COMMENT '[V2] Blast-radius analysis: which models and dashboards are downstream of an upstream table change.'
TBLPROPERTIES (
  'third_eye.version'   = '2.0',
  'third_eye.component' = 'blast_radius'
);

-- ---------------------------------------------------------------------------
-- 9. retrain_candidates (V2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.model_health.retrain_candidates (
  candidate_id         STRING    NOT NULL  COMMENT 'UUID PK',
  model_id             STRING    NOT NULL  COMMENT 'FK → model_registry_map',
  incident_id          STRING              COMMENT 'FK → incidents (incident that triggered retraining)',
  triggered_at         TIMESTAMP NOT NULL,
  trigger_reason       STRING    NOT NULL  COMMENT 'health_score_below_threshold | manual | scheduled',
  -- Champion/challenger
  champion_model_version STRING  NOT NULL  COMMENT 'Current production version',
  challenger_model_version STRING          COMMENT 'New retrained version (MLflow version ID)',
  challenger_run_id    STRING              COMMENT 'MLflow run ID of the retrained model',
  challenger_health_score DOUBLE           COMMENT 'Health score of challenger on holdout window',
  champion_health_score DOUBLE             COMMENT 'Health score of champion on holdout window',
  comparison_window_start TIMESTAMP,
  comparison_window_end   TIMESTAMP,
  -- Approval workflow
  status               STRING    NOT NULL  DEFAULT 'pending' COMMENT 'pending | training | evaluation | awaiting_approval | approved | promoted | rejected | failed',
  promotion_approved_by STRING,
  promotion_approved_at TIMESTAMP,
  promoted_at          TIMESTAMP,
  rejection_reason     STRING
)
USING DELTA
COMMENT '[V2] Champion/challenger retrain candidates. Populated by auto_retrain pipeline. Promotion requires approval for Tier 1/critical models.'
TBLPROPERTIES (
  'third_eye.version'   = '2.0',
  'third_eye.component' = 'auto_retrain'
);

-- ---------------------------------------------------------------------------
-- Done
-- ---------------------------------------------------------------------------
SELECT 'Step 02 complete: 9 core Delta tables created in governance.model_health.' AS status;
