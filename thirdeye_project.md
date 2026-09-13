# The Third Eye
### A Continuous AI Health & Governed ModelOps Control Plane for Databricks

> **Watch. Understand. Act.**
> Databricks provides the signals. The Third Eye turns them into decisions.

---

## 0. How to read this document

This spec merges two earlier drafts — a technically detailed but narrower governance-layer design, and a broader "control plane" product vision — into one buildable document, corrected against actual Databricks capabilities (verified against current docs, Sept 2026).

**The governing rule for everything below:** Third Eye never recomputes a metric Databricks already computes. It reads Databricks' own output tables, correlates across them, scores composite risk with business/criticality context, diagnoses root cause, and drives a governed action loop. Section 1 makes this explicit per-signal so nothing gets rebuilt by accident.

---

## 1. What Databricks Already Computes vs. What Third Eye Must Build

This is the most important table in the document — it is the actual scope boundary.

| Signal | Databricks-native source | What it gives you | What Third Eye does with it |
|---|---|---|---|
| **Data drift / distribution stats** | **Lakehouse Monitoring** (or its successor, Data Profiling) — creates `{output_schema}.{table}_profile_metrics` and `{output_schema}.{table}_drift_metrics` Delta tables per monitored table | Per-column summary stats, consecutive/baseline drift metrics, computed automatically on a schedule you configure | **Read** the drift/profile metrics tables. Do not reimplement PSI/KS — Databricks already computes "consecutive drift" (window vs. previous window) and baseline drift if you supply a baseline table |
| **Model quality / inference accuracy** | Lakehouse Monitoring configured with `InferenceLog` analysis type | The profile metrics table includes model accuracy metrics per model version when ground truth is joined in | **Read** accuracy metrics per model_id/version from the profile table; do not recompute accuracy yourself |
| **Data lineage (table & column level)** | **`system.access.table_lineage`** and **`system.access.column_lineage`** system tables, plus the Lineage REST API | Automatic, no-config lineage for every read/write event (jobs, notebooks, pipelines, dashboards, DBSQL queries) | **Read** these system tables directly for correlation (e.g., "which upstream table changed before this model's drift appeared"). Known limits: rolling 1-year retention on system tables (indefinite via Catalog Explorer/API since Sept 1 2024); REST API only returns one hop up/down per call — walk recursively for multi-hop graphs |
| **Inference request/response logs** | **Inference tables** (classic, per serving endpoint) or the newer **Unified Trace Table** (Unity Gateway, OpenTelemetry format, Beta) | Every request/response payload, latency, status codes, model version served | **Read**, don't duplicate. Prefer the Unified Trace Table for anything Gateway-routed (agents, LLMs, external models) since it's the recommended path going forward; use classic inference tables for traditional custom-model serving endpoints where Gateway isn't in front |
| **Cost / usage / token tracking** | **`system.serving.served_entities`** and **`system.serving.endpoint_usage`** system tables (Unity Gateway usage tracking) | Per-endpoint and per-served-entity usage/cost tracking, with a `usage_context` map for custom attribution | **Read** directly for the cost dashboard panel — no custom cost computation needed |
| **PII/PHI and unsafe-content detection** | **Unity Gateway AI Guardrails** | Detects/blocks/filters unsafe content and PII/PHI in model inputs/outputs at the gateway level | **Read** guardrail violation events as a risk signal; do not reimplement PII detection (this also removes duplicate scope from the separate dbx-guardrails project — reuse, don't rebuild) |
| **Model registry / versions / experiments** | **MLflow Model Registry + Unity Catalog model objects** | Registered models, versions, aliases, experiment/run metadata, signatures | **Read** via MLflow API / UC model objects to build the asset inventory — this is your `model_registry_map` population source, not a UI you build |
| **AI asset discovery (models, agents, tools, MCPs)** | **Unity Gateway AI Asset Registry** | Centralized catalog of governed models, agents, MCP servers, tools | **Read** as the primary zero-touch discovery source — supplements/replaces manually walking MLflow+UC for asset inventory |
| **Fairness / bias metrics** | Lakehouse Monitoring supports fairness/bias monitoring for classification models as a documented capability | Bias metrics computed on schedule if configured on the monitored table | **Read** if the customer has configured it; if not configured, Third Eye can *provision* the monitor (via API) as part of zero-touch onboarding — this is orchestration, not reimplementation |

**What is NOT natively provided anywhere in Databricks — this is the actual product:**

1. **Cross-signal correlation** — joining a Lakehouse Monitoring drift event with a lineage change event with a Gateway cost spike with a business KPI dip into one incident. No Databricks primitive does this join.
2. **Composite health scoring with criticality/business weighting** — Databricks gives independent metrics per table/endpoint; nothing scores "how much does this matter" across a fleet ranked by business tier.
3. **Confidence scoring on top of metrics** — flagging when a health score is based on incomplete evidence (no ground truth yet, low traffic volume, short baseline window).
4. **Root-cause narrative generation** — turning correlated raw signals into a plain-language explanation, optionally LLM-assisted via Foundation Model APIs.
5. **The zero-touch discovery/registration glue** — polling and diffing MLflow + UC + Gateway registries into one governed asset inventory, and auto-provisioning monitors where missing.
6. **The action/decision layer** — retraining triggers, champion/challenger comparison, promotion gating, rollback, configurable autonomy levels. Databricks gives you the primitives (Workflows, MLflow aliases, AutoML) but no orchestration logic that connects "health degraded" to "here is a vetted retrained candidate awaiting your approval."
7. **Unified multi-channel delivery** — pushing a correlated, prioritized digest to Teams/Slack/Google Chat with an AI-generated remediation suggestion. Gateway and Lakehouse Monitoring have no notification/digest layer of their own beyond basic SQL alerts.

This reframes scope honestly: **most of Section 1's left column is "wire up a reader," and the real engineering effort is concentrated in the seven items above.** That is a meaningfully smaller and more achievable V1 than either earlier draft implied.

---

## 2. Product Positioning

**Avoid:** a drift-detection tool, a better PSI calculator, another monitoring dashboard, an automated retraining script.

**Use:** a continuous AI health control plane for Databricks — the intelligence and action layer above the native stack.

**Key lines:**
- *Databricks provides the signals. The Third Eye turns them into decisions.*
- *Don't monitor 500 models. Let Third Eye tell you which 5 need attention today.*
- *From telemetry to root cause to remediation in one governed control plane.*

---

## 3. Design Principles

1. **Databricks-native, read-first.** Consume Lakehouse Monitoring, lineage system tables, Gateway usage/trace tables, MLflow, and AI/BI. Never recreate a metric Databricks already computes (see Section 1). Provisioning a missing monitor via API is orchestration, not duplication.
2. **Zero-touch by default.** Registering a model the normal way (`mlflow.register_model()` / UC model registration) should be sufficient to activate discovery, a health profile, and dashboard/Genie visibility — no separate onboarding UI for the common path.
3. **Evidence before action.** No single statistical signal should trigger an incident or a retraining action. Correlate drift + performance + lineage change + business KPI before asserting root cause; carry a confidence score alongside every health score.
4. **Risk is contextual.** A criticality tier (Tier 0 Experimental → Tier 1 Business/regulatory-critical) must weight the composite health score, alert priority, and autonomy level — a large drift on a Tier 3 model may rank below a small drift on a Tier 1 model.
5. **Human-in-the-loop for high-impact actions.** Investigation, correlation, and candidate preparation (retraining) can be automated. Production promotion for Tier 1/critical models requires explicit approval by default; autonomy is configurable per model/tier.
6. **One source of truth.** AI/BI dashboard, Genie Space, and Copilot Studio/Teams notifications all read from the same governed `model_health` schema — no divergent copies of "the answer."

---

## 4. Scope

### 4.1 V1 — Buildable slice (the actual first build)

Deliberately cut down from both earlier drafts to a complete, working vertical:

- Zero-touch discovery: sync `model_registry_map` from MLflow/UC + Unity Gateway AI Asset Registry
- Lineage-based context: pull upstream table/feature dependencies from `system.access.table_lineage` / `column_lineage`
- Read-and-normalize adapters for: Lakehouse Monitoring drift/profile tables, Gateway usage/cost tables, inference/trace tables
- Model criticality tiers (config-driven, with sensible defaults)
- Composite health score + confidence score (the one genuinely new computation)
- Basic evidence correlation: link a drift event to a lineage change within a time window, and to a Gateway guardrail violation if present
- AI/BI dashboard: fleet overview, model leaderboard, model detail drill-through
- Genie Space with metric views over the governed schema
- Alerting workflow: threshold breach → Slack/Teams notification with the correlated evidence attached
- Copilot Studio: scheduled KPI digest + breach notification, with foundation-model-generated remediation text
- Model passport (a simple auto-generated summary per model: purpose, lineage, current health, recent history) — cheap to build since it's just a formatted read of the governed schema, and it's a strong demo artifact

### 4.2 V2 — Governed autonomy (do not build first, but design V1's schema to support it)

- Auto-retraining pipeline with champion/challenger evaluation
- Deployment gates and configurable autonomy levels (1-5)
- Rollback automation
- Blast-radius analysis (which downstream models/dashboards are affected by an upstream change, using lineage graph traversal)
- Silent failure detection (metrics look green in aggregate but a slice has collapsed)
- Multi-workspace federation
- Compliance/audit report generation
- Cost-of-drift business-impact estimation
- Marketplace/Databricks App packaging

V1 and V2 share one data model, one health/scoring engine, and one set of interfaces — V2 adds action-taking on top, it does not replace anything.

### 4.3 Non-Goals

- Not replacing Unity Catalog, MLflow, Lakehouse Monitoring, Unity Gateway, or Databricks Workflows.
- Not rebuilding drift/statistical algorithms.
- Not replacing a full SIEM or enterprise ticketing system (integrate via webhook instead).
- Not auto-promoting regulated/Tier-1 models without policy-defined approval.

---

## 5. Architecture

```
┌──────────────────────────── DATABRICKS (native, unmodified) ────────────────────────────┐
│ Unity Catalog · MLflow Registry · Lakehouse Monitoring · Inference/Trace Tables          │
│ system.access.table_lineage / column_lineage · Unity Gateway (Registry, Usage, Guardrails)│
└───────────────────────────────────────┬────────────────────────────────────────────────┘
                                         │ read-only adapters
                                         ▼
                        ┌────────────────────────────────┐
                        │   DISCOVERY & SYNC JOB          │
                        │  MLflow/UC diff → registry_map  │
                        │  Gateway AI Asset Registry sync │
                        │  Lineage walk (1-hop, cached)   │
                        └───────────────┬─────────────────┘
                                         ▼
                        ┌────────────────────────────────┐
                        │   SIGNAL NORMALIZATION LAYER     │
                        │  reads: drift/profile tables,   │
                        │  usage/cost tables, guardrail   │
                        │  events, lineage change events  │
                        └───────────────┬─────────────────┘
                                         ▼
                        ┌────────────────────────────────┐
                        │   CORRELATION & SCORING ENGINE   │  ← the actual new engineering
                        │  time-window correlation         │
                        │  composite health + confidence   │
                        │  criticality weighting           │
                        └───────────────┬─────────────────┘
                                         ▼
                     governance.model_health.* (Unity Catalog, Delta)
                                         │
                 ┌───────────────────────┼────────────────────────┐
                 ▼                       ▼                        ▼
           AI/BI DASHBOARD          GENIE SPACE            COPILOT STUDIO
           (control room)         (investigation)        (Teams/Slack/GChat)
                                         │                        │
                                         ▼                        ▼
                                 DIAGNOSIS/REMEDIATION   FOUNDATION MODEL
                                 (root-cause narrative)   (ai_query remediation text)
                                         │
                                         ▼
                             [V2] ACTION ENGINE → retrain → champion/challenger → approval → promote
```

---

## 6. Unity Catalog Governance Schema

Catalog: `governance` · Schema: `model_health`

### 6.1 `model_registry_map`
Populated by the discovery/sync job from MLflow + UC + Gateway AI Asset Registry — not manually maintained.

| column | type | source |
|---|---|---|
| model_id | STRING (PK) | generated |
| model_name | STRING | MLflow/UC |
| model_version | STRING | MLflow |
| serving_endpoint | STRING | Model Serving / Gateway |
| gateway_registered | BOOLEAN | Gateway AI Asset Registry |
| owning_team | STRING | UC tag |
| business_domain | STRING | UC tag |
| criticality_tier | STRING | config, default inferred from tag/domain |
| lakehouse_monitor_configured | BOOLEAN | detected via monitor API |
| created_at | TIMESTAMP | |
| is_active | BOOLEAN | |

### 6.2 `signal_index` (normalized pointer table, not a copy of raw data)
Rather than copying Databricks' own metric tables wholesale, store pointers + latest snapshot values, so the source tables remain the system of record.

| column | type | notes |
|---|---|---|
| model_id | STRING | FK |
| signal_type | STRING | drift / accuracy / cost / usage / guardrail / lineage_change |
| source_table | STRING | fully qualified name of the native Databricks table this reads from |
| computed_at | TIMESTAMP | |
| latest_value | DOUBLE | normalized numeric snapshot for scoring |
| raw_reference | STRING | pointer (row key / query) back to the full native record |

### 6.3 `lineage_events` (derived from system.access.table_lineage/column_lineage)

| column | type | notes |
|---|---|---|
| model_id | STRING | FK, via upstream table match |
| upstream_table | STRING | |
| event_type | STRING | schema_change / new_write / etc |
| event_time | TIMESTAMP | |
| entity_type | STRING | JOB / NOTEBOOK / PIPELINE / DASHBOARD_V3 / DBSQL_QUERY |

### 6.4 `risk_scores` (the central composite table — Third Eye's core computation)

| column | type | notes |
|---|---|---|
| model_id | STRING | FK |
| computed_at | TIMESTAMP | |
| drift_component | DOUBLE | normalized from `signal_index` drift rows |
| quality_component | DOUBLE | normalized from Lakehouse Monitoring accuracy metrics |
| cost_component | DOUBLE | normalized from Gateway usage tables |
| guardrail_component | DOUBLE | normalized from Gateway guardrail violation counts |
| criticality_weight | DOUBLE | from `model_registry_map.criticality_tier` |
| health_score | DOUBLE | 0-100, weighted composite |
| confidence | DOUBLE | 0-1, based on evidence volume/completeness (see 7.2) |
| health_tier | STRING | healthy / watch / at_risk / critical |

### 6.5 `incidents` (correlated evidence bundle — the output of the correlation engine)

| column | type | notes |
|---|---|---|
| incident_id | STRING (PK) | |
| model_id | STRING | FK |
| opened_at | TIMESTAMP | |
| trigger_signals | STRING | JSON array of `signal_index` rows that co-occurred within the correlation window |
| lineage_context | STRING | JSON, linked `lineage_events` rows if any fell in-window |
| root_cause_narrative | STRING | LLM-generated explanation (Foundation Model API) |
| root_cause_confidence | DOUBLE | |
| recommended_action | STRING | investigate / no_action / remediate |
| status | STRING | open / acknowledged / resolved |

### 6.6 `remediation_suggestions`

| column | type | notes |
|---|---|---|
| incident_id | STRING | FK |
| generated_at | TIMESTAMP | |
| explanation | STRING | plain-language, from foundation model |
| suggested_actions | STRING | JSON array, ranked by effort |
| urgency | STRING | urgent / can_wait |

---

## 7. Correlation & Scoring Engine (the genuinely new part)

### 7.1 Correlation logic
A scheduled Databricks Workflow task runs after each Lakehouse Monitoring refresh cycle:
1. Pull new rows from `signal_index` since last run.
2. For each model with a new drift/quality/cost/guardrail signal crossing its configured threshold, look up `lineage_events` for that model's upstream tables within a configurable time window (e.g., ±2 hours) before the signal.
3. If a qualifying combination exists (e.g., drift signal + upstream lineage event within window, or drift + quality decline + guardrail violation), open a row in `incidents` with all contributing signals attached as evidence — this is the "evidence fusion" step, and it's a straightforward time-window join, not a new statistical method.
4. If only one weak signal fired with no corroborating evidence, do not open an incident — this is the "do nothing" path, and it's essential for avoiding alert fatigue.

### 7.2 Health score & confidence formula
```
health_score = 100 - (
    w1 * drift_component +
    w2 * (1 - quality_component) +
    w3 * cost_component +
    w4 * guardrail_component
) * criticality_weight

confidence = f(evidence_volume, baseline_window_completeness, ground_truth_availability)
```
Default weights configurable per criticality tier in a `risk_weights_config` table — mirrors the config-driven pattern already used in the dbx-guardrails project.

### 7.3 Root-cause narrative generation
Once an incident is opened, a Databricks Workflow task calls a Foundation Model endpoint via `ai_query()` with the incident's evidence bundle (drift values, lineage change description, guardrail events, timing) and asks for: (1) plain-language explanation, (2) ranked remediation actions, (3) urgency assessment. Written to `remediation_suggestions`. This keeps the LLM call inside Databricks, with Copilot Studio/dashboard as pure readers — avoids duplicating model access logic in the notification layer.

---

## 8. AI/BI Dashboard

- **Fleet Overview:** KPI tiles (% healthy/watch/at_risk/critical, average health_score trend, open incident count), risk-tier distribution over time.
- **Model Leaderboard:** sortable by health_score/criticality, filterable by business_domain/team, drift trend sparkline, last-retrain date, gateway cost trend.
- **Model Detail (drill-through):** time-series of health_score/component breakdown, feature-level drift chart (sourced straight from the native Lakehouse Monitoring drift table), lineage graph snippet for that model's upstream tables, open/past incidents with root-cause narrative, cost/usage trend from Gateway tables.
- Built directly on `governance.model_health.*` — shares one source of truth with Genie and Copilot Studio.

---

## 9. Genie Space

- Metric views over `risk_scores`, `incidents`, `signal_index` so Genie resolves "risky," "drifting," "stale," "costly" to specific columns/thresholds without raw joins per question.
- Instruction doc (same pattern as the Meijer Nexus Analytics doc): vocabulary mapping, business_domain routing, and a two-step drill-down pattern — "which models are at risk" → ranked list; "why is X at risk" → pulls the linked `incidents.trigger_signals` and `root_cause_narrative`.
- Genie should also be able to answer cost/usage questions directly against Gateway-sourced `signal_index` rows, not just health questions — one Genie Space, two investigation modes.
- Daily digest query: "list all models that changed health_tier in the last 24 hours" → feeds the Copilot Studio scheduled digest.

---

## 10. Copilot Studio Integration

**Two message types**, both reading from the governed schema rather than computing anything themselves:

**A. Scheduled KPI digest** — Copilot Studio topic on a schedule pulls fleet health %, top at-risk models, and week-over-week health_score trend from `risk_scores`/`model_registry_map`, posts to Teams natively and fans out to Slack/Google Chat via webhook actions in the same topic.

**B. Breach notification with remediation** — triggered by the same `incidents` creation event (Section 7). Copilot Studio reads the linked `remediation_suggestions` row and formats it into an adaptive card: model name, health_tier, what changed, foundation-model explanation, ranked remediation actions, urgency.

This keeps Copilot Studio as a thin multi-channel delivery layer — the "brains" (correlation + LLM reasoning) stay inside Databricks where the data and Foundation Model already live.

---

## 11. Zero-Touch Discovery — Concrete Flow

1. User registers a model normally: `mlflow.register_model()` or via UC model registration UI/API. No separate step.
2. Scheduled discovery job (hourly) diffs MLflow/UC model objects **and** Unity Gateway's AI Asset Registry against `model_registry_map`; inserts new rows for anything not yet tracked.
3. For each new model, the job queries `system.access.table_lineage`/`column_lineage` (or the Lineage REST API for a quick one-hop check) to populate upstream dependencies into `lineage_events`, and checks whether a Lakehouse Monitor is already configured on its inference/serving table.
4. If no monitor exists, the job can optionally **provision one via the Lakehouse Monitoring API** (`w.lakehouse_monitors.create(...)`) using sensible defaults — this is orchestrating Databricks' own tool, not reimplementing it.
5. The new `model_registry_map` row is the only thing that "activates" dashboard inclusion, Genie scope, and monitoring — because both the dashboard and Genie's metric views are parameterized off this table's contents, not hardcoded per model.

**Known limits to design around:**
- Lineage is not preserved across renames of catalogs/schemas/tables/columns, and no lineage exists before Sept 1, 2024.
- Lineage system tables retain a rolling 1-year window (Catalog Explorer/API retain indefinitely since Sept 2024) — snapshot anything needed for long-term trend charts into your own tables.
- The Lineage REST API returns only one hop up/down per call — walk recursively for multi-hop graphs, or query the system tables directly for a full join.
- Trace/inference table delivery is best-effort — expect up to ~1 hour of delay for Gateway-routed traffic, and inference tables are not guaranteed for 401/403/429/500 responses.

---

## 12. Enterprise Differentiators (V2, but design for them now)

1. **Auto-retraining pipeline** — sustained critical health_tier triggers a retraining job against the same lineage-derived source tables; registers a challenger model version tagged `auto_retrain_candidate=true`; never auto-promotes for Tier 1/critical models; runs champion/challenger comparison on a holdout window and surfaces it in the dashboard before any promotion decision.
2. **Blast-radius analysis** — using the lineage graph, determine which other models/dashboards depend on an upstream table that just changed, before those models show symptoms — this is a lineage-traversal feature, buildable once the 1-hop lineage walk in Section 11 is generalized to multi-hop.
3. **Silent failure detection** — flag when aggregate health looks fine but a specific slice (fairness monitoring's population subgroup) has collapsed.
4. **Multi-workspace federation** — aggregate `risk_scores` across workspaces with `workspace_id` tagging and UC row-level security so each business unit sees only its own models.
5. **Compliance reporting & model passport** — auto-generated periodic exports summarizing model health, fairness audit history, and remediation actions — populated entirely from the governance schema, no manual compilation.
6. **Cost-of-drift estimation** — translate health_score degradation into an estimated dollar exposure, configurable per model, for leadership framing.
7. **Marketplace/Databricks App packaging** — installable app with a setup wizard (point at a workspace, pick catalogs/schemas to monitor), usage-based pricing mirroring Databricks' own consumption model.

---

## 13. Build Plan (order for Claude Code)

1. **Scaffolding:** UC catalog/schema creation SQL for `governance.model_health.*`; config tables for risk weights and criticality defaults.
2. **Discovery/sync job:** MLflow + UC + Gateway AI Asset Registry diff → `model_registry_map`. Build against a synthetic/mock registry first if no live Gateway access during development.
3. **Signal adapters:** read-only jobs that normalize Lakehouse Monitoring drift/profile tables, Gateway usage/cost tables, and lineage system tables into `signal_index` and `lineage_events`. This is the largest "glue" surface — budget the most time here.
4. **Correlation & scoring engine:** the time-window join logic (Section 7.1) and the health/confidence formula (Section 7.2) — this is the one truly novel computation and deserves the most testing.
5. **Root-cause narrative job:** `ai_query()` call against a Foundation Model endpoint, writing to `remediation_suggestions`.
6. **AI/BI Dashboard:** three pages against the governed schema.
7. **Genie Space:** metric views + instruction doc.
8. **Alerting + Copilot Studio:** threshold breach → Slack/Teams native alert, plus Copilot Studio topics for scheduled digest and breach notification.
9. **Model passport generator:** a simple formatted read of the governed schema per model — cheap, high demo value.
10. **README + architecture diagram + demo script:** inject a synthetic drift/lineage-change scenario end-to-end and show discovery → correlation → dashboard update → Genie investigation → Teams alert with remediation.

Do not start V2 (auto-retraining, blast radius, multi-workspace) until the V1 slice above runs end-to-end on synthetic data.

---

## 14. Repo Structure

```
third-eye/
├── README.md
├── thirdeye_project.md            (this file)
├── sql/
│   ├── 01_create_catalog_schema.sql
│   ├── 02_create_tables.sql
│   └── 03_create_metric_views.sql
├── config/
│   ├── risk_weights.yaml
│   └── criticality_defaults.yaml
├── discovery/
│   ├── sync_model_registry.py       (MLflow + UC + Gateway registry diff)
│   └── lineage_walker.py            (system.access.table_lineage / column_lineage reader)
├── adapters/
│   ├── read_lakehouse_monitoring.py (drift/profile table normalizer)
│   ├── read_gateway_usage.py        (system.serving.* + unified trace table reader)
│   └── read_guardrail_events.py
├── scoring/
│   ├── correlate_signals.py         (Section 7.1)
│   └── compute_health_score.py      (Section 7.2)
├── remediation/
│   └── generate_root_cause.py       (ai_query() call)
├── genie/
│   └── instruction_doc.md
├── dashboard/
│   └── dashboard_spec.json
├── copilot_studio/
│   └── topic_flow_notes.md
├── passport/
│   └── generate_model_passport.py
├── v2_stretch/
│   ├── auto_retrain/
│   ├── blast_radius/
│   └── multi_workspace/
└── workflows/
    └── third_eye_pipeline.yml
```

---

## 15. Tech Stack

- Unity Catalog, Delta Lake
- Lakehouse Monitoring / Data Profiling (native drift & quality — read-only integration)
- `system.access.table_lineage`, `system.access.column_lineage` (native lineage)
- Unity Gateway: AI Asset Registry, Unified Trace Table, `system.serving.*` usage tables, AI Guardrails
- MLflow Model Registry
- Databricks Workflows (orchestration)
- Databricks Foundation Model APIs (`ai_query()`) for root-cause narrative + remediation text
- Databricks AI/BI (Lakeview) dashboards
- Genie Space + metric views
- Microsoft Copilot Studio (Teams-native, fanned out to Slack/Google Chat)
- PySpark / Databricks SQL for adapters and scoring jobs

---

## 16. Why "The Third Eye"

Traditional monitoring says: *something crossed a threshold.*
The Third Eye says: *something changed, this is why, this is what it affects, this is how important it is, this is what you should do next.*

```
AI Fleet → Signals (Databricks-native) → Third Eye (correlate, score, diagnose) → Decision → Action → Verify → Watch Again
```
