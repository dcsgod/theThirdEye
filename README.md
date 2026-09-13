# The Third Eye

AI Model Health & Governance Control Plane for Databricks

Watch. Understand. Act.

Databricks provides the signals. The Third Eye turns them into decisions.

<img width="1672" height="941" alt="ChatGPT Image Sep 13, 2026, 08_43_04 PM" src="https://github.com/user-attachments/assets/255a6eb7-6836-4570-9f84-2633b03916ed" />

---

## Table of contents

- [Overview](#overview)
- [Key Features](#key-features)
- [V1 Scope (what this repo builds)](#v1-scope-what-this-repo-builds)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Quickstart (developer flow)](#quickstart-developer-flow)
- [Configuration](#configuration)
- [Development & testing](#development--testing)
- [Deploy & run (Databricks)](#deploy--run-databricks)
- [Integrations](#integrations)
- [Roadmap & V2](#roadmap--v2)
- [Contributing](#contributing)
- [License & credits](#license--credits)

---

## Overview

The Third Eye is a Databricks-native control plane that continuously monitors an organization's AI fleet, correlates signals from Databricks primitives (Lakehouse Monitoring, lineage tables, Unity Gateway, MLflow, etc.), scores composite risk, generates human-friendly root-cause narratives, and surfaces prioritized remediation suggestions to the right people and tools (dashboards, Genie, Copilot Studio / Teams / Slack).

The design principle is read-first and zero-touch: Third Eye never recomputes a Databricks-native metric; it reads the system tables and adds cross-signal correlation, composite scoring, confidence, and an action layer.

This README summarizes the V1 build (the implementable slice), architecture, and developer steps to run the project on Databricks.

---

## Key features

- Zero-touch discovery of models and AI assets (MLflow, Unity Catalog, Unity Gateway)
- Read-and-normalize adapters for:
  - Lakehouse Monitoring (drift/profile/quality)
  - Inference trace tables / Unified Trace
  - Unity Gateway usage and guardrail events
  - Lineage system tables (`system.access.table_lineage` / `column_lineage`)
- Signal normalization (`signal_index`) referencing native tables (no duplication of source-of-record)
- Correlation engine that opens evidence-backed incidents
- Composite risk scoring + confidence metrics (health_score, health_tier)
- Root-cause narrative generation using Databricks Foundation Model APIs (`ai_query()`)
- Dashboard-ready governed schema (`governance.model_health.*`) for AI/BI, Genie, and Copilot Studio
- Alerting workflow → Teams/Slack + Copilot Studio breach digest and remediation messages
- Model passport auto-generation for governance and audit

---

## V1 Scope (what this repo builds)

The repo focuses on a realistic, testable V1:

- discovery/sync jobs to populate `model_registry_map`
- lineage ingestion (one-hop cached walk) → `lineage_events`
- adapters to normalize signals into `signal_index`
- correlation & scoring engine (time-window joins → `incidents`, `risk_scores`)
- root-cause narrative job using `ai_query()` → `remediation_suggestions`
- simple AI/BI dashboard spec, Genie metric views, and Copilot Studio topic flow
- model passport generator

Refer to `thirdeye_project.md` for the full product spec and reasoning (this README is an actionable summary).
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 08_47_53 PM" src="https://github.com/user-attachments/assets/f2f9fae5-a41e-4e3b-8583-7cc63f1d43c7" />
<img width="2752" height="1536" alt="Gemini_Generated_Image_7ftvrm7ftvrm7ftv" src="https://github.com/user-attachments/assets/ed0aea25-86fa-4918-b21f-56fb9518e507" />

---

## Architecture

High level:

- Databricks native inputs (source of truth)
  - Unity Catalog, MLflow, Lakehouse Monitoring, inference/trace tables, system.access lineage, Unity Gateway tables & guardrails
- Read-only adapters → normalize signals into `signal_index` (pointers + snapshots)
- Discovery/sync → `model_registry_map`
- Correlation & scoring engine → `risk_scores` + `incidents`
- Root cause generation → `remediation_suggestions` (Foundation Model)
- Output: `governance.model_health.*` (Delta tables / UC) consumed by:
  - AI/BI dashboard (control room)
  - Genie Space metric views
  - Copilot Studio (scheduled digest & incident alerts)
- V2: action engine (auto-retrain, promote/rollback) — designed for but not implemented in V1

A compact ASCII diagram lives in `thirdeye_project.md` and the repo's docs.

---

## Repository layout

Refer to the top-level structure in `thirdeye_project.md`. Key folders:

- `sql/` — catalog/schema/table/view creation scripts (run on Databricks SQL)
  - `01_create_catalog_schema.sql`
  - `02_create_tables.sql`
  - `03_create_metric_views.sql`
- `discovery/` — discovery & registry sync jobs
  - `sync_model_registry.py`, `lineage_walker.py`
- `adapters/` — normalization adapters
  - `read_lakehouse_monitoring.py`, `read_gateway_usage.py`, `read_guardrail_events.py`
- `scoring/` — correlation & scoring
  - `correlate_signals.py`, `compute_health_score.py`
- `remediation/` — ai_query wrappers & narrative generation
  - `generate_root_cause.py`
- `dashboard/`, `genie/`, `copilot_studio/` — UI specs and integration notes
- `passport/` — model passport generator
- `workflows/` — Databricks Workflows pipeline spec(s)

---

## Quickstart (developer flow)

This project expects a Databricks workspace to run on. The steps below assume you have workspace access, sufficient privileges to create Unity Catalog objects, and a working Python environment for development.

1. Clone the repo
   - git clone git@github.com:dcsgod/theThirdEye.git
2. Prepare Unity Catalog catalog/schema (run SQL in Databricks)
   - Open Databricks SQL and run `sql/01_create_catalog_schema.sql`
   - Then run `sql/02_create_tables.sql` to create the governance tables
3. Configure secrets & environment
   - Store Databricks PAT and any Foundation Model API credentials in Databricks Secrets
   - Edit `config/risk_weights.yaml` and `config/criticality_defaults.yaml` to match org preferences
4. Run discovery (local dev or Databricks job)
   - Example (local, needs connectivity and credentials):
     - python discovery/sync_model_registry.py --databricks-host <HOST> --token <TOKEN>
   - Or create a Databricks Job that runs the sync script on a schedule
5. Run adapters to populate `signal_index`
   - Example: run `adapters/read_lakehouse_monitoring.py` periodically (after Lakehouse Monitoring refresh)
6. Run correlation + scoring
   - Run `scoring/correlate_signals.py` to generate `incidents` and update `risk_scores`
7. Generate remediation text
   - Run `remediation/generate_root_cause.py` which calls `ai_query()` with incident evidence
8. Explore in AI/BI dashboard & Genie (view metric views that read `governance.model_health.*`)

---

## Configuration

- `config/risk_weights.yaml` — default weight values for scoring components (drift/quality/cost/guardrail)
- `config/criticality_defaults.yaml` — default criticality tier mappings and behavior
- Secrets:
  - Databricks PAT / Workspace host
  - Foundation Model API credentials (if using workspace external model endpoints)
  - Gateway / MLflow API credentials if needed

---

## Development & testing

- Unit-test scoring logic locally using synthetic `signal_index`/`lineage_events` CSVs.
- The correlation engine is the most critical area to test — add time-window edge cases and low-evidence scenarios (high confidence vs low confidence).
- To demo end-to-end in a sandbox workspace, create synthetic model entries in MLflow and synthetic signals in test tables, then run the full pipeline. The repo includes a recommended demo script in `workflows/third_eye_pipeline.yml` and a demo plan in `thirdeye_project.md`.

---

## Deploy & run (Databricks)

- Databricks Workflows orchestrate the pipeline:
  - Hourly discovery job
  - Lakehouse Monitoring adapter runs after monitoring refresh
  - Correlation & scoring runs once per monitoring refresh
  - Remediation (ai_query) runs per new incident (rate-limited)
- Use UC Delta tables for `governance.model_health.*` so AI/BI dashboards & Genie can read them directly.
- When provisioning monitors automatically, the discovery job may call Lakehouse Monitoring APIs; ensure service principal permissions are configured.

---

## Integrations

- Databricks Lakehouse Monitoring (drift/profile/quality)
- system.access lineage tables & Lineage REST API
- Unity Gateway: Asset Registry, usage tables, guardrails, Unified Trace (OpenTelemetry)
- MLflow Model Registry & Unity Catalog model objects
- Genie Space (metric views)
- Copilot Studio / Microsoft Teams / Slack (alerts & digest)
- Foundation Model APIs (`ai_query()`) for root-cause narrative & remediation suggestions

---

## Roadmap & V2 (planned, not in V1)

- Auto-retraining pipeline with champion/challenger evaluation
- Promotion gates and rollback automation
- Blast-radius analysis powered by lineage graph traversal
- Multi-workspace federation and UC row-level security
- Compliance reporting and richer model passports
- Cost-of-drift business impact estimation

V1 intentionally avoids action automation on Tier-1 models by default; design is schema-ready for V2 actions.

---

## Contributing

- Read `thirdeye_project.md` for the product spec and design rationale before contributing.
- Use feature branches (git flow) and open PRs for changes. Add tests for scoring/logic changes.
- For sensitive changes (security, data retention, automated actions), require at least one senior reviewer and a test/demo run in a sandbox workspace.

---

## Screenshots & visuals

UI mockups and architecture visuals are included in the repo's `docs/` (or the top-level images). They illustrate:
- Teams/Copilot Studio incident card (alert + remediation)
- End-to-end lifecycle diagram (Discover → Watch Again)
- Control Room dashboard mockup
- Product hero / visual identity
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 09_00_11 PM" src="https://github.com/user-attachments/assets/22c52c31-75f6-4b6d-bfa9-3e75208e36e4" />
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 08_54_12 PM" src="https://github.com/user-attachments/assets/0c819ca3-34d6-4bce-83f1-436550dcc7c9" />
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 08_52_09 PM" src="https://github.com/user-attachments/assets/53d5a8bc-c6f5-4815-994a-8891f9e857a9" />
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 08_49_11 PM" src="https://github.com/user-attachments/assets/60227d8a-30f4-4c4a-a7d8-b737c4901238" />
<img width="1536" height="1024" alt="ChatGPT Image Sep 13, 2026, 08_47_53 PM" src="https://github.com/user-attachments/assets/20aba4d4-95a4-4f79-b693-af5192520ee1" />


---

## License & credits

- Copyright (c) [@dcsgod]
-  Apache-2.0

---

If you'd like, I can:
- Draft a ready-to-commit README.md in the repository (create file + PR).
- Generate example dataset JSON/CSV and a demo notebook to run an end-to-end synthetic pipeline.
- Produce the SQL files in `sql/` formatted for a particular Unity Catalog naming convention.
