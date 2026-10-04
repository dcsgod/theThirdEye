# The Third Eye

### AI model health and governance control plane for Databricks

> **Watch. Understand. Act.**
>
> Databricks provides the signals. The Third Eye turns them into decisions.

[![CI](https://github.com/dcsgod/theThirdEye/actions/workflows/ci.yml/badge.svg)](https://github.com/dcsgod/theThirdEye/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/the-third-eye?style=for-the-badge&color=4267E8)](https://pypi.org/project/the-third-eye/)
[![Python](https://img.shields.io/pypi/pyversions/the-third-eye?style=for-the-badge)](https://pypi.org/project/the-third-eye/)
[![License](https://img.shields.io/badge/License-Apache%202.0-4267E8?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

---

## Release

The current PyPI distribution is `the-third-eye`.

```bash
pip install the-third-eye
```

The Python import namespace is `third_eye`.

---

## What is The Third Eye?

The Third Eye is a **Databricks-native AI health and governance control plane**.

It does not replace Model Serving, MLflow, Unity Catalog, Lakehouse Monitoring, Unity Gateway, or Workflows. It sits above those primitives and turns their signals into an explainable operating view:

```text
Databricks signals
      │
      ├── monitoring
      ├── inference / trace
      ├── lineage
      ├── usage / cost
      └── governance / guardrails
      │
      ▼
The Third Eye
      │
      ├── normalize
      ├── correlate
      ├── score
      ├── explain
      └── recommend
      │
      ▼
governed model health
      │
      ├── dashboard
      ├── Genie
      └── Teams / Slack
```

**Positioning:** not another drift detector. Not another dashboard. The Third Eye is the **decision layer above the native Databricks AI stack**.

---

## Why this exists

AI fleets fail in ways that are difficult to diagnose from a single metric.

A model can look healthy in one system while another already shows a warning:

- feature drift is rising
- quality is deteriorating
- an upstream table changed
- token or endpoint cost spiked
- a gateway policy was breached
- model criticality makes the event more urgent

The Third Eye correlates these signals into a single evidence-backed health view.

### The core distinction

```text
Traditional monitoring:
metric → threshold → alert

The Third Eye:
signals → correlation → context → risk → explanation → action
```

---

## Core capabilities

| Capability | What The Third Eye adds |
|---|---|
| **Zero-touch discovery** | Builds a governed model inventory from MLflow, Unity Catalog, and Gateway assets |
| **Signal normalization** | Creates a common index over native Databricks signals |
| **Evidence correlation** | Links drift, quality, usage, guardrail, and lineage events |
| **Composite health** | Produces a criticality-aware 0-100 health score |
| **Confidence** | Separates strong evidence from incomplete evidence |
| **Root cause** | Generates evidence-grounded narratives |
| **Remediation** | Produces ranked next actions rather than just alerts |
| **Model passport** | Produces a compact governance and audit view per model |
| **Decision surface** | Feeds AI/BI, Genie, Teams, and Slack from governed outputs |

---

## Architecture

```text
┌──────────────────────── Databricks native layer ────────────────────────┐
│ Unity Catalog · MLflow · Monitoring · Inference / Trace · Lineage       │
│ Unity Gateway · Usage · Guardrails · Databricks Workflows               │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │ Discovery & Sync         │
                    │ model / asset registry  │
                    └─────────────┬────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │ Signal Normalization     │
                    │ common signal index      │
                    └─────────────┬────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │ Correlation & Scoring    │
                    │ health + confidence      │
                    └─────────────┬────────────┘
                                  ▼
                    governance.model_health.*
                         │        │        │
                         ▼        ▼        ▼
                       AI/BI    Genie   Alerts
                                  │
                                  ▼
                        Root cause / remediation
```

![The Third Eye](https://github.com/user-attachments/assets/255a6eb7-6836-4570-9f84-2633b03916ed)

---

## Repository structure

The repository remains the full Databricks implementation and deployment reference:

```text
adapters/         native-signal adapters
discovery/        model and lineage discovery
scoring/          correlation and health scoring jobs
remediation/     root-cause and remediation generation
passport/         model passport generation
sql/              Unity Catalog bootstrap + governed tables
genie/            Genie instructions
copilot_studio/  notification / topic notes
workflows/       Databricks Workflow definitions
demo/             product demonstration assets
v2_stretch/       planned governed-autonomy components
third_eye/       reusable Python scoring primitives
```

---

## Quickstart

### Install the reusable Python package

```bash
pip install the-third-eye
```

### Use the portable scoring kernel

```python
from third_eye.scoring import health_score, health_tier

score = health_score(
    drift=0.30,
    quality=0.92,
    cost=0.15,
    guardrail=0.05,
)

print(f"Health: {score:.2f}")
print(f"Tier:   {health_tier(score)}")
```

The package is intentionally small. It exposes deterministic scoring primitives while the Databricks-specific orchestration remains in the repository.

### Databricks integrations

```bash
pip install "the-third-eye[databricks]"
```

This optional extra installs the Databricks SDK, MLflow, and Spark dependencies used by the workspace integration layer.

---

## Run locally

The Databricks jobs support the existing mock-mode development path:

```bash
set THIRD_EYE_MOCK_MODE=true
python scoring/compute_health_score.py
python scoring/correlate_signals.py
```

PowerShell:

```powershell
$env:THIRD_EYE_MOCK_MODE = "true"
python scoring/compute_health_score.py
python scoring/correlate_signals.py
```

---

## The health score

The central composite calculation is:

```text
health_score = 100 - (
    w1 · drift
  + w2 · (1 - quality)
  + w3 · cost
  + w4 · guardrail
) · criticality_weight
```

The portable Python implementation keeps this kernel deterministic and independent of Databricks runtime concerns.

---

## Evidence before action

The Third Eye follows a **read-first, evidence-before-action** principle.

A single weak signal should not automatically become a production action.

```text
signal
  ↓
corroborating evidence
  ↓
criticality context
  ↓
risk / confidence
  ↓
incident
  ↓
root cause
  ↓
recommended action
```

High-impact production actions remain policy-controlled.

---

## Security and governance

The control plane is designed around governed access rather than copying source systems.

Key principles:

- native Databricks tables remain the source of record
- normalized signals store pointers and snapshots rather than duplicating raw telemetry
- secrets should be resolved through Databricks Secrets
- high-impact automation remains policy-controlled
- evidence and confidence accompany health decisions

---

## Roadmap

### V1

Discovery → normalization → correlation → scoring → diagnosis → governed outputs.

### V2

Governed autonomy:

```text
health degradation
      ↓
candidate retraining
      ↓
champion / challenger
      ↓
approval gate
      ↓
promotion / rollback
```

Planned work includes blast-radius analysis, multi-workspace federation, silent-failure detection, richer compliance reporting, and cost-of-drift business impact.

---

## Development

```bash
git clone https://github.com/dcsgod/theThirdEye.git
cd theThirdEye

python -m pip install -e ".[dev]"
ruff check .
pytest
python -m build
python -m twine check dist/*
```

---

## Publishing to PyPI

The repository includes a GitHub Actions workflow for **PyPI Trusted Publishing**.

```text
GitHub Release
      ↓
build sdist + wheel
      ↓
twine check
      ↓
PyPI Trusted Publishing
```

For the first release, configure a PyPI Trusted Publisher with:

```text
Owner:       dcsgod
Repository:  theThirdEye
Workflow:    publish.yml
Environment: pypi
```

Then create a GitHub release such as:

```text
v0.1.0
```

The workflow publishes the built distributions to PyPI without a long-lived API token.

---

## License

Apache-2.0.

---

<div align="center">

**The Third Eye**

*Watch. Understand. Act.*

</div>
