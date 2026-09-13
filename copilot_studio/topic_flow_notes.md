# Third Eye — Copilot Studio Integration Notes

## Overview

Copilot Studio acts as the **thin multi-channel delivery layer** for Third Eye.
It does NOT call Foundation Models or compute anything — it reads from the governed
`governance.model_health` schema and formats the results for delivery.

Two message types:
- **Topic A**: Scheduled KPI digest (fleet health + top at-risk models + week-over-week trend)
- **Topic B**: Breach notification with AI-generated remediation card (triggered by incident creation)

Both read from the metric views. The "brains" (correlation + LLM reasoning) stay in Databricks.

---

## Prerequisites

1. A Power Automate connection to the Databricks workspace (using a service principal with
   `SELECT` on `governance.model_health.*`).
2. A Copilot Studio environment with the Teams channel enabled.
3. (Optional) Slack and Google Chat webhook URLs stored as Power Automate variables.
4. The `{{secrets/third_eye/teams_webhook}}` webhook URL configured in Databricks
   (referenced in `governance.model_health.alert_routing_config`).

---

## Topic A — Scheduled KPI Digest

### Trigger
- Schedule: daily at 08:00 AM (workspace timezone, configurable).
- Also triggerable on-demand via a Teams command: *"@ThirdEye status"*.

### Data Sources
```sql
-- Step 1: Fleet summary
SELECT * FROM governance.model_health.vw_fleet_health_summary;

-- Step 2: Top 5 at-risk models (for the digest body)
SELECT model_name, health_score, health_tier, criticality_tier,
       open_incidents, trend_label
FROM governance.model_health.vw_model_leaderboard
WHERE health_tier IN ('at_risk', 'critical')
ORDER BY health_score ASC
LIMIT 5;

-- Step 3: 24-hour tier changes
SELECT model_name, previous_health_tier, new_health_tier, change_direction, score_delta
FROM governance.model_health.vw_daily_health_changes
ORDER BY score_delta DESC
LIMIT 5;
```

### Adaptive Card Template (Teams)

```json
{
  "type": "AdaptiveCard",
  "version": "1.5",
  "body": [
    {
      "type": "Container",
      "style": "emphasis",
      "items": [
        { "type": "TextBlock", "text": "🧿 Third Eye — Daily AI Fleet Digest", "weight": "Bolder", "size": "Large" },
        { "type": "TextBlock", "text": "{{DATE}} | {{WORKSPACE_NAME}}", "isSubtle": true }
      ]
    },
    {
      "type": "ColumnSet",
      "columns": [
        { "type": "Column", "width": "stretch", "items": [
          { "type": "TextBlock", "text": "✅ Healthy", "weight": "Bolder", "color": "Good" },
          { "type": "TextBlock", "text": "{{HEALTHY_COUNT}} models ({{PCT_HEALTHY}}%)" }
        ]},
        { "type": "Column", "width": "stretch", "items": [
          { "type": "TextBlock", "text": "👀 Watch", "weight": "Bolder", "color": "Warning" },
          { "type": "TextBlock", "text": "{{WATCH_COUNT}} models ({{PCT_WATCH}}%)" }
        ]},
        { "type": "Column", "width": "stretch", "items": [
          { "type": "TextBlock", "text": "⚠️ At Risk", "weight": "Bolder", "color": "Attention" },
          { "type": "TextBlock", "text": "{{AT_RISK_COUNT}} models ({{PCT_AT_RISK}}%)" }
        ]},
        { "type": "Column", "width": "stretch", "items": [
          { "type": "TextBlock", "text": "🚨 Critical", "weight": "Bolder", "color": "Attention" },
          { "type": "TextBlock", "text": "{{CRITICAL_COUNT}} models ({{PCT_CRITICAL}}%)" }
        ]}
      ]
    },
    {
      "type": "TextBlock",
      "text": "Fleet Average Health Score: **{{AVG_HEALTH_SCORE}}** | Open Incidents: **{{OPEN_INCIDENTS}}**",
      "wrap": true
    },
    {
      "type": "TextBlock",
      "text": "📋 Top Models Needing Attention",
      "weight": "Bolder",
      "separator": true
    },
    {
      "type": "Table",
      "columns": [
        { "width": 2 }, { "width": 1 }, { "width": 1 }, { "width": 1 }
      ],
      "rows": [
        {
          "type": "TableRow",
          "style": "accent",
          "cells": [
            { "type": "TableCell", "items": [{ "type": "TextBlock", "text": "Model", "weight": "Bolder" }] },
            { "type": "TableCell", "items": [{ "type": "TextBlock", "text": "Score", "weight": "Bolder" }] },
            { "type": "TableCell", "items": [{ "type": "TextBlock", "text": "Tier", "weight": "Bolder" }] },
            { "type": "TableCell", "items": [{ "type": "TextBlock", "text": "Incidents", "weight": "Bolder" }] }
          ]
        }
        // Repeat this row pattern for each of the top 5 at-risk models
        // (populated dynamically by Power Automate Apply-To-Each)
      ]
    },
    {
      "type": "TextBlock",
      "text": "📊 24h Tier Changes: {{TIER_CHANGES_TODAY}} models changed health tier",
      "separator": true
    }
  ],
  "actions": [
    {
      "type": "Action.OpenUrl",
      "title": "Open Dashboard",
      "url": "{{DASHBOARD_URL}}"
    },
    {
      "type": "Action.OpenUrl",
      "title": "Ask Genie",
      "url": "{{GENIE_SPACE_URL}}"
    }
  ]
}
```

### Fan-out to Slack / Google Chat

After sending the Teams card, the Power Automate flow uses HTTP actions to
post a simplified text version to Slack and Google Chat webhooks:

**Slack message template:**
```
🧿 *Third Eye Daily Digest*
Fleet: ✅ {{HEALTHY_COUNT}} | 👀 {{WATCH_COUNT}} | ⚠️ {{AT_RISK_COUNT}} | 🚨 {{CRITICAL_COUNT}}
Avg Health Score: *{{AVG_HEALTH_SCORE}}* | Open Incidents: *{{OPEN_INCIDENTS}}*

Top at-risk models:
{{FOR EACH model IN top_5_at_risk}}
• `{{model.model_name}}` — {{model.health_score}} / 100 ({{model.criticality_tier}})
{{END FOR}}

<{{DASHBOARD_URL}}|Open Dashboard> | <{{GENIE_SPACE_URL}}|Ask Genie>
```

---

## Topic B — Breach Notification with Remediation

### Trigger
- Databricks Workflow writes a new row to `governance.model_health.incidents`.
- A Databricks Workflows alert (or a Power Automate polling trigger) detects
  `status = 'open' AND opened_at > DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())`.
- Fires for incidents where `model.criticality_tier >= 'tier_2_operational'` (configurable
  in `governance.model_health.alert_routing_config`).

### Data Sources
```sql
-- Step 1: Get incident details + remediation
SELECT
  i.incident_id, i.model_id, i.severity, i.trigger_signal_types,
  i.recommended_action, i.opened_at,
  mrm.model_name, mrm.serving_endpoint, mrm.criticality_tier, mrm.business_domain, mrm.owning_team,
  rs.health_score, rs.health_tier,
  rem.explanation, rem.urgency, rem.suggested_actions
FROM governance.model_health.incidents i
JOIN governance.model_health.model_registry_map mrm ON i.model_id = mrm.model_id
JOIN (SELECT model_id, health_score, health_tier,
      ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY computed_at DESC) AS rn
      FROM governance.model_health.risk_scores) rs
  ON i.model_id = rs.model_id AND rs.rn = 1
LEFT JOIN governance.model_health.remediation_suggestions rem ON i.incident_id = rem.incident_id
WHERE i.incident_id = '{{INCIDENT_ID}}';
```

### Adaptive Card Template (Teams) — Breach Notification

```json
{
  "type": "AdaptiveCard",
  "version": "1.5",
  "body": [
    {
      "type": "Container",
      "style": "attention",
      "items": [
        { "type": "TextBlock", "text": "🚨 Third Eye — Model Health Incident", "weight": "Bolder", "size": "Large", "color": "Attention" },
        { "type": "TextBlock", "text": "{{SEVERITY}} | {{OPENED_AT}}", "isSubtle": true }
      ]
    },
    {
      "type": "FactSet",
      "facts": [
        { "title": "Model", "value": "{{MODEL_NAME}} v{{MODEL_VERSION}}" },
        { "title": "Endpoint", "value": "{{SERVING_ENDPOINT}}" },
        { "title": "Criticality", "value": "{{CRITICALITY_TIER}}" },
        { "title": "Health Score", "value": "{{HEALTH_SCORE}} / 100 ({{HEALTH_TIER}})" },
        { "title": "Signals Fired", "value": "{{TRIGGER_SIGNAL_TYPES}}" },
        { "title": "Recommended Action", "value": "{{RECOMMENDED_ACTION}}" },
        { "title": "Urgency", "value": "{{URGENCY}}" }
      ]
    },
    {
      "type": "TextBlock",
      "text": "🔍 Root Cause Analysis",
      "weight": "Bolder",
      "separator": true
    },
    {
      "type": "TextBlock",
      "text": "{{EXPLANATION}}",
      "wrap": true
    },
    {
      "type": "TextBlock",
      "text": "🛠️ Suggested Actions",
      "weight": "Bolder",
      "separator": true
    },
    {
      "type": "TextBlock",
      "text": "{{SUGGESTED_ACTIONS_TEXT}}",
      "wrap": true
    }
  ],
  "actions": [
    {
      "type": "Action.OpenUrl",
      "title": "View Model Detail",
      "url": "{{MODEL_DETAIL_DASHBOARD_URL}}"
    },
    {
      "type": "Action.OpenUrl",
      "title": "Investigate in Genie",
      "url": "{{GENIE_SPACE_URL}}"
    },
    {
      "type": "Action.Submit",
      "title": "✓ Acknowledge",
      "data": { "action": "acknowledge", "incident_id": "{{INCIDENT_ID}}" }
    }
  ]
}
```

---

## Power Automate Flow Structure

### Topic A (Scheduled Digest)

```
[Recurrence: Daily 08:00]
  → [Databricks SQL: SELECT vw_fleet_health_summary]
  → [Databricks SQL: SELECT vw_model_leaderboard top 5 at_risk]
  → [Databricks SQL: SELECT vw_daily_health_changes top 5]
  → [Build adaptive card JSON (compose)]
  → [Post card to Teams channel]
  → [HTTP POST to Slack webhook (text format)]
  → [HTTP POST to Google Chat webhook (text format)]
```

### Topic B (Breach Notification)

```
[Trigger: Databricks alert OR polling (5-min interval)]
  → [Condition: New open incident exists in last 5 minutes]
  → [Databricks SQL: SELECT incident + remediation for incident_id]
  → [Condition: criticality_tier >= configured minimum tier]
    YES →
      → [Build adaptive card JSON]
      → [Post card to Teams channel]
      → [If severity = critical: Post to PagerDuty via HTTP]
      → [HTTP POST to Slack webhook]
  NO → [End]
```

---

## Configuration Variables (Power Automate environment variables)

| Variable | Description | Example |
|---|---|---|
| `DATABRICKS_WORKSPACE_URL` | Workspace URL | `https://adb-12345.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Service principal token (stored in key vault) | `dapiXXX` |
| `TEAMS_WEBHOOK_URL` | Teams incoming webhook | `https://outlook.office.com/webhook/...` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook | `https://hooks.slack.com/services/...` |
| `GCHAT_WEBHOOK_URL` | Google Chat webhook | `https://chat.googleapis.com/v1/spaces/...` |
| `DASHBOARD_URL` | AI/BI dashboard URL | `https://adb-12345.azuredatabricks.net/sql/dashboards/...` |
| `GENIE_SPACE_URL` | Genie Space URL | `https://adb-12345.azuredatabricks.net/genie/spaces/...` |
| `MIN_ALERT_TIER` | Minimum criticality tier for breach alerts | `tier_2_operational` |
