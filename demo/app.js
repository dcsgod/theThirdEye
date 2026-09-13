// =============================================================================
// The Third Eye — Interactive Web Demo
// app.js — Application Logic
// =============================================================================

// ─────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────
const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

function timeAgo(date) {
  const mins = Math.round((Date.now() - date.getTime()) / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.round(hrs/24)}d ago`;
}

function healthColor(tier) {
  return { healthy: '#10b981', watch: '#f59e0b', at_risk: '#f97316', critical: '#ef4444' }[tier] || '#6b7280';
}

function tierClass(tier) {
  return { tier_1_business: 'tier-1', tier_2_operational: 'tier-2', tier_3_low: 'tier-3', tier_0_experimental: 'tier-0' }[tier] || 'tier-2';
}

function tierLabel(tier) {
  return { tier_1_business: '🔴 Tier 1', tier_2_operational: '🟠 Tier 2', tier_3_low: '🟢 Tier 3', tier_0_experimental: '🧪 Exp' }[tier] || tier;
}

function signalChip(type) {
  const cls = { drift: 'drift', cost: 'cost', guardrail: 'guardrail', accuracy: 'accuracy' };
  const icons = { drift: '〰️', cost: '💰', guardrail: '🛡️', accuracy: '🎯' };
  const types = type.split(',').map(t => t.trim());
  return types.map(t => `<span class="signal-chip ${cls[t] || ''}">${icons[t] || '•'} ${t}</span>`).join('');
}

function showToast(message, type = 'info') {
  const container = $('#toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  const icon = { success: '✅', error: '🚨', info: 'ℹ️' }[type] || 'ℹ️';
  toast.innerHTML = `<span>${icon}</span> ${message}`;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 4200);
}

// ─────────────────────────────────────────────
// Navigation
// ─────────────────────────────────────────────
let currentSection = 'fleet';

function navigate(sectionId) {
  // Hide all sections
  $$('.page-section').forEach(s => s.classList.remove('active'));
  $$('.nav-item').forEach(n => n.classList.remove('active'));

  // Show target
  const section = $(`#section-${sectionId}`);
  if (section) section.classList.add('active');

  const navItem = $(`.nav-item[data-section="${sectionId}"]`);
  if (navItem) navItem.classList.add('active');

  // Update topbar title
  const titles = {
    fleet:     'Fleet Health Overview',
    leaderboard: 'Model Leaderboard',
    incidents: 'Active Incidents',
    passport:  'Model Passports',
    pipeline:  'Pipeline Execution',
    genie:     'Genie Investigation Space',
    v2:        'V2 Capabilities',
  };
  $('#topbar-title').textContent = titles[sectionId] || sectionId;
  currentSection = sectionId;
}

// ─────────────────────────────────────────────
// Fleet Overview
// ─────────────────────────────────────────────
function renderFleetOverview() {
  const f = DEMO_DATA.fleet;

  // KPI tiles
  $('#kpi-healthy').textContent  = f.healthy_count;
  $('#kpi-watch').textContent    = f.watch_count;
  $('#kpi-at-risk').textContent  = f.at_risk_count;
  $('#kpi-critical').textContent = f.critical_count;
  $('#kpi-pct-healthy').textContent  = `${f.pct_healthy}% of fleet`;
  $('#kpi-pct-watch').textContent    = `${f.pct_watch}% of fleet`;
  $('#kpi-pct-at-risk').textContent  = `${f.pct_at_risk}% of fleet`;
  $('#kpi-pct-critical').textContent = `${f.pct_critical}% of fleet`;
  $('#kpi-avg-score').textContent    = f.avg_health_score.toFixed(1);
  $('#kpi-incidents').textContent    = f.total_open_incidents;
  $('#kpi-changes').textContent      = f.tier_changes_today;
  $('#kpi-total').textContent        = f.total_models;

  // Last run time
  $('#last-run-time').textContent = timeAgo(f.last_scoring_run);

  // Donut chart (CSS-based)
  renderDonutChart();

  // Mini leaderboard in overview (top 5 worst)
  const topWorst = DEMO_DATA.models.slice(0, 5);
  const overviewTbody = $('#overview-model-rows');
  overviewTbody.innerHTML = topWorst.map(m => renderModelRow(m)).join('');
  attachModelRowListeners(overviewTbody);
}

function renderDonutChart() {
  const f = DEMO_DATA.fleet;
  const canvas = $('#health-donut');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const cx = 80, cy = 80, r = 65, inner = 42;

  ctx.clearRect(0, 0, 160, 160);

  const slices = [
    { value: f.critical_count, color: '#ef4444' },
    { value: f.at_risk_count,  color: '#f97316' },
    { value: f.watch_count,    color: '#f59e0b' },
    { value: f.healthy_count,  color: '#10b981' },
  ];

  let startAngle = -Math.PI / 2;
  const total = slices.reduce((s, x) => s + x.value, 0);

  // Draw glow ring
  ctx.save();
  ctx.shadowColor = '#7c3aed';
  ctx.shadowBlur = 20;
  slices.forEach(slice => {
    if (slice.value === 0) return;
    const angle = (slice.value / total) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, startAngle, startAngle + angle);
    ctx.closePath();
    ctx.fillStyle = slice.color;
    ctx.fill();
    startAngle += angle;
  });
  ctx.restore();

  // Inner circle cutout
  ctx.beginPath();
  ctx.arc(cx, cy, inner, 0, Math.PI * 2);
  ctx.fillStyle = '#0d0d1f';
  ctx.fill();

  // Center text
  ctx.fillStyle = '#f1f5f9';
  ctx.font = 'bold 22px Inter';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(f.avg_health_score.toFixed(0), cx, cy - 8);
  ctx.font = '11px Inter';
  ctx.fillStyle = '#94a3b8';
  ctx.fillText('avg score', cx, cy + 10);
}

// ─────────────────────────────────────────────
// Model Leaderboard
// ─────────────────────────────────────────────
let sortColumn = 'health_score';
let sortAsc = true;
let filterTier = 'all';

function renderLeaderboard() {
  let models = [...DEMO_DATA.models];

  // Filter
  if (filterTier !== 'all') {
    models = models.filter(m => m.criticality_tier === filterTier);
  }

  // Sort
  models.sort((a, b) => {
    let av = a[sortColumn], bv = b[sortColumn];
    if (av === null || av === undefined) av = sortAsc ? Infinity : -Infinity;
    if (bv === null || bv === undefined) bv = sortAsc ? Infinity : -Infinity;
    return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  const tbody = $('#leaderboard-body');
  tbody.innerHTML = models.map(m => renderModelRow(m)).join('');
  attachModelRowListeners(tbody);
}

function renderModelRow(m) {
  const hc = healthColor(m.health_tier);
  const tierCls = tierClass(m.criticality_tier);
  const incCls = m.open_incidents > 0 ? 'has-incidents' : 'no-incidents';

  return `
    <tr data-model-id="${m.model_id}" class="model-row">
      <td>
        <div class="model-name-cell">
          <span class="model-name">${m.model_name}</span>
          <span class="model-endpoint">${m.serving_endpoint || '—'}</span>
        </div>
      </td>
      <td><span class="asset-badge">${m.asset_type}</span></td>
      <td><span class="tier-badge ${tierCls}">${tierLabel(m.criticality_tier)}</span></td>
      <td>
        <div class="health-bar-container">
          <div class="health-bar-track">
            <div class="health-bar-fill" style="width:${m.health_score}%;background:${hc}"></div>
          </div>
          <span class="health-score-num" style="color:${hc}">${m.health_score.toFixed(0)}</span>
        </div>
      </td>
      <td><span class="health-badge ${m.health_tier}">${{ healthy:'✅ Healthy', watch:'👀 Watch', at_risk:'⚠️ At Risk', critical:'🚨 Critical' }[m.health_tier]}</span></td>
      <td><span class="trend-label ${m.trend_label.includes('↓') ? 'degraded' : 'stable'}">${m.trend_label}</span></td>
      <td><span class="incident-count ${incCls}">${m.open_incidents > 0 ? '🔴 ' + m.open_incidents : '—'}</span></td>
      <td style="color:var(--text-muted);font-size:12px">${m.business_domain}</td>
      <td style="color:var(--text-muted);font-size:12px">${timeAgo(m.last_scored_at)}</td>
    </tr>
  `;
}

function attachModelRowListeners(ctx) {
  $$('.model-row', ctx).forEach(row => {
    row.addEventListener('click', () => {
      const modelId = row.getAttribute('data-model-id');
      const model = DEMO_DATA.models.find(m => m.model_id === modelId);
      if (model) openModelDrawer(model);
    });
  });
}

// ─────────────────────────────────────────────
// Model Detail Drawer
// ─────────────────────────────────────────────
function openModelDrawer(model) {
  const drawer = $('#model-drawer');
  const overlay = $('#drawer-overlay');

  // Populate header
  $('#drawer-model-name').textContent = model.model_name;
  $('#drawer-model-version').textContent = `v${model.model_version}`;
  $('#drawer-model-endpoint').textContent = model.serving_endpoint || '—';
  const hc = healthColor(model.health_tier);
  $('#drawer-health-score').textContent = model.health_score.toFixed(1);
  $('#drawer-health-score').style.color = hc;
  $('#drawer-health-tier').textContent = model.health_tier.replace('_',' ').toUpperCase();
  $('#drawer-health-tier').style.color = hc;
  $('#drawer-tier-badge').textContent = tierLabel(model.criticality_tier);
  $('#drawer-tier-badge').className = `tier-badge ${tierClass(model.criticality_tier)}`;

  // Confidence
  const conf = model.confidence;
  $('#drawer-confidence').textContent = (conf * 100).toFixed(0) + '%';
  $('#drawer-confidence-fill').style.width = (conf * 100) + '%';

  // Component breakdown
  const components = [
    { id: 'drawer-drift',     value: model.drift_component,     label: 'Drift' },
    { id: 'drawer-quality',   value: model.quality_component,   label: 'Quality' },
    { id: 'drawer-cost',      value: model.cost_component,      label: 'Cost Anomaly' },
    { id: 'drawer-guardrail', value: model.guardrail_component, label: 'Guardrail' },
  ];

  components.forEach(c => {
    const el = $(`#${c.id}`);
    if (!el) return;
    if (c.value === null || c.value === undefined) {
      el.textContent = 'N/A';
      el.className = 'component-value na';
    } else {
      el.textContent = (c.value * 100).toFixed(0) + '%';
      el.className = 'component-value ' + (c.value > 0.5 ? 'high' : c.value > 0.25 ? 'medium' : 'low');
    }
  });

  // Incidents for this model
  const modelIncidents = DEMO_DATA.incidents.filter(i => i.model_id === model.model_id);
  const incHtml = modelIncidents.length > 0
    ? modelIncidents.map(i => `
        <div class="incident-card ${i.severity}" style="margin-bottom:10px">
          <div style="font-size:13px;font-weight:700;margin-bottom:6px">${{'critical':'🚨','high':'⚠️','medium':'👀','low':'ℹ️'}[i.severity]} ${i.severity.toUpperCase()} Incident</div>
          <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px">Signals: ${i.trigger_signal_types}</div>
          <div class="incident-narrative" style="margin-bottom:0">${i.root_cause_narrative?.substring(0,200)}...</div>
        </div>`)
        .join('')
    : '<div style="color:var(--text-muted);font-size:13px">No active incidents.</div>';
  $('#drawer-incidents').innerHTML = incHtml;

  // Lineage
  const modelLineage = DEMO_DATA.incidents
    .filter(i => i.model_id === model.model_id)
    .flatMap(i => i.lineage_context || []);

  const lineageHtml = modelLineage.length > 0
    ? modelLineage.map(le => `
        <div class="lineage-item">
          <span class="lineage-icon">🔗</span>
          <span class="lineage-table">${le.upstream_table}</span>
          <span class="lineage-event-type ${le.event_type}">${le.event_type}</span>
        </div>`)
      .join('')
    : '<div style="color:var(--text-muted);font-size:12px">No upstream lineage events recorded.</div>';
  $('#drawer-lineage').innerHTML = lineageHtml;

  drawer.classList.add('open');
  overlay.classList.add('open');
}

function closeModelDrawer() {
  $('#model-drawer').classList.remove('open');
  $('#drawer-overlay').classList.remove('open');
}

// ─────────────────────────────────────────────
// Incidents
// ─────────────────────────────────────────────
function renderIncidents() {
  const container = $('#incidents-list');
  container.innerHTML = DEMO_DATA.incidents.map(inc => `
    <div class="incident-card ${inc.severity}" id="inc-${inc.incident_id}">
      <div class="incident-header">
        <span class="incident-severity-icon">${{ critical:'🚨', high:'⚠️', medium:'👀', low:'ℹ️' }[inc.severity]}</span>
        <div>
          <div class="incident-title">${inc.model_name}</div>
          <div class="incident-meta">
            <span>🆔 ${inc.incident_id}</span>
            <span>📅 ${timeAgo(inc.opened_at)}</span>
            <span>🎯 ${inc.recommended_action}</span>
            <span class="health-badge ${inc.health_tier}" style="font-size:11px">${inc.health_score.toFixed(0)}/100</span>
            <span class="tier-badge ${tierClass(inc.criticality_tier)}" style="font-size:11px">${tierLabel(inc.criticality_tier)}</span>
          </div>
        </div>
      </div>

      <div class="incident-signals">
        ${signalChip(inc.trigger_signal_types)}
      </div>

      <div class="incident-narrative">
        <strong style="color:var(--text-brand)">🔍 Root Cause Analysis:</strong><br>
        ${inc.root_cause_narrative}
      </div>

      <div class="incident-actions-title">Suggested Remediation Actions</div>
      <div class="action-list">
        ${inc.suggested_actions.map(a => `
          <div class="action-item">
            <span class="action-rank">${a.rank}</span>
            <div>
              <div class="action-text">${a.action}</div>
              <div class="action-badges">
                <span class="effort-badge">Effort: ${a.effort}</span>
                <span class="impact-badge ${a.impact}">Impact: ${a.impact}</span>
              </div>
            </div>
          </div>`).join('')}
      </div>

      ${inc.lineage_context && inc.lineage_context.length > 0 ? `
        <div style="margin-top:14px">
          <div class="incident-actions-title">🔗 Corroborating Lineage Events</div>
          ${inc.lineage_context.map(le => `
            <div class="lineage-item" style="margin-top:6px">
              <span class="lineage-icon">📋</span>
              <span class="lineage-table">${le.upstream_table}</span>
              <span class="lineage-event-type ${le.event_type}">${le.event_type}</span>
              <span style="font-size:11px;color:var(--text-muted)">${timeAgo(le.event_time)}</span>
            </div>`).join('')}
        </div>` : ''}

      <div class="incident-footer">
        <span class="urgency-badge ${inc.remediation_urgency}">
          ${inc.remediation_urgency === 'urgent' ? '🔥 URGENT' : '⏳ Can Wait'}
        </span>
        <button class="acknowledge-btn" onclick="acknowledgeIncident('${inc.incident_id}')">
          ✓ Acknowledge
        </button>
      </div>
    </div>
  `).join('');
}

function acknowledgeIncident(id) {
  const inc = DEMO_DATA.incidents.find(i => i.incident_id === id);
  if (!inc) return;
  inc.status = 'acknowledged';
  showToast(`Incident for ${inc.model_name} acknowledged.`, 'success');
  // Visual feedback
  const card = $(`#inc-${id}`);
  if (card) {
    card.style.opacity = '0.6';
    card.querySelector('.acknowledge-btn').textContent = '✓ Acknowledged';
    card.querySelector('.acknowledge-btn').disabled = true;
  }
}

// ─────────────────────────────────────────────
// Pipeline View
// ─────────────────────────────────────────────
function renderPipeline() {
  const container = $('#pipeline-tasks');
  const icons = {
    sync_model_registry: '🔍',
    lineage_walker: '🔗',
    read_lakehouse_monitoring: '📊',
    read_gateway_usage: '💸',
    read_guardrail_events: '🛡️',
    compute_health_score: '🧮',
    correlate_signals: '🔀',
    generate_root_cause: '🧠',
  };
  const statLabels = {
    models_discovered: 'models discovered',
    events_found: 'lineage events',
    signals_emitted: 'signals emitted',
    models_scored: 'models scored',
    incidents_opened: 'incidents opened',
    narratives_generated: 'narratives generated',
  };

  container.innerHTML = DEMO_DATA.pipeline_runs.map((run, i) => {
    const icon = icons[run.task] || '⚙️';
    const statKey = Object.keys(statLabels).find(k => run[k] !== undefined);
    const statVal = statKey ? `${run[statKey]} ${statLabels[statKey]}` : '';
    return `
      <div class="pipeline-task" style="animation-delay:${i * 60}ms">
        <div class="task-icon ${run.status}">
          ${run.status === 'running' ? '⟳' : run.status === 'success' ? icon : '○'}
        </div>
        <div class="task-details">
          <div class="task-name">${run.task}</div>
          <div class="task-stats">${statVal}</div>
          <div class="task-duration">${timeAgo(run.time)} · ${run.duration_s}s</div>
        </div>
        <span class="health-badge ${run.status === 'success' ? 'healthy' : 'watch'}" style="font-size:11px;margin-top:8px">
          ${run.status === 'success' ? '✓ success' : '● ' + run.status}
        </span>
      </div>`;
  }).join('');
}

function runPipelineDemo() {
  showToast('🚀 Pipeline triggered — running in demo mode...', 'info');
  $('#pipeline-running-bar').classList.add('active');
  const btn = $('#run-pipeline-btn');
  btn.textContent = '⟳ Running...';
  btn.disabled = true;

  // Simulate pipeline progress
  let step = 0;
  const interval = setInterval(() => {
    step++;
    if (step >= DEMO_DATA.pipeline_runs.length) {
      clearInterval(interval);
      $('#pipeline-running-bar').classList.remove('active');
      btn.textContent = '▶ Run Pipeline';
      btn.disabled = false;
      showToast('✅ Pipeline completed: 3 incidents updated, 12 models scored.', 'success');
      if (currentSection === 'pipeline') renderPipeline();
    }
  }, 600);
}

// ─────────────────────────────────────────────
// Genie Space
// ─────────────────────────────────────────────
const GENIE_RESPONSES = {
  'which models are at risk': () => {
    const atRisk = DEMO_DATA.models.filter(m => ['at_risk','critical'].includes(m.health_tier));
    return `Found **${atRisk.length} models** needing attention:\n\n${atRisk.map(m =>
      `• **${m.model_name}** — ${m.health_score.toFixed(1)}/100 (${m.health_tier}) | ${tierLabel(m.criticality_tier)}`
    ).join('\n')}\n\nThe most critical is **${atRisk[0].model_name}** with a health score of only **${atRisk[0].health_score.toFixed(1)}**.`;
  },
  'why is credit_risk at risk': () =>
    `**credit_risk_classifier** is in **CRITICAL** state (health score: 22.4/100).\n\n🔍 **Root Cause:** Three signals co-occurred within the 2-hour correlation window:\n• **Drift** on income_level feature (0.73 — 5× above threshold)\n• **Cost spike** — $47.80/hr vs. baseline ~$10/hr\n• **PII violations** — 45 per 1,000 requests\n\n📋 **Lineage correlation:** \`main.raw.transactions\` had a schema_change 26 hours before the drift appeared.\n\n⚡ **Recommended:** Inspect the schema change, fix PII masking in the upstream pipeline, then trigger a champion/challenger evaluation.`,
  'show me fleet health': () => {
    const f = DEMO_DATA.fleet;
    return `**Fleet Health Summary** (as of ${timeAgo(f.last_scoring_run)}):\n\n• ✅ Healthy: **${f.healthy_count}** models (${f.pct_healthy}%)\n• 👀 Watch: **${f.watch_count}** models (${f.pct_watch}%)\n• ⚠️ At Risk: **${f.at_risk_count}** models (${f.pct_at_risk}%)\n• 🚨 Critical: **${f.critical_count}** models (${f.pct_critical}%)\n\nAverage health score: **${f.avg_health_score.toFixed(1)} / 100**\nOpen incidents: **${f.total_open_incidents}**\nTier changes today: **${f.tier_changes_today}**`;
  },
  'what are open incidents': () => {
    return `There are **${DEMO_DATA.incidents.length} open incidents**:\n\n${DEMO_DATA.incidents.map((i,n) =>
      `**${n+1}. ${i.model_name}** — ${i.severity.toUpperCase()}\n   Signals: ${i.trigger_signal_types}\n   Urgency: ${i.remediation_urgency}`
    ).join('\n\n')}`;
  },
  'which model costs the most': () => {
    const sorted = [...DEMO_DATA.models].filter(m => m.cost_component > 0).sort((a,b) => b.cost_component - a.cost_component);
    return `**Cost anomaly ranking** (by normalized cost component):\n\n${sorted.slice(0,4).map((m,i) =>
      `${i+1}. **${m.model_name}** — cost anomaly: ${(m.cost_component*100).toFixed(0)}%`
    ).join('\n')}\n\n⚠️ **${sorted[0].model_name}** has the highest cost anomaly at **${(sorted[0].cost_component*100).toFixed(0)}%** above baseline.`;
  },
  'default': (q) =>
    `I searched the **governance.model_health** schema for: "${q}"\n\nI can help you investigate:\n• Model health scores and tiers\n• Active incidents with root-cause narratives\n• Cost and usage anomalies\n• Upstream lineage changes\n• Feature drift signals\n\nTry asking: *"Which models are at risk?"*, *"Why is credit_risk at risk?"*, or *"Show me fleet health"*`,
};

function matchGenieResponse(query) {
  const q = query.toLowerCase().trim();
  if (q.includes('at risk') || q.includes('risky') || q.includes('critical')) return GENIE_RESPONSES['which models are at risk']();
  if (q.includes('credit') && (q.includes('why') || q.includes('risk'))) return GENIE_RESPONSES['why is credit_risk at risk']();
  if (q.includes('fleet') || q.includes('summary') || q.includes('overview')) return GENIE_RESPONSES['show me fleet health']();
  if (q.includes('incident') || q.includes('open')) return GENIE_RESPONSES['what are open incidents']();
  if (q.includes('cost') || q.includes('expensive') || q.includes('spend')) return GENIE_RESPONSES['which model costs the most']();
  return GENIE_RESPONSES['default'](query);
}

let typingTimeout = null;

function sendGenieMessage(query) {
  if (!query.trim()) return;
  const messages = $('#genie-messages');

  // User bubble
  const userMsg = document.createElement('div');
  userMsg.className = 'genie-message user';
  userMsg.innerHTML = `
    <div class="genie-avatar">👤</div>
    <div class="genie-bubble">${query}</div>`;
  messages.appendChild(userMsg);

  // Typing indicator
  const typing = document.createElement('div');
  typing.className = 'genie-message assistant';
  typing.id = 'genie-typing';
  typing.innerHTML = `
    <div class="genie-avatar">🧿</div>
    <div class="genie-bubble" style="color:var(--text-muted)">
      <span style="animation:pulse-dot 1s infinite">●</span>
      <span style="animation:pulse-dot 1s 0.2s infinite">●</span>
      <span style="animation:pulse-dot 1s 0.4s infinite">●</span>
    </div>`;
  messages.appendChild(typing);
  messages.scrollTop = messages.scrollHeight;

  setTimeout(() => {
    typing.remove();
    const response = matchGenieResponse(query);
    const botMsg = document.createElement('div');
    botMsg.className = 'genie-message assistant';
    // Convert markdown-ish bold and bullets
    const formatted = response
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\n/g, '<br>');
    botMsg.innerHTML = `
      <div class="genie-avatar">🧿</div>
      <div class="genie-bubble">${formatted}</div>`;
    messages.appendChild(botMsg);
    messages.scrollTop = messages.scrollHeight;
  }, 900 + Math.random() * 400);
}

// ─────────────────────────────────────────────
// Passport View
// ─────────────────────────────────────────────
function renderPassport(modelId) {
  const model = DEMO_DATA.models.find(m => m.model_id === modelId);
  if (!model) return;

  const hc = healthColor(model.health_tier);
  const bar = '█'.repeat(Math.round(model.health_score / 5)) + '░'.repeat(20 - Math.round(model.health_score / 5));
  const incidents = DEMO_DATA.incidents.filter(i => i.model_id === modelId);

  return `
    <div class="card" style="margin-bottom:14px">
      <div class="card-header">
        <span class="card-title">🧿 ${model.model_name} v${model.model_version}</span>
        <span class="tier-badge ${tierClass(model.criticality_tier)}">${tierLabel(model.criticality_tier)}</span>
      </div>
      <div style="padding:18px 20px">
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:16px">
          <div>
            <div class="drawer-section-title">Health Score</div>
            <div style="font-size:28px;font-weight:800;color:${hc}">${model.health_score.toFixed(1)}</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text-muted);margin-top:3px">[${bar}]</div>
          </div>
          <div>
            <div class="drawer-section-title">Health Tier</div>
            <span class="health-badge ${model.health_tier}" style="margin-top:6px;display:inline-flex">
              ${{ healthy:'✅ Healthy', watch:'👀 Watch', at_risk:'⚠️ At Risk', critical:'🚨 Critical' }[model.health_tier]}
            </span>
          </div>
          <div>
            <div class="drawer-section-title">Confidence</div>
            <div style="font-size:22px;font-weight:700;color:var(--accent-blue)">${(model.confidence*100).toFixed(0)}%</div>
          </div>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px">
          ${[
            ['Domain', model.business_domain || '—'],
            ['Team', model.owning_team || '—'],
            ['Endpoint', model.serving_endpoint || '—'],
            ['Gateway', model.gateway_registered ? '✓ Registered' : '✗ Not registered'],
            ['Monitor', model.lakehouse_monitor_configured !== false ? '✓ Configured' : '✗ Not configured'],
            ['Last Retrained', model.last_retrained_at || '—'],
          ].map(([k,v]) => `
            <div style="background:var(--bg-glass);border:1px solid var(--border);border-radius:8px;padding:10px 12px">
              <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">${k}</div>
              <div style="font-size:13px;font-weight:600">${v}</div>
            </div>`).join('')}
        </div>

        <div class="drawer-section-title">Open Incidents</div>
        ${incidents.length > 0
          ? incidents.map(i => `<div style="padding:10px;background:var(--critical-bg);border:1px solid rgba(239,68,68,.2);border-radius:8px;margin-bottom:8px;font-size:12px">
              <strong>${i.severity.toUpperCase()}:</strong> ${i.trigger_signal_types} — ${i.root_cause_narrative?.substring(0,120)}...</div>`).join('')
          : '<div style="color:var(--text-muted);font-size:13px">No active incidents.</div>'
        }
      </div>
    </div>`;
}

function renderAllPassports() {
  const container = $('#passport-container');
  // Show top 4 passports
  container.innerHTML = DEMO_DATA.models.slice(0, 4).map(m => renderPassport(m.model_id)).join('');
}

// ─────────────────────────────────────────────
// V2 Capabilities View
// ─────────────────────────────────────────────
function renderV2View() {
  // Already static HTML, just animate numbers
}

// ─────────────────────────────────────────────
// Real-time ticker simulation
// ─────────────────────────────────────────────
function startRealtimeTicker() {
  // Occasionally show "new signal" toasts
  const signals = [
    { msg: '📊 Drift signal updated for demand_forecaster — score: 0.31', type: 'info' },
    { msg: '💸 Cost normalized: prod-fraud-gateway within baseline', type: 'info' },
    { msg: '🛡️ Guardrail: 2 new prompt injection attempts blocked on fraud-gateway', type: 'info' },
    { msg: '✅ credit_risk_classifier acknowledged by risk_analytics team', type: 'success' },
  ];
  let idx = 0;
  setInterval(() => {
    if (Math.random() < 0.4) {
      showToast(signals[idx % signals.length].msg, signals[idx % signals.length].type);
      idx++;
    }
  }, 15000);
}

// ─────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Render all sections
  renderFleetOverview();
  renderLeaderboard();
  renderIncidents();
  renderPipeline();
  renderAllPassports();

  // Navigation
  $$('.nav-item').forEach(item => {
    item.addEventListener('click', () => navigate(item.getAttribute('data-section')));
  });

  // Leaderboard sort
  $$('#leaderboard-table th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.getAttribute('data-sort');
      if (sortColumn === col) { sortAsc = !sortAsc; } else { sortColumn = col; sortAsc = true; }
      $$('#leaderboard-table th').forEach(h => h.style.color = '');
      th.style.color = 'var(--text-brand)';
      renderLeaderboard();
    });
  });

  // Leaderboard tier filter
  $$('.tier-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('.tier-filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      filterTier = btn.getAttribute('data-tier');
      renderLeaderboard();
    });
  });

  // Run pipeline button
  $('#run-pipeline-btn').addEventListener('click', () => {
    navigate('pipeline');
    setTimeout(runPipelineDemo, 300);
  });

  // Drawer close
  $('#drawer-close-btn').addEventListener('click', closeModelDrawer);
  $('#drawer-overlay').addEventListener('click', closeModelDrawer);

  // Genie
  const genieInput = $('#genie-input');
  const genieSend  = $('#genie-send');

  genieSend.addEventListener('click', () => {
    const q = genieInput.value.trim();
    if (!q) return;
    sendGenieMessage(q);
    genieInput.value = '';
  });

  genieInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      genieSend.click();
    }
  });

  $$('.genie-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      genieInput.value = chip.textContent;
      genieSend.click();
    });
  });

  // Real-time ticker
  startRealtimeTicker();

  // Start on fleet section
  navigate('fleet');

  // Welcome toast
  setTimeout(() => showToast('🧿 Third Eye loaded — 12 models scored, 3 incidents open.', 'info'), 800);
});
