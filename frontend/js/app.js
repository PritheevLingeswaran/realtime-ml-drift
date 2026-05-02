const API = window.location.origin;
const POLL_MS = 2000;

const state = {
  health: {},
  sysState: {},
  alerts: [],
  history: { scores: [], thresholds: [], driftScores: [], timestamps: [] },
  maxHistory: 60,
  startTime: Date.now(),
  eventsProcessed: 0,
};

// ===== DOM HELPERS =====
const $ = (s) => document.querySelector(s);
const $$ = (s) => document.querySelectorAll(s);

function formatTime(unix) {
  if (!unix) return '—';
  const d = new Date(unix * 1000);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function timeAgo(unix) {
  if (!unix) return '—';
  const s = Math.floor(Date.now() / 1000 - unix);
  if (s < 2) return 'just now';
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  return Math.floor(s / 3600) + 'h ago';
}

// ===== CHART SETUP =====
let driftChart, latencyChart;

function initCharts() {
  const driftCtx = document.getElementById('driftChart');
  if (!driftCtx) return;

  Chart.defaults.color = '#8b95a8';
  Chart.defaults.font.family = "'Space Grotesk', monospace";
  Chart.defaults.font.size = 11;

  driftChart = new Chart(driftCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        {
          label: 'Anomaly Rate',
          data: [],
          borderColor: '#00f0ff',
          backgroundColor: 'rgba(0, 240, 255, 0.05)',
          borderWidth: 2,
          fill: true,
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4,
        },
        {
          label: 'Threshold',
          data: [],
          borderColor: '#ff2d95',
          backgroundColor: 'rgba(255, 45, 149, 0.03)',
          borderWidth: 2,
          borderDash: [6, 4],
          fill: false,
          tension: 0,
          pointRadius: 0,
        },
        {
          label: 'Drift Score',
          data: [],
          borderColor: '#00e676',
          backgroundColor: 'rgba(0, 230, 118, 0.05)',
          borderWidth: 1.5,
          fill: true,
          tension: 0.4,
          pointRadius: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'top',
          align: 'end',
          labels: { boxWidth: 12, padding: 16, usePointStyle: true, pointStyle: 'circle' },
        },
        tooltip: {
          backgroundColor: 'rgba(10, 14, 26, 0.95)',
          borderColor: 'rgba(255,255,255,0.1)',
          borderWidth: 1,
          padding: 12,
          titleFont: { family: "'Space Grotesk'" },
          bodyFont: { family: "'Space Grotesk'" },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(255,255,255,0.03)', drawBorder: false },
          ticks: { maxTicksLimit: 8 },
        },
        y: {
          grid: { color: 'rgba(255,255,255,0.03)', drawBorder: false },
          min: 0,
          ticks: { maxTicksLimit: 6 },
        },
      },
    },
  });
}

function updateChart() {
  if (!driftChart) return;
  const h = state.history;
  driftChart.data.labels = h.timestamps;
  driftChart.data.datasets[0].data = h.scores;
  driftChart.data.datasets[1].data = h.thresholds;
  driftChart.data.datasets[2].data = h.driftScores;
  driftChart.update('none');
}

function pushHistory(score, threshold, driftScore) {
  const h = state.history;
  const now = new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  h.timestamps.push(now);
  h.scores.push(score);
  h.thresholds.push(threshold);
  h.driftScores.push(Math.min(driftScore, 3));
  if (h.timestamps.length > state.maxHistory) {
    h.timestamps.shift();
    h.scores.shift();
    h.thresholds.shift();
    h.driftScores.shift();
  }
}

// ===== API CALLS =====
async function fetchJSON(path) {
  try {
    const r = await fetch(API + path);
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } catch (e) {
    console.warn('Fetch error:', path, e.message);
    return null;
  }
}

async function pollHealth() {
  const d = await fetchJSON('/health');
  if (!d) return;
  state.health = d;
  const statusDot = $('#systemStatusDot');
  const statusText = $('#systemStatusText');
  if (d.status === 'ok' && d.model_ready) {
    statusDot.className = 'status-dot';
    statusText.textContent = 'All Systems Operational';
  } else if (d.status === 'ok') {
    statusDot.className = 'status-dot warning';
    statusText.textContent = 'Model Warming Up';
  } else {
    statusDot.className = 'status-dot error';
    statusText.textContent = 'System Issue';
  }
  $('#envBadge').textContent = d.env || 'dev';
}

async function pollState() {
  const d = await fetchJSON('/state');
  if (!d) return;
  state.sysState = d;

  // Hero metrics
  const score = d.anomaly_rate || 0;
  $('#metricScore').textContent = score.toFixed(4);
  $('#metricThreshold').textContent = (d.threshold || 0).toFixed(4);

  // Drift status
  const driftEl = $('#driftStatus');
  if (d.drift_active) {
    driftEl.innerHTML = '<span class="drift-badge active">⚠ DRIFT ACTIVE</span>';
  } else if (d.drift_warning_active) {
    driftEl.innerHTML = '<span class="drift-badge warning">⚡ WARNING</span>';
  } else {
    driftEl.innerHTML = '<span class="drift-badge stable">✓ STABLE</span>';
  }

  // Lag
  const lagMs = ((d.processing_lag_seconds || 0) * 1000).toFixed(1);
  const lagP50Ms = ((d.processing_lag_p50_seconds ?? d.processing_lag_seconds ?? 0) * 1000).toFixed(1);
  const lagP95Ms = ((d.processing_lag_p95_seconds || 0) * 1000).toFixed(1);
  const maxLagMs = ((d.max_processing_lag_seconds || 0) * 1000).toFixed(1);
  $('#metricLag').textContent = lagMs + 'ms';

  // Latency bars
  updateLatencyBar('lagP50', lagP50Ms, 50);
  updateLatencyBar('lagP95', lagP95Ms, 50);
  updateLatencyBar('lagP99', maxLagMs, 100);
  $('#lagP50Val').textContent = lagP50Ms + ' ms';
  $('#lagP95Val').textContent = lagP95Ms + ' ms';
  $('#lagP99Val').textContent = maxLagMs + ' ms';

  // Queue
  $('#queueDepth').textContent = Math.floor(d.queue_depth || 0);
  $('#queueP95').textContent = Math.floor(d.queue_depth_p95 || 0);
  $('#droppedEvents').textContent = (d.dropped_events_total || 0).toLocaleString();
  $('#duplicateEvents').textContent = (d.duplicate_events_total || 0).toLocaleString();

  // Adaptation
  $('#adaptFrozen').textContent = d.adaptation_frozen ? 'FROZEN' : 'ACTIVE';
  $('#adaptFrozen').className = 'score-pill ' + (d.adaptation_frozen ? 'warn' : 'ok');
  if (d.adaptation_freeze_reason) {
    $('#freezeReason').textContent = d.adaptation_freeze_reason;
  }

  // Drift score
  const ds = d.drift_score || 0;
  $('#driftScoreVal').textContent = ds.toFixed(3);

  // Push to chart
  pushHistory(score, d.threshold || 0, ds);
  updateChart();

  // Bottom bar
  $('#lastSnapshot').textContent = d.last_snapshot_unix ? timeAgo(d.last_snapshot_unix) : '—';
  $('#modelReady').textContent = d.model_ready ? 'Ready' : 'Warming Up';
  const uptime = Math.floor((Date.now() - state.startTime) / 1000);
  const mins = Math.floor(uptime / 60);
  const secs = uptime % 60;
  $('#uptime').textContent = mins + 'm ' + secs + 's';
  $('#dropRate').textContent = ((d.drop_rate || 0) * 100).toFixed(2) + '%';
}

async function pollAlerts() {
  const d = await fetchJSON('/alerts?limit=50');
  if (!d) return;
  state.alerts = d;
  renderAlerts(d);
  $('#alertCount').textContent = d.length;
}

function updateLatencyBar(id, val, max) {
  const el = document.getElementById(id);
  if (el) {
    const pct = Math.min(100, (parseFloat(val) / max) * 100);
    el.style.width = pct + '%';
  }
}

function renderAlerts(alerts) {
  const container = $('#alertsList');
  if (!alerts || alerts.length === 0) {
    container.innerHTML = `<div class="empty-state">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M9 12l2 2 4-4"/><circle cx="12" cy="12" r="10"/></svg>
      <span>No alerts — system healthy</span>
    </div>`;
    return;
  }

  container.innerHTML = alerts.slice(0, 25).map((a) => `
    <div class="alert-item">
      <div class="alert-severity ${a.severity}"></div>
      <div class="alert-content">
        <div class="alert-title">${escapeHtml(a.reason)} — ${escapeHtml(a.entity_id)}</div>
        <div class="alert-meta">
          <span class="score-pill ${a.severity === 'critical' || a.severity === 'high' ? 'bad' : a.severity === 'medium' ? 'warn' : 'ok'}">${a.severity.toUpperCase()}</span>
          <span>Score: ${a.score.toFixed(4)}</span>
          <span>Event: ${a.event_id.substring(0, 12)}…</span>
        </div>
      </div>
      <div class="alert-time">${formatTime(a.ts)}</div>
    </div>
  `).join('');
}

function escapeHtml(str) {
  const d = document.createElement('div');
  d.textContent = str || '';
  return d.innerHTML;
}

// ===== SEND TEST EVENT =====
async function sendTestEvent() {
  const btn = $('#scoreBtn');
  btn.textContent = 'Sending…';
  btn.disabled = true;

  const event = {
    event_id: 'test_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
    ts: Date.now() / 1000,
    entity_id: 'acct_' + String(Math.floor(Math.random() * 1000)).padStart(6, '0'),
    amount: Math.round((Math.random() * 500 + 10) * 100) / 100,
    merchant_id: 'm_' + String(Math.floor(Math.random() * 100)).padStart(4, '0'),
    merchant_category: ['electronics', 'grocery', 'travel', 'dining', 'gaming'][Math.floor(Math.random() * 5)],
    country: ['US', 'GB', 'DE', 'JP', 'IN'][Math.floor(Math.random() * 5)],
    channel: ['web', 'mobile', 'pos'][Math.floor(Math.random() * 3)],
    device_type: ['desktop', 'mobile', 'tablet'][Math.floor(Math.random() * 3)],
  };

  try {
    const r = await fetch(API + '/detect_drift', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event),
    });
    const data = await r.json();
    const resultEl = $('#scoreResult');
    if (data.status === 'warming_up') {
      resultEl.innerHTML = `<span style="color:var(--amber)">⏳ Model warming up…</span>`;
    } else {
      const color = data.is_anomaly ? 'var(--magenta)' : 'var(--emerald)';
      resultEl.innerHTML = `<span style="color:${color}">${data.is_anomaly ? '🚨 ANOMALY' : '✓ Normal'}</span> &nbsp; Score: <span style="color:var(--cyan)">${data.score.toFixed(4)}</span> &nbsp; Drift: ${data.drift_active ? '<span style="color:var(--magenta)">Active</span>' : '<span style="color:var(--emerald)">None</span>'}`;
    }
  } catch (e) {
    $('#scoreResult').innerHTML = `<span style="color:var(--red)">Error: ${e.message}</span>`;
  }

  btn.textContent = 'Send Test Event';
  btn.disabled = false;
}

// ===== INIT =====
async function init() {
  initCharts();
  await Promise.all([pollHealth(), pollState(), pollAlerts()]);
  setInterval(pollHealth, 5000);
  setInterval(pollState, POLL_MS);
  setInterval(pollAlerts, 4000);
}

document.addEventListener('DOMContentLoaded', init);
