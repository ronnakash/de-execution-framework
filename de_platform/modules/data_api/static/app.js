/* Fraud Detection Dashboard — client-side logic */
'use strict';

// ── Tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(`tab-${btn.dataset.tab}`).classList.add('active');
  });
});

// ── Utilities ─────────────────────────────────────────────────────────────────
function el(id) { return document.getElementById(id); }

function buildQuery(params) {
  const parts = Object.entries(params)
    .filter(([, v]) => v !== '' && v != null)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
  return parts.length ? `?${parts.join('&')}` : '';
}

function escapeHtml(val) {
  return String(val ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function severityBadge(sev) {
  const cls = `severity-${(sev || 'unknown').toLowerCase()}`;
  return `<span class="badge ${cls}">${escapeHtml(sev || 'unknown')}</span>`;
}

function showError(containerId, msg) {
  const div = document.createElement('div');
  div.className = 'error-msg';
  div.textContent = msg;
  const container = el(containerId);
  const existing = container.querySelector('.error-msg');
  if (existing) existing.remove();
  container.prepend(div);
  setTimeout(() => div.remove(), 5000);
}

// ── Alerts Dashboard ──────────────────────────────────────────────────────────
async function fetchAlerts() {
  const params = {
    tenant_id: el('alerts-tenant').value.trim(),
    severity:  el('alerts-severity').value,
    limit:     el('alerts-limit').value || 100,
    offset:    el('alerts-offset').value || 0,
  };
  try {
    const resp = await fetch(`/api/v1/alerts${buildQuery(params)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    renderAlerts(await resp.json());
  } catch (err) {
    showError('tab-alerts', `Failed to load alerts: ${err.message}`);
  }
}

function renderAlerts(alerts) {
  const tbody = el('alerts-body');
  const empty = el('alerts-empty');
  tbody.innerHTML = '';
  if (!alerts.length) { empty.classList.remove('hidden'); return; }
  empty.classList.add('hidden');
  tbody.innerHTML = alerts.map(a => `
    <tr>
      <td>${severityBadge(a.severity)}</td>
      <td>${escapeHtml(a.tenant_id)}</td>
      <td>${escapeHtml(a.algorithm)}</td>
      <td>${escapeHtml(a.event_type)}</td>
      <td>${escapeHtml(a.event_id)}</td>
      <td style="max-width:280px;white-space:normal">${escapeHtml(a.description)}</td>
      <td>${escapeHtml((a.created_at || '').slice(0, 19))}</td>
    </tr>`).join('');
}

el('alerts-search').addEventListener('click', fetchAlerts);
el('alerts-refresh').addEventListener('click', fetchAlerts);

// ── Events Explorer ───────────────────────────────────────────────────────────
const EVENT_COLUMNS = {
  orders: [
    'id', 'tenant_id', 'symbol', 'side', 'quantity', 'price',
    'notional_usd', 'order_type', 'status', 'transact_time',
  ],
  executions: [
    'id', 'tenant_id', 'order_id', 'symbol', 'side',
    'quantity', 'price', 'notional_usd', 'execution_venue', 'status', 'transact_time',
  ],
  transactions: [
    'id', 'tenant_id', 'account_id', 'counterparty_id',
    'amount', 'amount_usd', 'currency', 'transaction_type', 'status', 'transact_time',
  ],
};

async function fetchEvents() {
  const type = el('events-type').value;
  const params = {
    tenant_id: el('events-tenant').value.trim(),
    date:      el('events-date').value,
    limit:     el('events-limit').value || 100,
  };
  try {
    const resp = await fetch(`/api/v1/events/${type}${buildQuery(params)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    renderEvents(type, await resp.json());
  } catch (err) {
    showError('tab-events', `Failed to load events: ${err.message}`);
  }
}

function renderEvents(type, events) {
  const cols  = EVENT_COLUMNS[type] || [];
  const thead = el('events-thead');
  const tbody = el('events-body');
  const empty = el('events-empty');

  thead.innerHTML = `<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>`;
  tbody.innerHTML = '';

  if (!events.length) { empty.classList.remove('hidden'); return; }
  empty.classList.add('hidden');
  tbody.innerHTML = events.map(ev =>
    `<tr>${cols.map(c => `<td>${escapeHtml(ev[c])}</td>`).join('')}</tr>`
  ).join('');
}

el('events-search').addEventListener('click', fetchEvents);

// ── Initial load ──────────────────────────────────────────────────────────────
fetchAlerts();
