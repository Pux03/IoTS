const dashboardUrl = '/api/dashboard';
const refreshIntervalMs = 5000;

const nodes = {
  totalEvents: document.getElementById('total-events'),
  grantedAccesses: document.getElementById('granted-accesses'),
  unauthorizedAccesses: document.getElementById('unauthorized-accesses'),
  activeDevices: document.getElementById('active-devices'),
  statusDot: document.getElementById('status-dot'),
  statusText: document.getElementById('status-text'),
  lastUpdated: document.getElementById('last-updated'),
  eventsTable: document.getElementById('events-table'),
  alertsTable: document.getElementById('alerts-table'),
  timelineChart: document.getElementById('timeline-chart'),
  accessChart: document.getElementById('access-chart'),
  zoneChart: document.getElementById('zone-chart'),
};

function formatTime(value) {
  if (!value) {
    return 'n/a';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatFullDate(value) {
  if (!value) {
    return 'No data yet';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return `Updated ${date.toLocaleString()}`;
}

function createCell(content) {
  const td = document.createElement('td');
  if (content instanceof Node) {
    td.appendChild(content);
  } else {
    td.textContent = content;
  }
  return td;
}

function createPill(label, variant) {
  const span = document.createElement('span');
  span.className = `pill ${variant}`;
  span.textContent = label;
  return span;
}

function fillTable(tbody, rows, columns, emptyText) {
  tbody.innerHTML = '';

  if (!rows.length) {
    const tr = document.createElement('tr');
    tr.className = 'empty-row';
    const td = document.createElement('td');
    td.colSpan = columns.length;
    td.textContent = emptyText;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement('tr');
    columns.forEach((column) => {
      tr.appendChild(createCell(column(row)));
    });
    tbody.appendChild(tr);
  });
}

function prepareCanvas(canvas) {
  const context = canvas.getContext('2d');
  context.clearRect(0, 0, canvas.width, canvas.height);
  context.fillStyle = '#fffdf8';
  context.fillRect(0, 0, canvas.width, canvas.height);
  return context;
}

function drawAxes(context, width, height, left, right, top, bottom) {
  context.strokeStyle = 'rgba(31, 42, 46, 0.14)';
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(left, top);
  context.lineTo(left, height - bottom);
  context.lineTo(width - right, height - bottom);
  context.stroke();
}

function drawTimelineChart(series) {
  const canvas = nodes.timelineChart;
  const context = prepareCanvas(canvas);
  const { width, height } = canvas;
  const padding = { left: 44, right: 18, top: 22, bottom: 30 };

  drawAxes(context, width, height, padding.left, padding.right, padding.top, padding.bottom);

  if (!series.length) {
    context.fillStyle = '#657176';
    context.fillText('Waiting for RFID events...', 48, height / 2);
    return;
  }

  const maxValue = Math.max(...series.map((item) => item.total), 1);
  const stepX = series.length === 1
    ? 0
    : (width - padding.left - padding.right) / (series.length - 1);

  context.strokeStyle = '#0f766e';
  context.lineWidth = 3;
  context.beginPath();

  series.forEach((item, index) => {
    const x = padding.left + (index * stepX);
    const y = height - padding.bottom - ((item.total / maxValue) * (height - padding.top - padding.bottom));
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();

  context.fillStyle = 'rgba(15, 118, 110, 0.14)';
  context.beginPath();
  series.forEach((item, index) => {
    const x = padding.left + (index * stepX);
    const y = height - padding.bottom - ((item.total / maxValue) * (height - padding.top - padding.bottom));
    if (index === 0) {
      context.moveTo(x, height - padding.bottom);
      context.lineTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.lineTo(padding.left + ((series.length - 1) * stepX), height - padding.bottom);
  context.closePath();
  context.fill();

  context.fillStyle = '#1f2a2e';
  context.font = '12px Trebuchet MS';
  series.slice(-6).forEach((item, index) => {
    const actualIndex = series.length - Math.min(series.length, 6) + index;
    const x = padding.left + (actualIndex * stepX);
    context.fillText(formatTime(item.bucket), x - 18, height - 10);
  });
}

function drawAccessChart(breakdown) {
  const canvas = nodes.accessChart;
  const context = prepareCanvas(canvas);
  const { width, height } = canvas;
  const granted = breakdown.granted || 0;
  const unauthorized = breakdown.unauthorized || 0;
  const total = Math.max(granted + unauthorized, 1);

  drawAxes(context, width, height, 56, 18, 26, 34);

  const chartHeight = height - 74;
  const barWidth = 78;
  const gap = 86;
  const baseY = height - 34;
  const leftX = 100;
  const rightX = leftX + barWidth + gap;

  const bars = [
    { x: leftX, value: granted, label: 'Granted', color: '#0f766e' },
    { x: rightX, value: unauthorized, label: 'Unauthorized', color: '#d97706' },
  ];

  bars.forEach((bar) => {
    const barHeight = (bar.value / total) * chartHeight;
    context.fillStyle = bar.color;
    context.fillRect(bar.x, baseY - barHeight, barWidth, barHeight);
    context.fillStyle = '#1f2a2e';
    context.font = 'bold 14px Trebuchet MS';
    context.fillText(String(bar.value), bar.x + 20, baseY - barHeight - 10);
    context.font = '12px Trebuchet MS';
    context.fillText(bar.label, bar.x + 4, baseY + 18);
  });
}

function drawZoneChart(zones) {
  const canvas = nodes.zoneChart;
  const context = prepareCanvas(canvas);
  const { width, height } = canvas;
  const data = zones.slice(0, 5);

  drawAxes(context, width, height, 40, 20, 22, 28);

  if (!data.length) {
    context.fillStyle = '#657176';
    context.fillText('No zone distribution yet.', 44, height / 2);
    return;
  }

  const maxValue = Math.max(...data.map((item) => item.count), 1);
  const chartWidth = width - 60;
  const chartHeight = height - 58;
  const barGap = 12;
  const barWidth = (chartWidth - (barGap * (data.length - 1))) / data.length;

  data.forEach((item, index) => {
    const x = 40 + index * (barWidth + barGap);
    const barHeight = (item.count / maxValue) * (chartHeight - 20);
    const y = height - 28 - barHeight;

    context.fillStyle = index % 2 === 0 ? '#0f766e' : '#f4bb5e';
    context.fillRect(x, y, barWidth, barHeight);
    context.fillStyle = '#1f2a2e';
    context.font = '11px Trebuchet MS';
    context.fillText(item.zone.replace('_', ' '), x, height - 10, barWidth + 4);
    context.fillText(String(item.count), x + 6, y - 8);
  });
}

function renderDashboard(payload) {
  const summary = payload.summary || {};
  const charts = payload.charts || {};
  const recentEvents = payload.recent_events || [];
  const recentAlerts = payload.recent_alerts || [];

  nodes.totalEvents.textContent = summary.total_events ?? 0;
  nodes.grantedAccesses.textContent = summary.granted_accesses ?? 0;
  nodes.unauthorizedAccesses.textContent = summary.unauthorized_accesses ?? 0;
  nodes.activeDevices.textContent = summary.active_devices ?? 0;
  nodes.statusDot.classList.add('live');
  nodes.statusText.textContent = 'Analytics service online';
  nodes.lastUpdated.textContent = formatFullDate(payload.meta?.last_updated_at);

  fillTable(
    nodes.eventsTable,
    recentEvents,
    [
      (row) => formatTime(row.timestamp),
      (row) => row.device_id,
      (row) => row.card_uid,
      (row) => row.zone,
      (row) => row.event_type,
      (row) => createPill(row.access_status, row.access_granted ? 'granted' : 'denied'),
    ],
    'No RFID events received yet.'
  );

  fillTable(
    nodes.alertsTable,
    recentAlerts,
    [
      (row) => formatTime(row.timestamp),
      (row) => row.device_id,
      (row) => row.zone,
      (row) => row.card_uid,
      (row) => createPill(row.risk_level, String(row.risk_level || '').toLowerCase()),
    ],
    'No unauthorized access alerts yet.'
  );

  drawTimelineChart(charts.events_over_time || []);
  drawAccessChart(charts.access_breakdown || {});
  drawZoneChart(charts.events_by_zone || []);
}

async function loadDashboard() {
  try {
    const response = await fetch(dashboardUrl, { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Analytics API returned ${response.status}`);
    }

    const payload = await response.json();
    renderDashboard(payload);
  } catch (error) {
    nodes.statusDot.classList.remove('live');
    nodes.statusText.textContent = 'Analytics unavailable';
    nodes.lastUpdated.textContent = error.message;
  }
}

loadDashboard();
window.setInterval(loadDashboard, refreshIntervalMs);
