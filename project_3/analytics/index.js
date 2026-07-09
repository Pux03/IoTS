const express = require('express');
const mqtt = require('mqtt');
const client = require('prom-client');

const MQTT_HOST = process.env.MQTT_HOST || 'mqtt-broker';
const MQTT_PORT = process.env.MQTT_PORT || '1883';
const MQTT_QOS = Number.parseInt(process.env.MQTT_QOS || '1', 10);
const MQTT_EVENTS_TOPIC = process.env.MQTT_EVENTS_TOPIC || 'rfid/events';
const MQTT_ALERTS_TOPIC = process.env.MQTT_ALERTS_TOPIC || 'rfid/alerts';
const MQTT_ANALYTICS_TOPIC = process.env.MQTT_ANALYTICS_TOPIC || 'rfid/analytics';
const MAAS_URL = process.env.MAAS_URL || 'http://maas:8003/predict';
const RECENT_EVENTS_LIMIT = Number.parseInt(process.env.RECENT_EVENTS_LIMIT || '40', 10);
const RECENT_ALERTS_LIMIT = Number.parseInt(process.env.RECENT_ALERTS_LIMIT || '25', 10);
const TIMESERIES_RETENTION_MINUTES = Number.parseInt(process.env.TIMESERIES_RETENTION_MINUTES || '60', 10);
const PORT = process.env.PORT || 8002;

const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ register: client.register });

const eventsReceived = new client.Counter({
  name: 'analytics_rfid_events_received_total',
  help: 'Total number of RFID events received by analytics.',
});

const alertsReceived = new client.Counter({
  name: 'analytics_alerts_received_total',
  help: 'Total number of unauthorized access alerts received by analytics.',
});

const payloadErrors = new client.Counter({
  name: 'analytics_payload_errors_total',
  help: 'Total number of payload parsing errors.',
});

const maasRequests = new client.Counter({
  name: 'analytics_maas_requests_total',
  help: 'Total number of MaaS prediction requests.',
});

const maasErrors = new client.Counter({
  name: 'analytics_maas_errors_total',
  help: 'Total number of MaaS request failures.',
});

const analyticsPublishes = new client.Counter({
  name: 'analytics_mqtt_publishes_total',
  help: 'Total number of analytics snapshots published to MQTT.',
});

const analyticsPublishErrors = new client.Counter({
  name: 'analytics_mqtt_publish_errors_total',
  help: 'Total number of analytics snapshot publish failures.',
});

const state = {
  brokerReady: false,
  lastBrokerError: null,
  lastMaasError: null,
  lastUpdatedAt: null,
  totalEvents: 0,
  grantedAccesses: 0,
  unauthorizedAccesses: 0,
  totalAlerts: 0,
  recentEvents: [],
  recentAlerts: [],
  eventsByDevice: {},
  eventsByZone: {},
  failedAttemptsByCard: {},
  activeDevices: new Set(),
  recentEventIndex: new Map(),
  timeBuckets: new Map(),
};

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function incrementCounter(bucket, key) {
  bucket[key] = (bucket[key] || 0) + 1;
}

function pushLimited(list, item, limit) {
  list.unshift(item);
  if (list.length > limit) {
    list.length = limit;
  }
}

function correlationKey(data) {
  return `${data.timestamp || ''}|${data.device_id || ''}|${data.card_uid || ''}`;
}

function parsePayload(payloadString) {
  let parsed = JSON.parse(payloadString);

  if (Array.isArray(parsed) && parsed.length > 0) {
    parsed = parsed[0];
  }

  if (parsed && typeof parsed.result === 'string') {
    try {
      parsed = JSON.parse(parsed.result);
    } catch (error) {
      return parsed;
    }
  }

  return parsed;
}

function normalizeEvent(raw) {
  const accessGranted = raw.access_granted === true || raw.access_granted === 'true';
  return {
    event_id: String(raw.event_id || `${raw.device_id || 'device'}-${Date.now()}`),
    timestamp: String(raw.timestamp || new Date().toISOString()),
    device_id: String(raw.device_id || 'UNKNOWN_DEVICE'),
    card_uid: String(raw.card_uid || 'UNKNOWN_CARD'),
    access_granted: accessGranted,
    door_id: String(raw.door_id || 'UNKNOWN_DOOR'),
    zone: String(raw.zone || 'UNKNOWN_ZONE'),
    signal_strength: toNumber(raw.signal_strength, -65),
    battery_voltage: toNumber(raw.battery_voltage, 3.7),
    response_time_ms: toNumber(raw.response_time_ms, 80),
    event_type: String(raw.event_type || (accessGranted ? 'ENTRY' : 'ACCESS_DENIED')),
  };
}

function normalizeAlert(raw) {
  return {
    alert: String(raw.alert || 'UNAUTHORIZED_ACCESS'),
    timestamp: String(raw.timestamp || new Date().toISOString()),
    device_id: String(raw.device_id || 'UNKNOWN_DEVICE'),
    door_id: String(raw.door_id || 'UNKNOWN_DOOR'),
    zone: String(raw.zone || 'UNKNOWN_ZONE'),
    card_uid: String(raw.card_uid || 'UNKNOWN_CARD'),
  };
}

function upsertTimeBucket(event) {
  const eventDate = new Date(event.timestamp);
  const bucketDate = Number.isNaN(eventDate.getTime()) ? new Date() : eventDate;
  bucketDate.setUTCSeconds(0, 0);
  const bucketKey = bucketDate.toISOString();

  const currentBucket = state.timeBuckets.get(bucketKey) || {
    bucket: bucketKey,
    total: 0,
    granted: 0,
    unauthorized: 0,
  };

  currentBucket.total += 1;
  if (event.access_granted) {
    currentBucket.granted += 1;
  } else {
    currentBucket.unauthorized += 1;
  }

  state.timeBuckets.set(bucketKey, currentBucket);

  const cutoff = Date.now() - (TIMESERIES_RETENTION_MINUTES * 60 * 1000);
  for (const [key] of state.timeBuckets) {
    if (Date.parse(key) < cutoff) {
      state.timeBuckets.delete(key);
    }
  }
}

function rememberEvent(event) {
  state.recentEventIndex.set(correlationKey(event), event);

  if (state.recentEventIndex.size <= RECENT_EVENTS_LIMIT * 4) {
    return;
  }

  const allowedKeys = new Set(state.recentEvents.map((item) => correlationKey(item)));
  for (const key of Array.from(state.recentEventIndex.keys())) {
    if (!allowedKeys.has(key)) {
      state.recentEventIndex.delete(key);
    }
  }
}

function buildSummary() {
  return {
    total_events: state.totalEvents,
    granted_accesses: state.grantedAccesses,
    unauthorized_accesses: state.unauthorizedAccesses,
    active_devices: state.activeDevices.size,
    total_alerts: state.totalAlerts,
  };
}

function buildCharts() {
  return {
    events_over_time: Array.from(state.timeBuckets.values()).sort(
      (left, right) => Date.parse(left.bucket) - Date.parse(right.bucket)
    ),
    access_breakdown: {
      granted: state.grantedAccesses,
      unauthorized: state.unauthorizedAccesses,
    },
    events_by_zone: Object.entries(state.eventsByZone)
      .map(([zone, count]) => ({ zone, count }))
      .sort((left, right) => right.count - left.count),
    events_by_device: Object.entries(state.eventsByDevice)
      .map(([device_id, count]) => ({ device_id, count }))
      .sort((left, right) => right.count - left.count),
  };
}

function buildDashboardPayload() {
  const charts = buildCharts();
  return {
    summary: buildSummary(),
    charts,
    stats: {
      events_by_device: charts.events_by_device,
      events_by_zone: charts.events_by_zone,
    },
    recent_events: state.recentEvents,
    recent_alerts: state.recentAlerts,
    meta: {
      last_updated_at: state.lastUpdatedAt,
      maas_url: MAAS_URL,
      events_topic: MQTT_EVENTS_TOPIC,
      alerts_topic: MQTT_ALERTS_TOPIC,
      analytics_topic: MQTT_ANALYTICS_TOPIC,
    },
  };
}

function publishAnalyticsSnapshot(trigger) {
  if (!state.brokerReady) {
    return;
  }

  const snapshot = {
    type: 'RFID_ANALYTICS_SUMMARY',
    trigger,
    generated_at: new Date().toISOString(),
    summary: buildSummary(),
    latest_alert: state.recentAlerts[0] || null,
  };

  mqttClient.publish(
    MQTT_ANALYTICS_TOPIC,
    JSON.stringify(snapshot),
    { qos: MQTT_QOS },
    (error) => {
      if (error) {
        analyticsPublishErrors.inc();
        console.error('Failed to publish analytics snapshot:', error.message);
        return;
      }
      analyticsPublishes.inc();
    }
  );
}

async function requestRiskLevel(alert, matchedEvent) {
  const payload = {
    signal_strength: matchedEvent ? matchedEvent.signal_strength : -65,
    response_time_ms: matchedEvent ? matchedEvent.response_time_ms : 80,
    battery_voltage: matchedEvent ? matchedEvent.battery_voltage : 3.7,
    zone: alert.zone,
    door_id: alert.door_id,
    timestamp: alert.timestamp,
    previous_failed_attempts: matchedEvent ? matchedEvent.previous_failed_attempts || 0 : 0,
  };

  maasRequests.inc();

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const response = await fetch(MAAS_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(`MaaS returned HTTP ${response.status}`);
      }

      const result = await response.json();
      state.lastMaasError = null;
      return String(result.risk_level || 'MEDIUM');
    } catch (error) {
      state.lastMaasError = error.message;
      if (attempt === 3) {
        maasErrors.inc();
        console.error('MaaS request failed:', error.message);
        return 'UNKNOWN';
      }
      await delay(500 * attempt);
    }
  }

  return 'UNKNOWN';
}

async function findMatchingEvent(alert) {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const directMatch = state.recentEventIndex.get(correlationKey(alert));
    if (directMatch) {
      return directMatch;
    }

    if (attempt < 2) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  return state.recentEvents.find((event) =>
    event.timestamp === alert.timestamp &&
    event.device_id === alert.device_id &&
    event.card_uid === alert.card_uid
  ) || null;
}

async function handleEventMessage(payloadString) {
  try {
    const event = normalizeEvent(parsePayload(payloadString));
    eventsReceived.inc();

    event.previous_failed_attempts = state.failedAttemptsByCard[event.card_uid] || 0;

    state.totalEvents += 1;
    state.activeDevices.add(event.device_id);
    incrementCounter(state.eventsByDevice, event.device_id);
    incrementCounter(state.eventsByZone, event.zone);
    upsertTimeBucket(event);

    if (event.access_granted) {
      state.grantedAccesses += 1;
    } else {
      state.unauthorizedAccesses += 1;
      state.failedAttemptsByCard[event.card_uid] = event.previous_failed_attempts + 1;
    }

    pushLimited(state.recentEvents, {
      event_id: event.event_id,
      timestamp: event.timestamp,
      device_id: event.device_id,
      card_uid: event.card_uid,
      zone: event.zone,
      door_id: event.door_id,
      event_type: event.event_type,
      access_status: event.access_granted ? 'GRANTED' : 'DENIED',
      access_granted: event.access_granted,
      previous_failed_attempts: event.previous_failed_attempts,
      signal_strength: event.signal_strength,
      battery_voltage: event.battery_voltage,
      response_time_ms: event.response_time_ms,
    }, RECENT_EVENTS_LIMIT);

    rememberEvent(event);
    state.lastUpdatedAt = new Date().toISOString();
    publishAnalyticsSnapshot('event');
  } catch (error) {
    payloadErrors.inc();
    console.error('Failed to process RFID event:', error.message);
  }
}

async function handleAlertMessage(payloadString) {
  try {
    const alert = normalizeAlert(parsePayload(payloadString));
    alertsReceived.inc();
    state.totalAlerts += 1;

    const matchedEvent = await findMatchingEvent(alert);
    const riskLevel = await requestRiskLevel(alert, matchedEvent);

    pushLimited(state.recentAlerts, {
      alert: alert.alert,
      timestamp: alert.timestamp,
      device_id: alert.device_id,
      zone: alert.zone,
      door_id: alert.door_id,
      card_uid: alert.card_uid,
      risk_level: riskLevel,
    }, RECENT_ALERTS_LIMIT);

    state.lastUpdatedAt = new Date().toISOString();
    publishAnalyticsSnapshot('alert');
  } catch (error) {
    payloadErrors.inc();
    console.error('Failed to process RFID alert:', error.message);
  }
}

const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}:${MQTT_PORT}`, {
  reconnectPeriod: 1000,
});

mqttClient.on('connect', () => {
  state.brokerReady = true;
  state.lastBrokerError = null;
  console.log(`Connected to MQTT broker. Subscribing to ${MQTT_EVENTS_TOPIC} and ${MQTT_ALERTS_TOPIC}.`);
  mqttClient.subscribe([MQTT_EVENTS_TOPIC, MQTT_ALERTS_TOPIC], { qos: MQTT_QOS });
});

mqttClient.on('message', (topic, message) => {
  const payloadString = message.toString();
  if (topic === MQTT_EVENTS_TOPIC) {
    void handleEventMessage(payloadString);
    return;
  }

  if (topic === MQTT_ALERTS_TOPIC) {
    void handleAlertMessage(payloadString);
  }
});

mqttClient.on('close', () => {
  state.brokerReady = false;
});

mqttClient.on('offline', () => {
  state.brokerReady = false;
});

mqttClient.on('error', (error) => {
  state.brokerReady = false;
  state.lastBrokerError = error.message;
  console.error('MQTT error:', error.message);
});

const app = express();

app.get('/health', (req, res) => {
  const ready = state.brokerReady;
  res.status(ready ? 200 : 503).json({
    status: ready ? 'ok' : 'degraded',
    ready,
    total_events: state.totalEvents,
    total_alerts: state.totalAlerts,
    last_broker_error: state.lastBrokerError,
    last_maas_error: state.lastMaasError,
  });
});

app.get('/api/summary', (req, res) => {
  res.json(buildSummary());
});

app.get('/api/events', (req, res) => {
  const requestedLimit = Number.parseInt(String(req.query.limit || '20'), 10);
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(requestedLimit, RECENT_EVENTS_LIMIT))
    : 20;
  res.json({
    events: state.recentEvents.slice(0, limit),
  });
});

app.get('/api/alerts', (req, res) => {
  const requestedLimit = Number.parseInt(String(req.query.limit || '10'), 10);
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(requestedLimit, RECENT_ALERTS_LIMIT))
    : 10;
  res.json({
    alerts: state.recentAlerts.slice(0, limit),
  });
});

app.get('/api/dashboard', (req, res) => {
  res.json(buildDashboardPayload());
});

app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', client.register.contentType);
    res.end(await client.register.metrics());
  } catch (error) {
    res.status(500).end(error.message);
  }
});

app.listen(PORT, () => {
  console.log(`RFID analytics service listening on port ${PORT}`);
});
