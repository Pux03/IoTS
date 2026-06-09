const express = require('express');
const mqtt = require('mqtt');
const { Kafka } = require('kafkajs');
const client = require('prom-client');

// =====================================
// CONFIG
// =====================================
const BROKER_TYPE = (process.env.BROKER_TYPE || 'mqtt').toLowerCase();
const MQTT_HOST = process.env.MQTT_HOST || 'mqtt-broker';
const MQTT_PORT = process.env.MQTT_PORT || '1883';
const MQTT_QOS = parseInt(process.env.MQTT_QOS || '0', 10);
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'iot/events';

const KAFKA_BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka-broker:29092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'iot-events';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'analytics-group';
const KAFKA_STARTUP_RETRY_MS = parseInt(process.env.KAFKA_STARTUP_RETRY_MS || '3000', 10);
const KAFKA_TOPIC_PARTITIONS = parseInt(process.env.KAFKA_TOPIC_PARTITIONS || '1', 10);

const WINDOW_DURATION_MS = 10000;
const TEMP_THRESHOLD = 50.0;
const ALERT_HISTORY_LIMIT = parseInt(process.env.ALERT_HISTORY_LIMIT || '200', 10);

// =====================================
// PROMETHEUS METRICS
// =====================================
const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ register: client.register });

const messagesProcessed = new client.Counter({
  name: 'analytics_messages_processed_total',
  help: 'Total messages processed by analytics service',
  labelNames: ['broker_type'],
});

const alertsTotal = new client.Counter({
  name: 'analytics_alerts_total',
  help: 'Total number of temperature threshold breach alerts',
});

const windowAvgTemp = new client.Gauge({
  name: 'analytics_window_avg_temperature',
  help: 'Average temperature in the current tumbling window',
});

const e2eLatencyGauge = new client.Gauge({
  name: 'analytics_e2e_latency_ms',
  help: 'End-to-end latency in milliseconds from generation to alert output',
});

const alertFirstLatencyGauge = new client.Gauge({
  name: 'analytics_alert_e2e_first_latency_ms',
  help: 'Alert latency in milliseconds from the first critical event generation to alert output',
  labelNames: ['broker_type'],
});

const alertLastLatencyGauge = new client.Gauge({
  name: 'analytics_alert_e2e_last_latency_ms',
  help: 'Alert latency in milliseconds from the last critical event generation to alert output',
  labelNames: ['broker_type'],
});

const messageLatencyHistogram = new client.Histogram({
  name: 'analytics_message_e2e_latency_ms',
  help: 'End-to-end latency in milliseconds from generation to analytics consumption',
  labelNames: ['broker_type'],
  buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 120000],
});

const messageLatencyMaxGauge = new client.Gauge({
  name: 'analytics_message_e2e_latency_max_ms',
  help: 'Maximum observed end-to-end latency in milliseconds from generation to analytics consumption',
  labelNames: ['broker_type'],
});

const currentWindowQueueDepth = new client.Gauge({
  name: 'analytics_window_event_queue_depth',
  help: 'Current number of events buffered in the active tumbling window',
  labelNames: ['broker_type'],
});

const lastWindowFlushAtGauge = new client.Gauge({
  name: 'analytics_window_last_flush_at_ms',
  help: 'Unix timestamp in milliseconds for the latest tumbling window flush',
  labelNames: ['broker_type'],
});

const lastAlertEmittedAtGauge = new client.Gauge({
  name: 'analytics_last_alert_emitted_at_ms',
  help: 'Unix timestamp in milliseconds for the latest emitted alert',
  labelNames: ['broker_type'],
});

// =====================================
// STREAM PROCESSING (TUMBLING WINDOW)
// =====================================
let currentWindowEvents = [];
let brokerReady = false;
let consumerReady = false;
let lastBrokerError = null;
let maxObservedMessageLatencyMs = 0;
const serviceStartedAtMs = Date.now();
let currentWindowStartedAtMs = serviceStartedAtMs;
let lastWindowFlushedAtMs = null;
let alertSequence = 0;
let lastAlertRecord = null;
const alertHistory = [];

function updateWindowQueueMetrics() {
  currentWindowQueueDepth.labels(BROKER_TYPE).set(currentWindowEvents.length);
}

function parseNumeric(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function getEventGeneratedAtMs(event) {
  const directMs = parseNumeric(
    event.benchmark_sent_at_ms
      ?? event.sent_at_ms
      ?? event.generated_at_ms
      ?? event.emitted_at_ms
  );
  if (directMs !== null) {
    return directMs;
  }

  const sentAt = parseNumeric(event.sent_at);
  if (sentAt !== null) {
    return sentAt > 1e12 ? sentAt : sentAt * 1000;
  }

  return null;
}

function getEventTemperature(event) {
  const temperature = parseNumeric(event.temperature);
  return temperature !== null ? temperature : null;
}

function getEventRunId(event) {
  return typeof event.run_id === 'string' && event.run_id ? event.run_id : null;
}

function getEventWindowMode(event) {
  return typeof event.window_mode === 'string' && event.window_mode ? event.window_mode : 'unspecified';
}

function cacheGeneratedAt(event) {
  const generatedAtMs = getEventGeneratedAtMs(event);
  if (generatedAtMs !== null) {
    event._generated_at_ms = generatedAtMs;
  }
  return generatedAtMs;
}

function getCachedGeneratedAt(event) {
  if (typeof event._generated_at_ms === 'number' && Number.isFinite(event._generated_at_ms)) {
    return event._generated_at_ms;
  }
  return cacheGeneratedAt(event);
}

function pushAlertRecord(alertRecord) {
  lastAlertRecord = alertRecord;
  alertHistory.push(alertRecord);
  while (alertHistory.length > ALERT_HISTORY_LIMIT) {
    alertHistory.shift();
  }
}

function getNextWindowFlushAtMs(nowMs = Date.now()) {
  const elapsedMs = Math.max(0, nowMs - currentWindowStartedAtMs);
  const remainingMs = Math.max(0, WINDOW_DURATION_MS - elapsedMs);
  return nowMs + remainingMs;
}

function handleIncomingMessage(payloadString) {
  messagesProcessed.labels(BROKER_TYPE).inc();
  try {
    const event = JSON.parse(payloadString);
    const generatedAtMs = cacheGeneratedAt(event);
    if (generatedAtMs !== null) {
      const latencyMs = Date.now() - generatedAtMs;
      if (Number.isFinite(latencyMs) && latencyMs >= 0) {
        messageLatencyHistogram.labels(BROKER_TYPE).observe(latencyMs);
        if (latencyMs > maxObservedMessageLatencyMs) {
          maxObservedMessageLatencyMs = latencyMs;
          messageLatencyMaxGauge.labels(BROKER_TYPE).set(latencyMs);
        }
      }
    }
    currentWindowEvents.push(event);
    updateWindowQueueMetrics();
  } catch (e) {
    console.error('Failed to parse message payload:', e.message);
  }
}

setInterval(() => {
  const windowEndedAtMs = Date.now();
  const windowStartedAtMs = currentWindowStartedAtMs;
  const events = [...currentWindowEvents];
  currentWindowEvents = [];
  currentWindowStartedAtMs = windowEndedAtMs;
  lastWindowFlushedAtMs = windowEndedAtMs;
  lastWindowFlushAtGauge.labels(BROKER_TYPE).set(windowEndedAtMs);
  updateWindowQueueMetrics();

  if (events.length === 0) {
    return;
  }

  const tempSum = events.reduce((sum, event) => sum + (getEventTemperature(event) || 0), 0);
  const avgTemp = tempSum / events.length;
  windowAvgTemp.set(parseFloat(avgTemp.toFixed(2)));

  console.log(
    `[WINDOW] Completed window. Messages: ${events.length}. ` +
    `Average temperature: ${avgTemp.toFixed(2)}C`
  );

  if (avgTemp > TEMP_THRESHOLD) {
    alertsTotal.inc();

    const criticalEvents = events.filter((event) => {
      if (event.critical === true) {
        return true;
      }
      const temperature = getEventTemperature(event);
      return temperature !== null && temperature > TEMP_THRESHOLD;
    });

    const timestampedEvents = events
      .map((event) => ({
        event,
        generatedAtMs: getCachedGeneratedAt(event),
      }))
      .filter((item) => item.generatedAtMs !== null);

    const timestampedCriticalEvents = criticalEvents
      .map((event) => ({
        event,
        generatedAtMs: getCachedGeneratedAt(event),
      }))
      .filter((item) => item.generatedAtMs !== null);

    const latencyBasis = timestampedCriticalEvents.length > 0
      ? timestampedCriticalEvents
      : timestampedEvents;

    const runIds = Array.from(
      new Set(
        criticalEvents
          .map((event) => getEventRunId(event))
          .filter((value) => value !== null)
      )
    );
    const windowModes = Array.from(
      new Set(
        criticalEvents
          .map((event) => getEventWindowMode(event))
          .filter((value) => value)
      )
    );
    const primaryWindowMode = windowModes.length === 1
      ? windowModes[0]
      : (windowModes[0] || 'unspecified');

    const alertRecord = {
      alert_id: `alert-${windowEndedAtMs}-${++alertSequence}`,
      broker_type: BROKER_TYPE,
      run_ids: runIds,
      primary_run_id: runIds.length > 0 ? runIds[0] : null,
      window_mode: windowModes.length > 1 ? 'mixed' : primaryWindowMode,
      avg_temperature: parseFloat(avgTemp.toFixed(3)),
      events_in_window: events.length,
      critical_events_in_window: criticalEvents.length,
      alert_emitted_at_ms: windowEndedAtMs,
      window_started_at_ms: windowStartedAtMs,
      window_ended_at_ms: windowEndedAtMs,
      first_critical_sent_at_ms: null,
      last_critical_sent_at_ms: null,
      alert_latency_first_ms: null,
      alert_latency_last_ms: null,
    };

    let latencyInfoStr = '';
    if (latencyBasis.length > 0) {
      const firstEvent = latencyBasis.reduce((earliest, item) => {
        return item.generatedAtMs < earliest.generatedAtMs ? item : earliest;
      }, latencyBasis[0]);
      const lastEvent = latencyBasis.reduce((latest, item) => {
        return item.generatedAtMs > latest.generatedAtMs ? item : latest;
      }, latencyBasis[0]);

      const alertLatencyFirstMs = Math.max(0, windowEndedAtMs - firstEvent.generatedAtMs);
      const alertLatencyLastMs = Math.max(0, windowEndedAtMs - lastEvent.generatedAtMs);

      alertRecord.first_critical_sent_at_ms = firstEvent.generatedAtMs;
      alertRecord.last_critical_sent_at_ms = lastEvent.generatedAtMs;
      alertRecord.alert_latency_first_ms = alertLatencyFirstMs;
      alertRecord.alert_latency_last_ms = alertLatencyLastMs;

      alertFirstLatencyGauge.labels(BROKER_TYPE).set(alertLatencyFirstMs);
      alertLastLatencyGauge.labels(BROKER_TYPE).set(alertLatencyLastMs);
      e2eLatencyGauge.set(alertLatencyLastMs);
      latencyInfoStr =
        ` | alert_latency_first_ms=${alertLatencyFirstMs}` +
        ` | alert_latency_last_ms=${alertLatencyLastMs}`;
    }

    lastAlertEmittedAtGauge.labels(BROKER_TYPE).set(windowEndedAtMs);
    pushAlertRecord(alertRecord);

    console.warn(
      `[ALARM] Critical temperature window detected. Average temperature ` +
      `(${avgTemp.toFixed(2)}C) exceeded threshold ${TEMP_THRESHOLD}C.${latencyInfoStr}`
    );
  }
}, WINDOW_DURATION_MS);

// =====================================
// BROKER CONNECTIONS
// =====================================
if (BROKER_TYPE === 'mqtt') {
  const brokerUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
  console.log(`Connecting to MQTT broker at ${brokerUrl}...`);
  const mqttClient = mqtt.connect(brokerUrl);

  mqttClient.on('connect', () => {
    brokerReady = true;
    consumerReady = true;
    lastBrokerError = null;
    console.log(`Connected to MQTT broker. Subscribing to topic: ${MQTT_TOPIC} with QoS: ${MQTT_QOS}`);
    mqttClient.subscribe(MQTT_TOPIC, { qos: MQTT_QOS });
  });

  mqttClient.on('message', (topic, message) => {
    handleIncomingMessage(message.toString());
  });

  mqttClient.on('close', () => {
    brokerReady = false;
    consumerReady = false;
  });

  mqttClient.on('offline', () => {
    brokerReady = false;
    consumerReady = false;
  });

  mqttClient.on('error', (err) => {
    brokerReady = false;
    consumerReady = false;
    lastBrokerError = err.message;
    console.error('MQTT error:', err.message);
  });
} else if (BROKER_TYPE === 'kafka') {
  console.log(`Connecting to Kafka broker at ${KAFKA_BOOTSTRAP_SERVERS}...`);
  const kafka = new Kafka({
    clientId: 'analytics-service',
    brokers: KAFKA_BOOTSTRAP_SERVERS.split(','),
  });

  const ensureKafkaTopicExists = async () => {
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: KAFKA_TOPIC,
            numPartitions: KAFKA_TOPIC_PARTITIONS,
            replicationFactor: 1,
          },
        ],
      });
    } finally {
      await admin.disconnect().catch(() => {});
    }
  };

  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const runKafkaConsumer = async () => {
    while (true) {
      let consumer = null;
      try {
        await ensureKafkaTopicExists();

        consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });
        consumer.on(consumer.events.GROUP_JOIN, () => {
          brokerReady = true;
          consumerReady = true;
          lastBrokerError = null;
        });
        consumer.on(consumer.events.CRASH, ({ payload }) => {
          brokerReady = false;
          consumerReady = false;
          lastBrokerError = payload.error ? payload.error.message : 'Kafka consumer crashed';
        });
        consumer.on(consumer.events.DISCONNECT, () => {
          brokerReady = false;
          consumerReady = false;
        });

        await consumer.connect();
        brokerReady = true;
        console.log(`Connected to Kafka. Subscribing to topic: ${KAFKA_TOPIC}`);
        await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
        await consumer.run({
          eachMessage: async ({ message }) => {
            if (message.value) {
              handleIncomingMessage(message.value.toString());
            }
          },
        });
        return;
      } catch (err) {
        brokerReady = false;
        consumerReady = false;
        lastBrokerError = err.message;
        console.error(`Kafka consumer error: ${err.message}. Retrying in ${KAFKA_STARTUP_RETRY_MS}ms...`);
        if (consumer) {
          await consumer.disconnect().catch(() => {});
        }
        await delay(KAFKA_STARTUP_RETRY_MS);
      }
    }
  };

  runKafkaConsumer().catch((err) => {
    lastBrokerError = err.message;
    console.error('Kafka consumer fatal error:', err.message);
  });
} else {
  console.error(`Invalid BROKER_TYPE: ${BROKER_TYPE}`);
}

// =====================================
// HEALTH & METRICS SERVER
// =====================================
const server = express();
const PORT = process.env.PORT || 8002;
updateWindowQueueMetrics();

server.get('/health', (req, res) => {
  const ready = brokerReady && consumerReady;
  res.status(ready ? 200 : 503).json({
    status: ready ? 'ok' : 'degraded',
    ready,
    broker_type: BROKER_TYPE,
    threshold: TEMP_THRESHOLD,
    window_duration_ms: WINDOW_DURATION_MS,
    last_broker_error: lastBrokerError,
  });
});

server.get('/window-state', (req, res) => {
  const nowMs = Date.now();
  res.json({
    broker_type: BROKER_TYPE,
    threshold: TEMP_THRESHOLD,
    window_duration_ms: WINDOW_DURATION_MS,
    service_started_at_ms: serviceStartedAtMs,
    current_window_started_at_ms: currentWindowStartedAtMs,
    last_window_flushed_at_ms: lastWindowFlushedAtMs,
    next_window_flush_at_ms: getNextWindowFlushAtMs(nowMs),
    now_ms: nowMs,
    buffered_events: currentWindowEvents.length,
    alert_history_size: alertHistory.length,
    last_alert_primary_run_id: lastAlertRecord ? lastAlertRecord.primary_run_id : null,
  });
});

server.get('/alerts/latest', (req, res) => {
  const runId = typeof req.query.run_id === 'string' && req.query.run_id
    ? req.query.run_id
    : null;
  const windowMode = typeof req.query.window_mode === 'string' && req.query.window_mode
    ? req.query.window_mode
    : null;

  const alert = [...alertHistory].reverse().find((entry) => {
    const runMatches = !runId || entry.run_ids.includes(runId);
    const modeMatches = !windowMode || entry.window_mode === windowMode;
    return runMatches && modeMatches;
  }) || null;

  res.json({
    found: Boolean(alert),
    alert,
  });
});

server.get('/alerts/recent', (req, res) => {
  const requestedLimit = parseInt(String(req.query.limit || '20'), 10);
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(requestedLimit, ALERT_HISTORY_LIMIT))
    : 20;
  res.json({
    alerts: alertHistory.slice(-limit),
  });
});

server.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', client.register.contentType);
    res.end(await client.register.metrics());
  } catch (err) {
    res.status(500).end(err);
  }
});

server.listen(PORT, () => {
  console.log(`Analytics service metrics/health server running on port ${PORT}`);
});
