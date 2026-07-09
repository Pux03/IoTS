const express = require('express');
const { Pool } = require('pg');
const mqtt = require('mqtt');
const client = require('prom-client');

const MQTT_HOST = process.env.MQTT_HOST || 'mqtt-broker';
const MQTT_PORT = process.env.MQTT_PORT || '1883';
const MQTT_QOS = Number.parseInt(process.env.MQTT_QOS || '1', 10);
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'rfid/events';

const DB_CONFIG = {
  host: process.env.DB_HOST || 'db',
  port: Number.parseInt(process.env.DB_PORT || '5432', 10),
  database: process.env.DB_NAME || 'access_control_system',
  user: process.env.DB_USER || 'admin',
  password: process.env.DB_PASSWORD || 'admin',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
};

const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ register: client.register });

const eventsReceived = new client.Counter({
  name: 'storage_events_received_total',
  help: 'Total number of RFID events received from MQTT.',
});

const eventsPersisted = new client.Counter({
  name: 'storage_events_persisted_total',
  help: 'Total number of RFID events written to PostgreSQL.',
});

const persistErrors = new client.Counter({
  name: 'storage_persist_errors_total',
  help: 'Total number of storage errors.',
});

const pool = new Pool(DB_CONFIG);
const app = express();
const PORT = process.env.PORT || 8001;

const insertEventSql = `
  INSERT INTO events (
    event_id,
    timestamp,
    device_id,
    card_uid,
    access_granted,
    door_id,
    zone,
    signal_strength,
    battery_voltage,
    response_time_ms,
    event_type
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
  )
  ON CONFLICT (event_id) DO NOTHING
`;

const schemaSql = `
  CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    card_uid VARCHAR(32) NOT NULL,
    access_granted BOOLEAN NOT NULL,
    door_id VARCHAR(50) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    signal_strength INT NOT NULL,
    battery_voltage NUMERIC(4, 2) NOT NULL,
    response_time_ms INT NOT NULL,
    event_type VARCHAR(50) NOT NULL
  );

  ALTER TABLE events DROP COLUMN IF EXISTS temperature;
  CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
  CREATE INDEX IF NOT EXISTS idx_events_device_id ON events(device_id);
  CREATE INDEX IF NOT EXISTS idx_events_zone ON events(zone);
`;

let brokerReady = false;
let dbReady = false;
let lastBrokerError = null;
let lastDbError = null;
const state = {
  received: 0,
  persisted: 0,
  errors: 0,
  lastStoredEventId: null,
  lastStoredAt: null,
};

pool.on('error', (error) => {
  dbReady = false;
  lastDbError = error.message;
  console.error('Unexpected PostgreSQL error:', error.message);
});

function parseEventPayload(payloadString) {
  const raw = JSON.parse(payloadString);
  return {
    event_id: String(raw.event_id),
    timestamp: String(raw.timestamp),
    device_id: String(raw.device_id),
    card_uid: String(raw.card_uid),
    access_granted: raw.access_granted === true || raw.access_granted === 'true',
    door_id: String(raw.door_id),
    zone: String(raw.zone),
    signal_strength: Number(raw.signal_strength),
    battery_voltage: Number(raw.battery_voltage),
    response_time_ms: Number(raw.response_time_ms),
    event_type: String(raw.event_type),
  };
}

async function ensureDatabaseSchema() {
  await pool.query(schemaSql);
  dbReady = true;
  lastDbError = null;
}

async function persistEvent(payloadString) {
  try {
    const event = parseEventPayload(payloadString);
    eventsReceived.inc();
    state.received += 1;

    await pool.query(insertEventSql, [
      event.event_id,
      event.timestamp,
      event.device_id,
      event.card_uid,
      event.access_granted,
      event.door_id,
      event.zone,
      event.signal_strength,
      event.battery_voltage,
      event.response_time_ms,
      event.event_type,
    ]);

    dbReady = true;
    lastDbError = null;
    state.persisted += 1;
    state.lastStoredEventId = event.event_id;
    state.lastStoredAt = new Date().toISOString();
    eventsPersisted.inc();
  } catch (error) {
    dbReady = false;
    lastDbError = error.message;
    state.errors += 1;
    persistErrors.inc();
    console.error('Failed to persist RFID event:', error.message);
  }
}

async function initializeDatabase() {
  try {
    await pool.query('SELECT 1');
    await ensureDatabaseSchema();
    console.log('Connected to PostgreSQL and verified RFID schema.');
  } catch (error) {
    dbReady = false;
    lastDbError = error.message;
    console.error('Database initialization failed:', error.message);
  }
}

setInterval(() => {
  if (!dbReady) {
    void initializeDatabase();
  }
}, 10000);

const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}:${MQTT_PORT}`, {
  reconnectPeriod: 1000,
});

mqttClient.on('connect', () => {
  brokerReady = true;
  lastBrokerError = null;
  console.log(`Connected to MQTT broker. Subscribing to ${MQTT_TOPIC} with QoS ${MQTT_QOS}.`);
  mqttClient.subscribe(MQTT_TOPIC, { qos: MQTT_QOS });
});

mqttClient.on('message', (topic, message) => {
  void persistEvent(message.toString());
});

mqttClient.on('close', () => {
  brokerReady = false;
});

mqttClient.on('offline', () => {
  brokerReady = false;
});

mqttClient.on('error', (error) => {
  brokerReady = false;
  lastBrokerError = error.message;
  console.error('MQTT error:', error.message);
});

app.get('/health', (req, res) => {
  const ready = brokerReady && dbReady;
  res.status(ready ? 200 : 503).json({
    status: ready ? 'ok' : 'degraded',
    ready,
    mqtt_topic: MQTT_TOPIC,
    last_broker_error: lastBrokerError,
    last_db_error: lastDbError,
  });
});

app.get('/status', (req, res) => {
  res.json({
    service: 'data-storage',
    ready: brokerReady && dbReady,
    mqtt_topic: MQTT_TOPIC,
    received_events: state.received,
    persisted_events: state.persisted,
    storage_errors: state.errors,
    last_stored_event_id: state.lastStoredEventId,
    last_stored_at: state.lastStoredAt,
  });
});

app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', client.register.contentType);
    res.end(await client.register.metrics());
  } catch (error) {
    res.status(500).end(error.message);
  }
});

app.listen(PORT, async () => {
  await initializeDatabase();
  console.log(`RFID storage service listening on port ${PORT}`);
});

process.on('SIGTERM', async () => {
  mqttClient.end(true);
  await pool.end();
  process.exit(0);
});
