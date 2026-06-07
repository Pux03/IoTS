const express = require('express');
const { Pool } = require('pg');
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
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'data-storage-group';

const DISABLE_DB_WRITE = process.env.DISABLE_DB_WRITE === 'true';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500', 10);
const FLUSH_INTERVAL_MS = parseInt(process.env.FLUSH_INTERVAL_MS || '1000', 10);

const DB_CONFIG = {
  host: process.env.DB_HOST || 'db',
  port: parseInt(process.env.DB_PORT || '5432', 10),
  database: process.env.DB_NAME || 'access_control_system',
  user: process.env.DB_USER || 'admin',
  password: process.env.DB_PASSWORD || 'admin',
  max: 20, // Max connection pool size
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
};

// =====================================
// PROMETHEUS METRICS
// =====================================
const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ register: client.register });

const messagesReceived = new client.Counter({
  name: 'storage_messages_received_total',
  help: 'Total messages received from broker',
  labelNames: ['broker_type'],
});

const dbWrites = new client.Counter({
  name: 'storage_db_writes_total',
  help: 'Total database insert operations (batches)',
});

const dbWriteErrors = new client.Counter({
  name: 'storage_db_write_errors_total',
  help: 'Total database write errors',
});

const dbWriteLatency = new client.Histogram({
  name: 'storage_db_write_latency_seconds',
  help: 'Time spent executing database write',
  buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 3.0],
});

// =====================================
// DATABASE & BATCHING LOGIC
// =====================================
let pool = null;
if (!DISABLE_DB_WRITE) {
  console.log('Database writes are enabled. Connecting to PostgreSQL...');
  pool = new Pool(DB_CONFIG);
  pool.on('error', (err) => {
    console.error('Unexpected error on idle pg client', err);
  });
} else {
  console.log('Database writes are DISABLED (DISABLE_DB_WRITE=true).');
}

let messageQueue = [];
let flushTimeout = null;

async function flushBatch() {
  if (flushTimeout) {
    clearTimeout(flushTimeout);
    flushTimeout = null;
  }

  if (messageQueue.length === 0) {
    return;
  }

  const batch = [...messageQueue];
  messageQueue = [];

  if (DISABLE_DB_WRITE) {
    // DB writing disabled for profiling broker throughput
    return;
  }

  const endTimer = dbWriteLatency.startTimer();
  try {
    const values = [];
    const valuePlaceholders = [];
    let index = 1;

    for (const event of batch) {
      values.push(
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
        event.temperature
      );

      const placeholders = [];
      for (let j = 0; j < 12; j++) {
        placeholders.push(`$${index++}`);
      }
      valuePlaceholders.push(`(${placeholders.join(', ')})`);
    }

    const query = `
      INSERT INTO events (
        event_id, timestamp, device_id, card_uid, access_granted,
        door_id, zone, signal_strength, battery_voltage,
        response_time_ms, event_type, temperature
      ) VALUES ${valuePlaceholders.join(', ')}
    `;

    await pool.query(query, values);
    dbWrites.inc();
    endTimer();
  } catch (error) {
    dbWriteErrors.inc();
    console.error('Database write error:', error.message);
  }
}

function handleMessage(payloadString) {
  messagesReceived.labels(BROKER_TYPE).inc();
  try {
    const event = JSON.parse(payloadString);
    messageQueue.push(event);

    if (messageQueue.length >= BATCH_SIZE) {
      flushBatch();
    } else if (!flushTimeout) {
      flushTimeout = setTimeout(flushBatch, FLUSH_INTERVAL_MS);
    }
  } catch (e) {
    console.error('Failed to parse message payload:', e.message);
  }
}

// =====================================
// BROKER CONNECTIONS
// =====================================
if (BROKER_TYPE === 'mqtt') {
  const brokerUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
  console.log(`Connecting to MQTT broker at ${brokerUrl}...`);
  const mqttClient = mqtt.connect(brokerUrl);

  mqttClient.on('connect', () => {
    console.log(`Connected to MQTT broker. Subscribing to topic: ${MQTT_TOPIC} with QoS: ${MQTT_QOS}`);
    mqttClient.subscribe(MQTT_TOPIC, { qos: MQTT_QOS });
  });

  mqttClient.on('message', (topic, message) => {
    handleMessage(message.toString());
  });

  mqttClient.on('error', (err) => {
    console.error('MQTT error:', err.message);
  });
} else if (BROKER_TYPE === 'kafka') {
  console.log(`Connecting to Kafka broker at ${KAFKA_BOOTSTRAP_SERVERS}...`);
  const kafka = new Kafka({
    clientId: 'data-storage-service',
    brokers: KAFKA_BOOTSTRAP_SERVERS.split(','),
  });

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

  const runKafkaConsumer = async () => {
    await consumer.connect();
    console.log(`Connected to Kafka. Subscribing to topic: ${KAFKA_TOPIC}`);
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (message.value) {
          handleMessage(message.value.toString());
        }
      },
    });
  };

  runKafkaConsumer().catch((err) => {
    console.error('Kafka consumer error:', err.message);
  });
} else {
  console.error(`Invalid BROKER_TYPE: ${BROKER_TYPE}`);
}

// =====================================
// HEALTH & METRICS SERVER
// =====================================
const app = express();
const PORT = process.env.PORT || 8001;

app.get('/health', (req, res) => {
  res.json({ status: 'ok', broker_type: BROKER_TYPE, db_write_enabled: !DISABLE_DB_WRITE });
});

app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', client.register.contentType);
    res.end(await client.register.metrics());
  } catch (err) {
    res.status(500).end(err);
  }
});

app.listen(PORT, () => {
  console.log(`Storage service metrics/health server running on port ${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received. Flushing remaining messages and shutting down...');
  await flushBatch();
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});
