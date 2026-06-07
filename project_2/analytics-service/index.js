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

const WINDOW_DURATION_MS = 10000; // 10 seconds tumbling window
const TEMP_THRESHOLD = 50.0; // °C

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

// =====================================
// STREAM PROCESSING (TUMBLING WINDOW)
// =====================================
let currentWindowEvents = [];

function handleIncomingMessage(payloadString) {
  messagesProcessed.labels(BROKER_TYPE).inc();
  try {
    const event = JSON.parse(payloadString);
    currentWindowEvents.push(event);
  } catch (e) {
    console.error('Failed to parse message payload:', e.message);
  }
}

// Tumbling window execution
setInterval(() => {
  if (currentWindowEvents.length === 0) {
    return;
  }

  const events = [...currentWindowEvents];
  currentWindowEvents = [];

  const tempSum = events.reduce((sum, e) => sum + e.temperature, 0);
  const avgTemp = tempSum / events.length;
  windowAvgTemp.set(parseFloat(avgTemp.toFixed(2)));

  console.log(`[WINDOW] Završen prozor. Broj poruka: ${events.length}. Prosečna temperatura: ${avgTemp.toFixed(2)}°C`);

  if (avgTemp > TEMP_THRESHOLD) {
    alertsTotal.inc();
    
    // Find the latest message that had a sent_at timestamp to calculate latency
    const eventsWithTimestamp = events.filter(e => e.sent_at !== undefined);
    
    let latencyInfoStr = '';
    if (eventsWithTimestamp.length > 0) {
      // Get the latest sent event to measure end-to-end latency
      const latestEvent = eventsWithTimestamp.reduce((latest, e) => {
        return (e.sent_at > latest.sent_at) ? e : latest;
      }, eventsWithTimestamp[0]);
      
      const latencyMs = Date.now() - (latestEvent.sent_at * 1000);
      e2eLatencyGauge.set(latencyMs);
      latencyInfoStr = ` | E2E Latencija: ${latencyMs} ms`;
    }

    console.warn(`[ALARM] 🚨 KRITIČNA TEMPERATURA! Prosečna temperatura u prozoru (${avgTemp.toFixed(2)}°C) prelazi prag od ${TEMP_THRESHOLD}°C!${latencyInfoStr}`);
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
    console.log(`Connected to MQTT broker. Subscribing to topic: ${MQTT_TOPIC} with QoS: ${MQTT_QOS}`);
    mqttClient.subscribe(MQTT_TOPIC, { qos: MQTT_QOS });
  });

  mqttClient.on('message', (topic, message) => {
    handleIncomingMessage(message.toString());
  });

  mqttClient.on('error', (err) => {
    console.error('MQTT error:', err.message);
  });
} else if (BROKER_TYPE === 'kafka') {
  console.log(`Connecting to Kafka broker at ${KAFKA_BOOTSTRAP_SERVERS}...`);
  const kafka = new Kafka({
    clientId: 'analytics-service',
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
          handleIncomingMessage(message.value.toString());
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
const app = Math.max ? express() : null; // Ensure standard setup
const server = express();
const PORT = process.env.PORT || 8002;

server.get('/health', (req, res) => {
  res.json({ status: 'ok', broker_type: BROKER_TYPE, threshold: TEMP_THRESHOLD, window_duration_ms: WINDOW_DURATION_MS });
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
