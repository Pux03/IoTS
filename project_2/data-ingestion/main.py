import asyncio
import json
import logging
import os
import time
from typing import Optional

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import PlainTextResponse
import uvicorn
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY

from generator import generate_event, DEVICES

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestion")

# =====================================
# ENV CONFIG
# =====================================
BROKER_TYPE = os.getenv("BROKER_TYPE", "mqtt").lower() # "mqtt" or "kafka"
MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_QOS = int(os.getenv("MQTT_QOS", 0))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "1") # "0", "1", "all"

MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/events")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-events")

# =====================================
# PROMETHEUS METRICS
# =====================================
MSG_SENT = Counter(
    "ingestion_messages_sent_total",
    "Total messages successfully published",
    ["broker_type", "device_id"]
)
MSG_ERRORS = Counter(
    "ingestion_send_errors_total",
    "Total errors while publishing",
    ["broker_type"]
)
SEND_LATENCY = Histogram(
    "ingestion_send_duration_seconds",
    "Time taken to publish message",
    ["broker_type"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
)

# =====================================
# CLIENT WRAPPERS
# =====================================
class MqttPublisher:
    def __init__(self, host, port, qos):
        self.host = host
        self.port = port
        self.qos = qos
        self.client = None
        self.connected = False

    def connect(self):
        import paho.mqtt.client as mqtt
        self.client = mqtt.Client(client_id="ingestion_service_publisher", clean_session=True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        
        logger.info(f"Connecting to MQTT Broker at {self.host}:{self.port}...")
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Successfully connected to MQTT Broker")
        else:
            self.connected = False
            logger.error(f"Failed to connect to MQTT Broker, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        logger.warning(f"Disconnected from MQTT Broker with code {rc}")

    async def publish(self, topic, payload):
        if not self.connected:
            MSG_ERRORS.labels(broker_type="mqtt").inc()
            raise ConnectionError("MQTT broker not connected")

        start_time = time.time()
        try:
            info = self.client.publish(topic, payload, qos=self.qos)
            # If QoS > 0, we can optionally wait for the message to be published
            # to measure delivery latency accurately.
            if self.qos > 0:
                # wait_for_publish is blocking, so we run in executor to not block asyncio event loop
                await asyncio.get_event_loop().run_in_executor(None, info.wait_for_publish)
            
            duration = time.time() - start_time
            SEND_LATENCY.labels(broker_type="mqtt").observe(duration)
        except Exception as e:
            MSG_ERRORS.labels(broker_type="mqtt").inc()
            logger.error(f"MQTT Publish error: {e}")
            raise e

class KafkaPublisher:
    def __init__(self, bootstrap_servers, acks):
        self.bootstrap_servers = bootstrap_servers
        self.acks = acks
        self.producer = None

    def connect(self):
        from confluent_kafka import Producer
        
        # Svi razmaci su sada 100% konzistentni (čist Space, bez Tabova)
        acks_val = str(self.acks).strip()
        if acks_val in ["0", "1"]:
            acks_param = int(acks_val)  # Pretvaramo u integer (0 ili 1)
        else:
            acks_param = "all"
            
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': acks_param,
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.ms': 10,
            'batch.num.messages': 1000,
            # Handle broker disconnects gracefully by retrying
            'retries': 5,
            'retry.backoff.ms': 500
        }
        logger.info(f"Connecting to Kafka Broker at {self.bootstrap_servers} with acks={acks_param}...")
        self.producer = Producer(conf)

    async def publish(self, topic, key, payload):
        start_time = time.time()
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def delivery_report(err, msg):
            if err is not None:
                MSG_ERRORS.labels(broker_type="kafka").inc()
                loop.call_soon_threadsafe(future.set_exception, Exception(str(err)))
            else:
                duration = time.time() - start_time
                SEND_LATENCY.labels(broker_type="kafka").observe(duration)
                loop.call_soon_threadsafe(future.set_result, True)

        try:
            # Produce message
            self.producer.produce(topic, key=key, value=payload, callback=delivery_report)
            # Poll to trigger delivery report callbacks
            self.producer.poll(0)
            await future
        except Exception as e:
            MSG_ERRORS.labels(broker_type="kafka").inc()
            logger.error(f"Kafka Publish error: {e}")
            raise e
# =====================================
# GLOBAL STATE & INITIALIZATION
# =====================================
app = FastAPI(title="IoT Data Ingestion Service")
publisher = None

@app.on_event("startup")
def startup_event():
    global publisher
    if BROKER_TYPE == "mqtt":
        publisher = MqttPublisher(MQTT_HOST, MQTT_PORT, MQTT_QOS)
        publisher.connect()
    elif BROKER_TYPE == "kafka":
        publisher = KafkaPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_ACKS)
        publisher.connect()
        # Keep a background thread polling the producer to trigger callbacks
        def poll_loop():
            while True:
                publisher.producer.poll(0.1)
                time.sleep(0.05)
        import threading
        t = threading.Thread(target=poll_loop, daemon=True)
        t.start()
    else:
        logger.error(f"Unknown BROKER_TYPE: {BROKER_TYPE}")

# Global simulation control
sim_running = False
sim_tasks = []

async def publish_event_safe(event):
    payload = json.dumps(event)
    try:
        if BROKER_TYPE == "mqtt":
            await publisher.publish(MQTT_TOPIC, payload)
        elif BROKER_TYPE == "kafka":
            await publisher.publish(KAFKA_TOPIC, key=event["device_id"], payload=payload)
        MSG_SENT.labels(broker_type=BROKER_TYPE, device_id=event["device_id"]).inc()
    except Exception as e:
        logger.debug(f"Publish failed: {e}")

async def device_worker(device_id: str, interval_sec: float):
    logger.info(f"Worker started for device {device_id} with interval {interval_sec}s")
    while sim_running:
        event = generate_event(device_id=device_id)
        await publish_event_safe(event)
        await asyncio.sleep(interval_sec)

# =====================================
# API ENDPOINTS
# =====================================
@app.get("/health")
async def health():
    return {"status": "ok", "broker_type": BROKER_TYPE}

@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return generate_latest(REGISTRY)

@app.get("/status")
async def get_status():
    return {
        "simulation_running": sim_running,
        "active_devices": len(sim_tasks),
        "broker_type": BROKER_TYPE,
        "mqtt_qos": MQTT_QOS if BROKER_TYPE == "mqtt" else None,
        "kafka_acks": KAFKA_ACKS if BROKER_TYPE == "kafka" else None
    }

@app.post("/scenario/a/start")
async def start_scenario_a(devices: int = 100, interval: float = 1.0):
    global sim_running, sim_tasks
    if sim_running:
        return {"status": "already_running"}
    
    sim_running = True
    sim_tasks = []
    
    # Generate list of device IDs
    device_list = [f"DEVICE-{i:05d}" for i in range(devices)]
    
    for dev_id in device_list:
        task = asyncio.create_task(device_worker(dev_id, interval))
        sim_tasks.append(task)
        
    logger.info(f"Started Scenario A: {devices} devices publishing every {interval}s")
    return {"status": "started", "devices": devices, "interval": interval}

@app.post("/scenario/a/stop")
async def stop_scenario_a():
    global sim_running, sim_tasks
    if not sim_running:
        return {"status": "not_running"}
        
    sim_running = False
    for task in sim_tasks:
        task.cancel()
    
    # Wait for tasks to clean up
    if sim_tasks:
        await asyncio.gather(*sim_tasks, return_exceptions=True)
        
    sim_tasks = []
    logger.info("Stopped Scenario A simulation")
    return {"status": "stopped"}

async def run_burst(rate: int, duration_sec: int):
    # Generates a burst of events
    logger.info(f"Starting burst: {rate} msg/s for {duration_sec}s")
    total_to_send = rate * duration_sec
    delay = 1.0 / rate
    
    start = time.time()
    sent = 0
    
    while sent < total_to_send:
        event = generate_event()
        asyncio.create_task(publish_event_safe(event))
        sent += 1
        
        # Adjust delay dynamically to keep up with target rate
        expected_elapsed = sent * delay
        actual_elapsed = time.time() - start
        if actual_elapsed < expected_elapsed:
            await asyncio.sleep(expected_elapsed - actual_elapsed)
            
    logger.info(f"Burst complete. Sent {sent} events in {time.time() - start:.2f}s")

@app.post("/scenario/c/trigger")
async def trigger_scenario_c(background_tasks: BackgroundTasks, rate: int = 5000, duration: int = 5):
    # Trigger Burst Load
    background_tasks.add_task(run_burst, rate, duration)
    return {"status": "burst_triggered", "target_rate": rate, "duration_sec": duration}

@app.post("/scenario/d/trigger")
async def trigger_scenario_d(count: int = 50):
    # Generate messages with high temperature to trigger the streaming analytics alert
    # and measure E2E latency.
    logger.info(f"Triggering Scenario D: sending {count} critical temperature events")
    for _ in range(count):
        # We generate a critical temperature event (temp > 50)
        # We put a custom timestamp to measure E2E latency
        event = generate_event(critical_temp=True)
        # Add high-resolution sent_at time
        event["sent_at"] = time.time()
        await publish_event_safe(event)
    return {"status": "alert_events_sent", "count": count}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
