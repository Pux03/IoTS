import asyncio
import json
import logging
import os
import socket
import threading
import time
from collections import deque
from typing import Deque, Optional

from fastapi import BackgroundTasks, FastAPI, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, REGISTRY, generate_latest
import uvicorn

from generator import DEVICES, generate_event


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestion")


# =====================================
# ENV CONFIG
# =====================================
BROKER_TYPE = os.getenv("BROKER_TYPE", "mqtt").lower()
MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_QOS = int(os.getenv("MQTT_QOS", 0))
MQTT_KEEPALIVE_SEC = int(os.getenv("MQTT_KEEPALIVE_SEC", "5"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "1")

MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/events")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-events")

PUBLISH_QUEUE_MAX_SIZE = int(os.getenv("PUBLISH_QUEUE_MAX_SIZE", "200000"))
PUBLISH_WORKER_COUNT = int(os.getenv("PUBLISH_WORKER_COUNT", "8"))
OFFLINE_BUFFER_MAX_SIZE = int(os.getenv("OFFLINE_BUFFER_MAX_SIZE", "200000"))
DISCONNECTED_RETRY_DELAY_MS = int(os.getenv("DISCONNECTED_RETRY_DELAY_MS", "250"))
BROKER_PROBE_INTERVAL_MS = int(os.getenv("BROKER_PROBE_INTERVAL_MS", "1000"))


# =====================================
# PROMETHEUS METRICS
# =====================================
MSG_GENERATED = Counter(
    "ingestion_messages_generated_total",
    "Total generated messages before publish attempt",
    ["broker_type"],
)
MSG_SENT = Counter(
    "ingestion_messages_sent_total",
    "Total messages successfully published",
    ["broker_type", "device_id"],
)
MSG_DROPPED = Counter(
    "ingestion_messages_dropped_total",
    "Total messages dropped before successful publish",
    ["broker_type", "reason"],
)
MSG_ERRORS = Counter(
    "ingestion_send_errors_total",
    "Total errors while publishing",
    ["broker_type"],
)
SEND_LATENCY = Histogram(
    "ingestion_send_duration_seconds",
    "Time taken to publish message",
    ["broker_type"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)
PUBLISH_QUEUE_DEPTH = Gauge(
    "ingestion_publish_queue_depth",
    "Current number of generated events waiting in the publish queue",
    ["broker_type"],
)
OFFLINE_BUFFER_DEPTH = Gauge(
    "ingestion_offline_buffer_depth",
    "Current number of buffered events retained during broker outage",
    ["broker_type"],
)
SIMULATION_RUNNING = Gauge(
    "ingestion_simulation_running",
    "Whether Scenario A style simulation is currently active",
    ["broker_type"],
)
BROKER_CONNECTED = Gauge(
    "ingestion_broker_connected",
    "Whether the ingestion publisher currently considers the broker connected",
    ["broker_type"],
)


# =====================================
# CLIENT WRAPPERS
# =====================================
class MqttPublisher:
    def __init__(self, host: str, port: int, qos: int, keepalive_sec: int, probe_interval_ms: int):
        self.host = host
        self.port = port
        self.qos = qos
        self.keepalive_sec = keepalive_sec
        self.probe_interval_sec = max(probe_interval_ms / 1000.0, 0.25)
        self.client = None
        self.connected = False
        self._last_probe_ok = False
        self._last_probe_at = 0.0

    def connect(self) -> None:
        import paho.mqtt.client as mqtt

        clean_session = self.qos == 0
        self.client = mqtt.Client(
            client_id="ingestion_service_publisher",
            clean_session=clean_session,
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

        logger.info(
            "Connecting to MQTT broker at %s:%s with clean_session=%s...",
            self.host,
            self.port,
            clean_session,
        )
        self.client.connect(self.host, self.port, keepalive=self.keepalive_sec)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):  # noqa: ANN001
        self.connected = rc == 0
        self._last_probe_ok = self.connected
        self._last_probe_at = time.monotonic()
        BROKER_CONNECTED.labels(broker_type="mqtt").set(1 if self.connected else 0)
        if self.connected:
            logger.info("Successfully connected to MQTT broker")
        else:
            logger.error("Failed to connect to MQTT broker, rc=%s", rc)

    def on_disconnect(self, client, userdata, rc):  # noqa: ANN001
        self.connected = False
        self._last_probe_ok = False
        self._last_probe_at = time.monotonic()
        BROKER_CONNECTED.labels(broker_type="mqtt").set(0)
        logger.warning("Disconnected from MQTT broker with code %s", rc)

    def is_connected(self) -> bool:
        if not self.connected:
            return False

        now = time.monotonic()
        if now - self._last_probe_at < self.probe_interval_sec:
            return self._last_probe_ok

        try:
            with socket.create_connection((self.host, self.port), timeout=1):
                self._last_probe_ok = True
        except OSError:
            self.connected = False
            self._last_probe_ok = False
            logger.warning("MQTT broker reachability probe failed; marking publisher as disconnected")

        self._last_probe_at = now
        BROKER_CONNECTED.labels(broker_type="mqtt").set(1 if self.connected and self._last_probe_ok else 0)
        return self.connected and self._last_probe_ok

    async def publish(self, topic: str, payload: str) -> None:
        if not self.is_connected():
            MSG_ERRORS.labels(broker_type="mqtt").inc()
            raise ConnectionError("MQTT broker not connected")

        start_time = time.time()
        try:
            info = self.client.publish(topic, payload, qos=self.qos)
            if self.qos > 0:
                await asyncio.get_running_loop().run_in_executor(None, info.wait_for_publish)
            duration = time.time() - start_time
            SEND_LATENCY.labels(broker_type="mqtt").observe(duration)
        except Exception as exc:
            MSG_ERRORS.labels(broker_type="mqtt").inc()
            logger.error("MQTT publish error: %s", exc)
            raise


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, acks: str):
        self.bootstrap_servers = bootstrap_servers
        self.acks = acks
        self.producer = None

    def connect(self) -> None:
        from confluent_kafka import Producer

        acks_val = str(self.acks).strip()
        acks_param = int(acks_val) if acks_val in {"0", "1"} else "all"
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": acks_param,
            "queue.buffering.max.messages": 1000000,
            "queue.buffering.max.ms": 10,
            "batch.num.messages": 1000,
            "retries": 5,
            "retry.backoff.ms": 500,
        }
        logger.info(
            "Connecting to Kafka broker at %s with acks=%s...",
            self.bootstrap_servers,
            acks_param,
        )
        self.producer = Producer(conf)
        BROKER_CONNECTED.labels(broker_type="kafka").set(1)

    async def publish(self, topic: str, key: str, payload: str) -> None:
        start_time = time.time()
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def delivery_report(err, msg):  # noqa: ANN001
            if err is not None:
                MSG_ERRORS.labels(broker_type="kafka").inc()
                loop.call_soon_threadsafe(future.set_exception, Exception(str(err)))
            else:
                duration = time.time() - start_time
                SEND_LATENCY.labels(broker_type="kafka").observe(duration)
                loop.call_soon_threadsafe(future.set_result, True)

        try:
            self.producer.produce(topic, key=key, value=payload, callback=delivery_report)
            self.producer.poll(0)
            await future
        except Exception as exc:
            MSG_ERRORS.labels(broker_type="kafka").inc()
            logger.error("Kafka publish error: %s", exc)
            raise


# =====================================
# GLOBAL STATE & INITIALIZATION
# =====================================
app = FastAPI(title="IoT Data Ingestion Service")
publisher = None
startup_error: Optional[str] = None
sim_running = False
sim_tasks = []
publisher_tasks = []
publish_queue: asyncio.Queue = asyncio.Queue(maxsize=PUBLISH_QUEUE_MAX_SIZE)
offline_buffer: Deque[dict] = deque()


def update_queue_metrics() -> None:
    PUBLISH_QUEUE_DEPTH.labels(broker_type=BROKER_TYPE).set(publish_queue.qsize())
    OFFLINE_BUFFER_DEPTH.labels(broker_type=BROKER_TYPE).set(len(offline_buffer))


def is_ready() -> bool:
    if startup_error or publisher is None:
        return False
    if BROKER_TYPE == "mqtt":
        return bool(publisher.is_connected())
    if BROKER_TYPE == "kafka":
        return getattr(publisher, "producer", None) is not None
    return False


def should_buffer_when_disconnected() -> bool:
    if BROKER_TYPE == "mqtt":
        return MQTT_QOS > 0
    return BROKER_TYPE == "kafka"


async def requeue_or_drop_event(event: dict, reason: str) -> None:
    if should_buffer_when_disconnected():
        if len(offline_buffer) < OFFLINE_BUFFER_MAX_SIZE:
            offline_buffer.append(event)
        else:
            MSG_DROPPED.labels(broker_type=BROKER_TYPE, reason="offline_buffer_full").inc()
    else:
        MSG_DROPPED.labels(broker_type=BROKER_TYPE, reason=reason).inc()
    update_queue_metrics()


async def enqueue_event(event: dict) -> None:
    event.setdefault("sent_at", time.time())
    MSG_GENERATED.labels(broker_type=BROKER_TYPE).inc()
    if publish_queue.full():
        MSG_DROPPED.labels(broker_type=BROKER_TYPE, reason="publish_queue_full").inc()
        return

    await publish_queue.put(event)
    update_queue_metrics()


async def publish_worker(worker_id: int) -> None:
    while True:
        event = None
        came_from_queue = False
        try:
            if offline_buffer and is_ready():
                event = offline_buffer.popleft()
                update_queue_metrics()
            else:
                event = await publish_queue.get()
                came_from_queue = True
                update_queue_metrics()

            payload = json.dumps(event)

            if BROKER_TYPE == "mqtt":
                if not publisher.is_connected():
                    await requeue_or_drop_event(event, "broker_unavailable")
                    await asyncio.sleep(DISCONNECTED_RETRY_DELAY_MS / 1000)
                    continue
                await publisher.publish(MQTT_TOPIC, payload)
            elif BROKER_TYPE == "kafka":
                if getattr(publisher, "producer", None) is None:
                    await requeue_or_drop_event(event, "broker_unavailable")
                    await asyncio.sleep(DISCONNECTED_RETRY_DELAY_MS / 1000)
                    continue
                await publisher.publish(KAFKA_TOPIC, key=event["device_id"], payload=payload)

            MSG_SENT.labels(broker_type=BROKER_TYPE, device_id=event["device_id"]).inc()
        except ConnectionError:
            if event is not None:
                await requeue_or_drop_event(event, "broker_unavailable")
            await asyncio.sleep(DISCONNECTED_RETRY_DELAY_MS / 1000)
        except Exception as exc:
            logger.debug("Publish worker %s failed to publish event: %s", worker_id, exc)
            if event is not None:
                await requeue_or_drop_event(event, "publish_failed")
            await asyncio.sleep(DISCONNECTED_RETRY_DELAY_MS / 1000)
        finally:
            if came_from_queue:
                publish_queue.task_done()
                update_queue_metrics()


@app.on_event("startup")
async def startup_event():
    global publisher, startup_error, publisher_tasks
    update_queue_metrics()
    SIMULATION_RUNNING.labels(broker_type=BROKER_TYPE).set(0)
    BROKER_CONNECTED.labels(broker_type=BROKER_TYPE).set(0)

    if BROKER_TYPE == "mqtt":
        publisher = MqttPublisher(
            MQTT_HOST,
            MQTT_PORT,
            MQTT_QOS,
            MQTT_KEEPALIVE_SEC,
            BROKER_PROBE_INTERVAL_MS,
        )
        publisher.connect()
    elif BROKER_TYPE == "kafka":
        publisher = KafkaPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_ACKS)
        publisher.connect()

        def poll_loop():
            while True:
                publisher.producer.poll(0.1)
                time.sleep(0.05)

        poller = threading.Thread(target=poll_loop, daemon=True)
        poller.start()
    else:
        startup_error = f"Unknown BROKER_TYPE: {BROKER_TYPE}"
        logger.error(startup_error)
        return

    publisher_tasks = [
        asyncio.create_task(publish_worker(worker_id))
        for worker_id in range(PUBLISH_WORKER_COUNT)
    ]


@app.on_event("shutdown")
async def shutdown_event():
    for task in sim_tasks:
        task.cancel()
    for task in publisher_tasks:
        task.cancel()
    if publisher_tasks:
        await asyncio.gather(*publisher_tasks, return_exceptions=True)


async def publish_event_safe(event: dict) -> None:
    await enqueue_event(event)


async def device_worker(device_id: str, interval_sec: float) -> None:
    logger.info("Worker started for device %s with interval %ss", device_id, interval_sec)
    while sim_running:
        event = generate_event(device_id=device_id)
        await publish_event_safe(event)
        await asyncio.sleep(interval_sec)


# =====================================
# API ENDPOINTS
# =====================================
@app.get("/health")
async def health(response: Response):
    ready = is_ready()
    response.status_code = 200 if ready else 503
    return {
        "status": "ok" if ready else "degraded",
        "ready": ready,
        "broker_type": BROKER_TYPE,
        "startup_error": startup_error,
    }


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
        "kafka_acks": KAFKA_ACKS if BROKER_TYPE == "kafka" else None,
        "publish_queue_depth": publish_queue.qsize(),
        "offline_buffer_depth": len(offline_buffer),
        "broker_ready": is_ready(),
    }


@app.post("/scenario/a/start")
async def start_scenario_a(devices: int = 100, interval: float = 1.0):
    global sim_running, sim_tasks
    if sim_running:
        return {"status": "already_running"}

    sim_running = True
    SIMULATION_RUNNING.labels(broker_type=BROKER_TYPE).set(1)
    sim_tasks = []

    device_list = [f"DEVICE-{i:05d}" for i in range(devices)]
    for device_id in device_list:
        sim_tasks.append(asyncio.create_task(device_worker(device_id, interval)))

    logger.info("Started Scenario A/B-style simulation: %s devices every %ss", devices, interval)
    return {"status": "started", "devices": devices, "interval": interval}


@app.post("/scenario/a/stop")
async def stop_scenario_a():
    global sim_running, sim_tasks
    if not sim_running:
        return {"status": "not_running"}

    sim_running = False
    SIMULATION_RUNNING.labels(broker_type=BROKER_TYPE).set(0)
    for task in sim_tasks:
        task.cancel()

    if sim_tasks:
        await asyncio.gather(*sim_tasks, return_exceptions=True)

    sim_tasks = []
    logger.info("Stopped Scenario A/B-style simulation")
    return {"status": "stopped"}


async def run_burst(rate: int, duration_sec: int) -> None:
    logger.info("Starting burst: %s msg/s for %ss", rate, duration_sec)
    total_to_send = rate * duration_sec
    delay = 1.0 / rate

    start = time.time()
    sent = 0
    while sent < total_to_send:
        await publish_event_safe(generate_event())
        sent += 1

        expected_elapsed = sent * delay
        actual_elapsed = time.time() - start
        if actual_elapsed < expected_elapsed:
            await asyncio.sleep(expected_elapsed - actual_elapsed)

    logger.info("Burst complete. Enqueued %s events in %.2fs", sent, time.time() - start)


@app.post("/scenario/c/trigger")
async def trigger_scenario_c(background_tasks: BackgroundTasks, rate: int = 5000, duration: int = 5):
    background_tasks.add_task(run_burst, rate, duration)
    return {"status": "burst_triggered", "target_rate": rate, "duration_sec": duration}


@app.post("/scenario/d/trigger")
async def trigger_scenario_d(count: int = 50):
    logger.info("Triggering Scenario D: sending %s critical temperature events", count)
    for _ in range(count):
        event = generate_event(critical_temp=True)
        event["sent_at"] = time.time()
        await publish_event_safe(event)
    return {"status": "alert_events_sent", "count": count}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
