import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import Optional

from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, REGISTRY, generate_latest
import uvicorn

from generator import DEVICE_IDS, generate_event


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rfid-generator")


MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_KEEPALIVE_SEC = int(os.getenv("MQTT_KEEPALIVE_SEC", "30"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "rfid/events")
PUBLISH_INTERVAL_MS = max(int(os.getenv("PUBLISH_INTERVAL_MS", "1000")), 100)
PUBLISH_INTERVAL_SEC = PUBLISH_INTERVAL_MS / 1000.0


EVENTS_GENERATED = Counter(
    "generator_events_generated_total",
    "Total number of RFID events generated.",
)
EVENTS_PUBLISHED = Counter(
    "generator_events_published_total",
    "Total number of RFID events published to MQTT.",
)
PUBLISH_ERRORS = Counter(
    "generator_publish_errors_total",
    "Total number of publish errors.",
)
BROKER_CONNECTED = Gauge(
    "generator_broker_connected",
    "Whether the generator is currently connected to MQTT.",
)

publisher_client: Optional[mqtt.Client] = None
publish_task: Optional[asyncio.Task] = None
publisher_connected = False
startup_error: Optional[str] = None
state = {
    "generated": 0,
    "published": 0,
    "errors": 0,
    "last_event_timestamp": None,
}


def on_connect(client, userdata, flags, reason_code, properties):  # noqa: ANN001
    del client, userdata, flags, properties
    global publisher_connected
    publisher_connected = reason_code == 0
    BROKER_CONNECTED.set(1 if publisher_connected else 0)
    if publisher_connected:
        logger.info("Connected to MQTT broker at %s:%s", MQTT_HOST, MQTT_PORT)
    else:
        logger.error("MQTT connection failed with result code %s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):  # noqa: ANN001
    del client, userdata, disconnect_flags, properties
    global publisher_connected
    publisher_connected = False
    BROKER_CONNECTED.set(0)
    logger.warning("Disconnected from MQTT broker with result code %s", reason_code)


def build_status() -> dict:
    return {
        "service": "generator",
        "ready": publisher_connected and startup_error is None,
        "mqtt": {
            "host": MQTT_HOST,
            "port": MQTT_PORT,
            "topic": MQTT_TOPIC,
            "qos": MQTT_QOS,
        },
        "publish_interval_ms": PUBLISH_INTERVAL_MS,
        "active_devices": len(DEVICE_IDS),
        "generated_events": state["generated"],
        "published_events": state["published"],
        "publish_errors": state["errors"],
        "last_event_timestamp": state["last_event_timestamp"],
        "startup_error": startup_error,
    }


def publish_payload(payload: str) -> None:
    if publisher_client is None or not publisher_connected:
        raise ConnectionError("MQTT broker is not connected")

    message_info = publisher_client.publish(MQTT_TOPIC, payload, qos=MQTT_QOS)
    if message_info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"MQTT publish failed with code {message_info.rc}")

    if MQTT_QOS > 0:
        message_info.wait_for_publish()


async def publishing_loop() -> None:
    while True:
        if not publisher_connected:
            await asyncio.sleep(0.25)
            continue

        event = generate_event()
        payload = json.dumps(event)

        state["generated"] += 1
        state["last_event_timestamp"] = event["timestamp"]
        EVENTS_GENERATED.inc()

        try:
            await asyncio.to_thread(publish_payload, payload)
            state["published"] += 1
            EVENTS_PUBLISHED.inc()
        except Exception as exc:
            state["errors"] += 1
            PUBLISH_ERRORS.inc()
            logger.warning("Failed to publish RFID event: %s", exc)

        await asyncio.sleep(PUBLISH_INTERVAL_SEC)


@asynccontextmanager
async def lifespan(app: FastAPI):
    del app
    global publisher_client, publish_task, startup_error

    try:
        publisher_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="rfid-generator",
            protocol=mqtt.MQTTv311,
        )
        publisher_client.on_connect = on_connect
        publisher_client.on_disconnect = on_disconnect
        publisher_client.reconnect_delay_set(min_delay=1, max_delay=5)
        publisher_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE_SEC)
        publisher_client.loop_start()
        publish_task = asyncio.create_task(publishing_loop())
        logger.info("RFID generator started with interval %sms", PUBLISH_INTERVAL_MS)
    except Exception as exc:
        startup_error = str(exc)
        logger.exception("Failed to start RFID generator")
    try:
        yield
    finally:
        if publish_task is not None:
            publish_task.cancel()
            with suppress(asyncio.CancelledError):
                await publish_task

        if publisher_client is not None:
            publisher_client.loop_stop()
            publisher_client.disconnect()


app = FastAPI(title="RFID Generator Service", lifespan=lifespan)


@app.get("/health")
async def health(response: Response) -> dict:
    ready = publisher_connected and startup_error is None
    response.status_code = 200 if ready else 503
    return {
        "status": "ok" if ready else "degraded",
        "ready": ready,
        "broker": "mosquitto",
        "topic": MQTT_TOPIC,
        "startup_error": startup_error,
    }


@app.get("/status")
async def status() -> dict:
    return build_status()


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> bytes:
    return generate_latest(REGISTRY)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
