import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Set, Tuple

import docker
from docker import errors as docker_errors
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import Gauge, REGISTRY, generate_latest


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("resource-monitor")


PORT = int(os.getenv("PORT", 8083))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "2"))
DOCKER_BASE_URL = os.getenv("DOCKER_BASE_URL", "unix:///var/run/docker.sock")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
STALE_AFTER_SECONDS = float(os.getenv("STALE_AFTER_SECONDS", str(max(POLL_INTERVAL_SECONDS * 10, 30.0))))


CPU_PERCENT = Gauge(
    "docker_container_cpu_percent",
    "Approximate Docker CPU usage percentage from docker stats snapshots",
    ["container_name", "compose_service", "image"],
)
MEMORY_USAGE_BYTES = Gauge(
    "docker_container_memory_usage_bytes",
    "Docker container memory usage in bytes",
    ["container_name", "compose_service", "image"],
)
MEMORY_WORKING_SET_BYTES = Gauge(
    "docker_container_memory_working_set_bytes",
    "Docker container working set memory in bytes",
    ["container_name", "compose_service", "image"],
)
NETWORK_RX_BYTES = Gauge(
    "docker_container_network_rx_bytes",
    "Total received bytes across container network interfaces",
    ["container_name", "compose_service", "image"],
)
NETWORK_TX_BYTES = Gauge(
    "docker_container_network_tx_bytes",
    "Total transmitted bytes across container network interfaces",
    ["container_name", "compose_service", "image"],
)
CONTAINER_UP = Gauge(
    "docker_container_up",
    "Whether the container was seen in the latest docker stats poll",
    ["container_name", "compose_service", "image"],
)
SCRAPE_SUCCESS = Gauge(
    "docker_stats_scrape_success",
    "Whether the latest docker stats scrape succeeded",
)
LAST_SUCCESS = Gauge(
    "docker_stats_last_success_timestamp_seconds",
    "Unix timestamp of the latest successful docker stats scrape",
)


app = FastAPI(title="Docker Resource Monitor")
docker_client = docker.DockerClient(base_url=DOCKER_BASE_URL)
previous_cpu: Dict[str, Tuple[float, float]] = {}
known_metric_labels: Set[Tuple[str, str, str]] = set()
last_error: Optional[str] = None
last_success_at: float = 0.0
monitor_thread: Optional[threading.Thread] = None


def calculate_cpu_percent(stats: dict, container_id: str) -> float:
    cpu_stats = stats.get("cpu_stats", {})
    cpu_usage = cpu_stats.get("cpu_usage", {})
    total_usage = float(cpu_usage.get("total_usage", 0.0))
    system_usage = float(cpu_stats.get("system_cpu_usage", 0.0))
    online_cpus = int(cpu_stats.get("online_cpus") or len(cpu_usage.get("percpu_usage", [])) or 1)

    previous_total, previous_system = previous_cpu.get(container_id, (total_usage, system_usage))
    previous_cpu[container_id] = (total_usage, system_usage)

    cpu_delta = total_usage - previous_total
    system_delta = system_usage - previous_system
    if cpu_delta <= 0 or system_delta <= 0:
        return 0.0

    return (cpu_delta / system_delta) * online_cpus * 100.0


def extract_memory_usage(stats: dict) -> Tuple[float, float]:
    memory_stats = stats.get("memory_stats", {})
    usage = float(memory_stats.get("usage", 0.0))
    stats_detail = memory_stats.get("stats", {})
    cache = float(stats_detail.get("inactive_file") or stats_detail.get("cache") or 0.0)
    working_set = max(usage - cache, 0.0)
    return usage, working_set


def extract_network_usage(stats: dict) -> Tuple[float, float]:
    networks = stats.get("networks", {})
    rx_total = 0.0
    tx_total = 0.0
    for details in networks.values():
        rx_total += float(details.get("rx_bytes", 0.0))
        tx_total += float(details.get("tx_bytes", 0.0))
    return rx_total, tx_total


def remove_stale_metrics(active_labels: Set[Tuple[str, str, str]]) -> None:
    stale_labels = known_metric_labels - active_labels
    for labels in stale_labels:
        CPU_PERCENT.remove(*labels)
        MEMORY_USAGE_BYTES.remove(*labels)
        MEMORY_WORKING_SET_BYTES.remove(*labels)
        NETWORK_RX_BYTES.remove(*labels)
        NETWORK_TX_BYTES.remove(*labels)
        CONTAINER_UP.remove(*labels)
    known_metric_labels.intersection_update(active_labels)


def is_not_found_error(exc: Exception) -> bool:
    if isinstance(exc, docker_errors.NotFound):
        return True
    if isinstance(exc, docker_errors.APIError):
        if getattr(exc, "status_code", None) == 404:
            return True
        return "No such container" in str(exc)
    return False


def fetch_container_snapshot(container) -> Optional[Tuple[str, str, str, str, dict]]:
    try:
        stats = container.stats(stream=False)
        labels = container.labels or {}
        compose_service = labels.get("com.docker.compose.service", "")
        image = container.image.tags[0] if container.image.tags else container.image.short_id
        return container.id, container.name, compose_service, image, stats
    except Exception as exc:  # noqa: BLE001
        if is_not_found_error(exc):
            logger.debug("Container disappeared during stats fetch: %s", getattr(container, "name", "unknown"))
            return None
        raise


def collect_stats() -> None:
    global last_error, last_success_at

    containers = docker_client.containers.list()
    active_labels: Set[Tuple[str, str, str]] = set()
    snapshots = []

    if containers:
        worker_count = min(len(containers), MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(fetch_container_snapshot, container) for container in containers]
            for future in as_completed(futures):
                try:
                    snapshot = future.result()
                except Exception as exc:  # noqa: BLE001
                    if is_not_found_error(exc):
                        continue
                    raise
                if snapshot is not None:
                    snapshots.append(snapshot)

    for container_id, container_name, compose_service, image, stats in sorted(snapshots, key=lambda item: item[1]):
        metric_labels = (container_name, compose_service, image)
        active_labels.add(metric_labels)
        known_metric_labels.add(metric_labels)

        cpu_percent = calculate_cpu_percent(stats, container_id)
        memory_usage, memory_working_set = extract_memory_usage(stats)
        network_rx, network_tx = extract_network_usage(stats)

        CPU_PERCENT.labels(*metric_labels).set(cpu_percent)
        MEMORY_USAGE_BYTES.labels(*metric_labels).set(memory_usage)
        MEMORY_WORKING_SET_BYTES.labels(*metric_labels).set(memory_working_set)
        NETWORK_RX_BYTES.labels(*metric_labels).set(network_rx)
        NETWORK_TX_BYTES.labels(*metric_labels).set(network_tx)
        CONTAINER_UP.labels(*metric_labels).set(1)

    remove_stale_metrics(active_labels)
    last_error = None
    last_success_at = time.time()
    SCRAPE_SUCCESS.set(1)
    LAST_SUCCESS.set(last_success_at)


def collection_loop() -> None:
    global last_error
    while True:
        started_at = time.time()
        try:
            collect_stats()
        except Exception as exc:
            last_error = str(exc)
            SCRAPE_SUCCESS.set(0)
            logger.error("Docker stats scrape failed: %s", exc)
        elapsed = time.time() - started_at
        time.sleep(max(POLL_INTERVAL_SECONDS - elapsed, 0.1))


def is_ready() -> bool:
    if not last_success_at:
        return False
    return (time.time() - last_success_at) <= STALE_AFTER_SECONDS


@app.on_event("startup")
def startup_event() -> None:
    global monitor_thread
    if monitor_thread and monitor_thread.is_alive():
        return
    monitor_thread = threading.Thread(target=collection_loop, daemon=True)
    monitor_thread.start()


@app.get("/health")
async def health(response: Response):
    ready = is_ready()
    response.status_code = 200 if ready else 503
    return {
        "status": "ok" if ready else "degraded",
        "ready": ready,
        "last_success_at": last_success_at,
        "last_error": last_error,
    }


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return generate_latest(REGISTRY)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
