"""
Dedicated Scenario B runner.

Scenario B simulates a 30 second edge network outage and compares broker
recovery in two distinct modes:
- tool_benchmark: dedicated benchmark tools act as the device simulator
- app_buffered: the existing data-ingestion service acts as the simulator

The runner restarts the stack for every test profile, disconnects the active
simulator container, captures recovery metrics, exports JSON results, and
generates Markdown artifacts that are ready to reuse in the report.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_PATH = Path(__file__).resolve().parent / "scenario_b_results.json"

INGESTION_URL = "http://localhost:8000"
STORAGE_URL = "http://localhost:8001"
ANALYTICS_URL = "http://localhost:8002"
RESOURCE_MONITOR_URL = "http://localhost:8083"

MQTT_TOPIC = "iot/events"
KAFKA_TOPIC = "iot-events"
MQTT_TOOL_IMAGE = "emqx/emqtt-bench:latest"
KAFKA_IMAGE = "apache/kafka:3.7.0"
BROKER_CONTAINER_NAME = {
    "mqtt": "mqtt-broker",
    "kafka": "kafka-broker",
}
PROJECT_CONTAINER_NAMES = (
    "db",
    "mqtt-broker",
    "kafka-broker",
    "data-ingestion",
    "data-storage",
    "analytics-service",
    "resource-monitor",
    "prometheus",
    "grafana",
)
KAFKA_LAG_GROUPS = ("data-storage-group", "analytics-group")
MESSAGE_LATENCY_HISTOGRAM = "analytics_message_e2e_latency_ms"
MESSAGE_LATENCY_MAX_GAUGE = "analytics_message_e2e_latency_max_ms"

KAFKA_SUMMARY_RE = re.compile(
    r"(?P<records>\d+)\s+records sent,\s+"
    r"(?P<records_per_sec>[0-9.]+)\s+records/sec(?:\s+\([0-9.]+\s+MB/sec\))?,\s+"
    r"(?P<avg_latency_ms>[0-9.]+)\s+ms avg latency,\s+"
    r"(?P<max_latency_ms>[0-9.]+)\s+ms max latency"
    r"(?:,\s+(?P<p50_latency_ms>[0-9.]+)\s+ms 50th,\s+"
    r"(?P<p95_latency_ms>[0-9.]+)\s+ms 95th,\s+"
    r"(?P<p99_latency_ms>[0-9.]+)\s+ms 99th,\s+"
    r"(?P<p999_latency_ms>[0-9.]+)\s+ms 99.9th\.)?"
)
PROM_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$"
)
MQTT_PUB_SUCC_RE = re.compile(r"pub_succ total=(?P<count>\d+)")
MQTT_PUB_TOTAL_RE = re.compile(r"pub total=(?P<count>\d+)")
TRANSIENT_HTTP_EXCEPTIONS = (
    urllib.error.URLError,
    urllib.error.HTTPError,
    TimeoutError,
    OSError,
    RemoteDisconnected,
)
TRANSIENT_STACK_ERROR_MARKERS = (
    "name is already in use",
    "container is marked for removal",
    "No such container",
    "dependency failed to start: container resource-monitor exited",
)


@dataclass(frozen=True)
class ScenarioBProfile:
    broker: str
    mode: str
    devices: int
    interval_sec: float
    warmup_sec: int
    outage_sec: int
    post_reconnect_run_sec: int
    qos: Optional[int] = None
    acks: Optional[str] = None
    topic_partitions: Optional[int] = None

    @property
    def config_name(self) -> str:
        if self.broker == "mqtt":
            return f"{self.mode}_mqtt_qos_{self.qos}_outage"
        return f"{self.mode}_kafka_acks_{self.acks}_partitions_{self.topic_partitions}_outage"

    @property
    def broker_value(self) -> str:
        return str(self.qos) if self.broker == "mqtt" else str(self.acks)

    @property
    def target_throughput(self) -> float:
        return self.devices / self.interval_sec if self.interval_sec > 0 else 0.0

    @property
    def planned_publish_window_sec(self) -> int:
        return self.warmup_sec + self.outage_sec + self.post_reconnect_run_sec

    @property
    def planned_messages(self) -> int:
        return max(1, math.ceil(self.target_throughput * self.planned_publish_window_sec))

    @property
    def simulator_container_name(self) -> str:
        if self.mode == "tool_benchmark":
            if self.broker == "mqtt":
                return f"scenario-b-mqtt-publisher-{self.qos}-{int(time.time() * 1000)}"
            return f"scenario-b-kafka-publisher-{self.acks}-{self.topic_partitions}-{int(time.time() * 1000)}"
        return "data-ingestion"


def run_cmd(
    cmd: List[str],
    *,
    env_overrides: Optional[Dict[str, str]] = None,
    timeout: int = 300,
    check: bool = True,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

    if check and result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or "(no output)"
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(cmd)}\n{detail}")
    return result


def fetch_text(url: str, timeout: int = 10) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def fetch_json(url: str, timeout: int = 10) -> Dict[str, object]:
    return json.loads(fetch_text(url, timeout=timeout))


def try_fetch_json(url: str, timeout: int = 10) -> Optional[Dict[str, object]]:
    try:
        return fetch_json(url, timeout=timeout)
    except (*TRANSIENT_HTTP_EXCEPTIONS, json.JSONDecodeError):
        return None


def parse_labels(raw_labels: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    if not raw_labels:
        return labels
    for part in raw_labels.split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        labels[key.strip()] = value.strip().strip('"')
    return labels


def parse_prometheus_counter(metrics_text: str, metric_name: str) -> float:
    total = 0.0
    found = False
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line.strip())
        if not match or match.group("name") != metric_name:
            continue
        total += float(match.group("value"))
        found = True
    return total if found else 0.0


def find_metric_value(metrics_text: str, metric_name: str, label_filters: Optional[Dict[str, str]] = None) -> float:
    label_filters = label_filters or {}
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line.strip())
        if not match or match.group("name") != metric_name:
            continue
        labels = parse_labels(match.group("labels") or "")
        if any(labels.get(key) != value for key, value in label_filters.items()):
            continue
        return float(match.group("value"))
    return 0.0


def collect_metric_samples(
    metrics_text: str,
    metric_name: str,
    label_filters: Optional[Dict[str, str]] = None,
) -> List[Dict[str, object]]:
    label_filters = label_filters or {}
    samples: List[Dict[str, object]] = []
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line.strip())
        if not match or match.group("name") != metric_name:
            continue
        labels = parse_labels(match.group("labels") or "")
        if any(labels.get(key) != value for key, value in label_filters.items()):
            continue
        samples.append(
            {
                "labels": labels,
                "value": float(match.group("value")),
            }
        )
    return samples


def percentile(values: Iterable[float], rank_pct: float) -> Optional[float]:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, math.ceil((rank_pct / 100.0) * len(ordered)) - 1)
    return ordered[index]


def compute_latency_summary(latencies_ms: List[float]) -> Dict[str, Optional[float]]:
    if not latencies_ms:
        return {
            "avg_latency_ms": None,
            "p95_latency_ms": None,
            "max_latency_ms": None,
            "observations": 0,
        }

    avg_latency_ms = sum(latencies_ms) / len(latencies_ms)
    return {
        "avg_latency_ms": round(avg_latency_ms, 3),
        "p95_latency_ms": round(percentile(latencies_ms, 95.0) or 0.0, 3),
        "max_latency_ms": round(max(latencies_ms), 3),
        "observations": len(latencies_ms),
    }


def parse_histogram(metrics_text: str, metric_name: str) -> Dict[str, object]:
    buckets: Dict[float, float] = {}
    count = 0.0
    total_sum = 0.0

    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line.strip())
        if not match:
            continue
        name = match.group("name")
        labels = parse_labels(match.group("labels") or "")
        value = float(match.group("value"))

        if name == f"{metric_name}_bucket" and "le" in labels and labels["le"] != "+Inf":
            try:
                buckets[float(labels["le"])] = value
            except ValueError:
                continue
        elif name == f"{metric_name}_count":
            count = value
        elif name == f"{metric_name}_sum":
            total_sum = value

    ordered_buckets = sorted(buckets.items(), key=lambda item: item[0])
    return {
        "buckets": ordered_buckets,
        "count": count,
        "sum": total_sum,
    }


def histogram_quantile(histogram: Dict[str, object], quantile: float) -> Optional[float]:
    count = float(histogram.get("count") or 0.0)
    buckets = list(histogram.get("buckets") or [])
    if count <= 0 or not buckets:
        return None

    target = count * quantile
    previous_boundary = 0.0
    previous_count = 0.0
    for boundary, cumulative_count in buckets:
        if cumulative_count >= target:
            bucket_count = cumulative_count - previous_count
            if bucket_count <= 0:
                return boundary
            fraction = (target - previous_count) / bucket_count
            return previous_boundary + ((boundary - previous_boundary) * fraction)
        previous_boundary = boundary
        previous_count = cumulative_count
    return buckets[-1][0]


def histogram_max_upper_bound(histogram: Dict[str, object]) -> Optional[float]:
    count = float(histogram.get("count") or 0.0)
    buckets = list(histogram.get("buckets") or [])
    if count <= 0 or not buckets:
        return None

    for boundary, cumulative_count in buckets:
        if cumulative_count >= count:
            return boundary
    return buckets[-1][0]


def compute_histogram_latency_summary(histogram: Dict[str, object]) -> Dict[str, Optional[float]]:
    count = float(histogram.get("count") or 0.0)
    total_sum = float(histogram.get("sum") or 0.0)
    if count <= 0:
        return {
            "avg_latency_ms": None,
            "p95_latency_ms": None,
            "max_latency_ms": None,
            "observations": 0,
        }

    return {
        "avg_latency_ms": round(total_sum / count, 3),
        "p95_latency_ms": round(histogram_quantile(histogram, 0.95) or 0.0, 3),
        "max_latency_ms": round(histogram_max_upper_bound(histogram) or 0.0, 3),
        "observations": int(count),
    }


def parse_latency_summary(metrics_text: str, broker: str) -> Dict[str, Optional[float]]:
    bucket_samples = collect_metric_samples(
        metrics_text,
        f"{MESSAGE_LATENCY_HISTOGRAM}_bucket",
        {"broker_type": broker},
    )
    buckets: List[Dict[str, float]] = []
    for sample in bucket_samples:
        le_raw = str(sample["labels"].get("le"))
        le = float("inf") if le_raw == "+Inf" else float(le_raw)
        buckets.append({"le": le, "count": float(sample["value"])})

    count = find_metric_value(
        metrics_text,
        f"{MESSAGE_LATENCY_HISTOGRAM}_count",
        {"broker_type": broker},
    )
    latency_sum = find_metric_value(
        metrics_text,
        f"{MESSAGE_LATENCY_HISTOGRAM}_sum",
        {"broker_type": broker},
    )
    max_latency = find_metric_value(
        metrics_text,
        MESSAGE_LATENCY_MAX_GAUGE,
        {"broker_type": broker},
    )

    avg_latency = round(latency_sum / count, 3) if count else None
    p95_latency = None
    if buckets:
        sorted_buckets = sorted(buckets, key=lambda item: item["le"])
        total = sorted_buckets[-1]["count"]
        if total > 0:
            target = total * 0.95
            previous_count = 0.0
            previous_le = 0.0
            for bucket in sorted_buckets:
                bucket_le = bucket["le"]
                bucket_count = bucket["count"]
                if bucket_count >= target:
                    if bucket_le == float("inf"):
                        p95_latency = round(previous_le, 3) if previous_le > 0 else None
                    else:
                        bucket_delta = bucket_count - previous_count
                        if bucket_delta <= 0:
                            p95_latency = round(bucket_le, 3)
                        else:
                            interpolation = (target - previous_count) / bucket_delta
                            p95_latency = round(previous_le + ((bucket_le - previous_le) * interpolation), 3)
                    break
                previous_count = bucket_count
                if bucket_le != float("inf"):
                    previous_le = bucket_le

    if p95_latency is not None and max_latency and p95_latency > max_latency:
        p95_latency = round(max_latency, 3)

    return {
        "avg_latency_ms": avg_latency,
        "p95_latency_ms": p95_latency,
        "max_latency_ms": round(max_latency, 3) if max_latency else None,
        "latency_sample_count": int(count),
    }


def build_payload_dir() -> tempfile.TemporaryDirectory:
    temp_dir = tempfile.TemporaryDirectory(prefix="scenario-b-bench-")
    payload_root = Path(temp_dir.name)

    mqtt_template = {
        "event_id": "scenario-b-mqtt-%UNIQUE%",
        "timestamp": "2026-01-01T00:00:00Z",
        "emitted_at_ms": "%TIMESTAMPMS%",
        "device_id": "B-EDGE-%RANDOM%",
        "card_uid": "AA:BB:CC:DD",
        "access_granted": True,
        "door_id": "MAIN_GATE",
        "zone": "GROUND_FLOOR",
        "signal_strength": -52,
        "battery_voltage": 3.85,
        "response_time_ms": 18,
        "event_type": "ENTRY",
        "temperature": 23.4,
    }
    (payload_root / "mqtt_payload_template.json").write_text(
        json.dumps(mqtt_template, separators=(",", ":")),
        encoding="utf-8",
    )

    kafka_lines: List[str] = []
    for index in range(256):
        kafka_lines.append(
            json.dumps(
                {
                    "event_id": f"scenario-b-kafka-{index}",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "device_id": f"B-EDGE-{index % 64:04d}",
                    "card_uid": f"AA:BB:CC:{index % 100:02d}",
                    "access_granted": True,
                    "door_id": "MAIN_GATE",
                    "zone": "GROUND_FLOOR",
                    "signal_strength": -55,
                    "battery_voltage": 3.9,
                    "response_time_ms": 20 + (index % 5),
                    "event_type": "ENTRY",
                    "temperature": 24.0 + ((index % 7) * 0.1),
                },
                separators=(",", ":"),
            )
        )
    (payload_root / "kafka_payloads.txt").write_text("\n".join(kafka_lines), encoding="utf-8")
    return temp_dir


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def wait_for_probe_metrics(port: int, timeout_sec: int = 60) -> None:
    deadline = time.time() + timeout_sec
    probe_url = f"http://localhost:{port}/metrics"
    while time.time() < deadline:
        try:
            fetch_text(probe_url, timeout=5)
            time.sleep(1)
            return
        except TRANSIENT_HTTP_EXCEPTIONS:
            time.sleep(1)
    raise TimeoutError(f"MQTT latency probe did not expose metrics on port {port}.")


def read_probe_recv_count(port: int) -> float:
    metrics_text = fetch_text(f"http://localhost:{port}/metrics")
    return find_metric_value(metrics_text, "recv")


def wait_for_probe_receipts(
    *,
    port: int,
    expected_messages: int,
    timeout_sec: int,
    stable_polls: int = 3,
) -> Dict[str, object]:
    deadline = time.time() + timeout_sec
    last_recv = -1.0
    stable_count = 0
    started_at = time.time()

    while time.time() < deadline:
        try:
            current_recv = read_probe_recv_count(port)
        except TRANSIENT_HTTP_EXCEPTIONS:
            time.sleep(1)
            continue

        if current_recv >= expected_messages:
            return {
                "received_messages": int(current_recv),
                "settled": True,
                "settle_reason": "all_probe_messages_observed",
                "completion_sec": round(time.time() - started_at, 3),
            }

        if math.isclose(current_recv, last_recv):
            stable_count += 1
        else:
            stable_count = 0
            last_recv = current_recv

        if stable_count >= stable_polls:
            return {
                "received_messages": int(current_recv),
                "settled": True,
                "settle_reason": "probe_counters_stable",
                "completion_sec": round(time.time() - started_at, 3),
            }
        time.sleep(1)

    try:
        final_recv = read_probe_recv_count(port)
    except TRANSIENT_HTTP_EXCEPTIONS:
        final_recv = last_recv if last_recv >= 0 else 0.0
    return {
        "received_messages": int(final_recv),
        "settled": False,
        "settle_reason": "timeout",
        "completion_sec": round(time.time() - started_at, 3),
    }


def start_mqtt_latency_probe(profile: ScenarioBProfile) -> Dict[str, object]:
    port = find_free_port()
    container_name = f"scenario-b-mqtt-probe-{profile.qos}-{int(time.time() * 1000)}"

    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        container_name,
        "--network",
        "iot_network",
        "-p",
        f"{port}:{port}",
        MQTT_TOOL_IMAGE,
        "sub",
        "-A",
        "true",
        "-h",
        "mqtt-broker",
        "-p",
        "1883",
        "-V",
        "4",
        "-c",
        "1",
        "-t",
        MQTT_TOPIC,
        "-q",
        str(profile.qos or 0),
        "--payload-hdrs",
        "ts",
        "-Q",
        "true",
        "--prometheus",
        "--restapi",
        str(port),
        "--log_to",
        "null",
    ]
    run_cmd(cmd, timeout=120)
    wait_for_probe_metrics(port)
    return {
        "container_name": container_name,
        "port": port,
    }


def stop_container(container_name: str) -> None:
    run_cmd(["docker", "rm", "-f", container_name], timeout=60, check=False)


def cleanup_scenario_b_tool_containers() -> None:
    result = run_cmd(["docker", "ps", "-aq", "--filter", "name=scenario-b-"], timeout=60, check=False)
    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    for container_id in container_ids:
        run_cmd(["docker", "rm", "-f", container_id], timeout=60, check=False)


def cleanup_stack_residue() -> None:
    for container_name in PROJECT_CONTAINER_NAMES:
        stop_container(container_name)
    cleanup_scenario_b_tool_containers()


def is_transient_stack_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker.lower() in message for marker in TRANSIENT_STACK_ERROR_MARKERS)


def parse_kafka_summary(output: str) -> Dict[str, float]:
    matches = list(KAFKA_SUMMARY_RE.finditer(output))
    if not matches:
        return {}

    match = matches[-1]
    summary: Dict[str, float] = {
        "records": float(match.group("records")),
        "records_per_sec": float(match.group("records_per_sec")),
        "avg_latency_ms": float(match.group("avg_latency_ms")),
        "max_latency_ms": float(match.group("max_latency_ms")),
    }
    for field_name in ("p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "p999_latency_ms"):
        value = match.group(field_name)
        if value is not None:
            summary[field_name] = float(value)
    return summary


def parse_kafka_progress_records(output: str) -> int:
    matches = list(KAFKA_SUMMARY_RE.finditer(output))
    return int(sum(int(match.group("records")) for match in matches))


def parse_mqtt_progress_records(output: str) -> int:
    success_matches = [int(match.group("count")) for match in MQTT_PUB_SUCC_RE.finditer(output)]
    if success_matches:
        return success_matches[-1]
    total_matches = [int(match.group("count")) for match in MQTT_PUB_TOTAL_RE.finditer(output)]
    return total_matches[-1] if total_matches else 0


def docker_logs(container_name: str) -> str:
    result = run_cmd(["docker", "logs", container_name], timeout=60, check=False)
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    return f"{stdout}\n{stderr}".strip()


def docker_container_running(container_name: str) -> bool:
    result = run_cmd(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
        timeout=30,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip().lower() == "true"


def wait_for_services(timeout_sec: int = 180) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        statuses = [
            try_fetch_json(f"{INGESTION_URL}/health"),
            try_fetch_json(f"{STORAGE_URL}/health"),
            try_fetch_json(f"{ANALYTICS_URL}/health"),
            try_fetch_json(f"{RESOURCE_MONITOR_URL}/health"),
        ]
        if all(status and status.get("ready") is True for status in statuses):
            time.sleep(2)
            return
        time.sleep(2)
    raise TimeoutError("Services did not become ready in time.")


def wait_for_ingestion_ready(timeout_sec: int = 120) -> float:
    started = time.time()
    deadline = started + timeout_sec
    while time.time() < deadline:
        try:
            status = fetch_json(f"{INGESTION_URL}/health")
            if status.get("ready") is True:
                return round(time.time() - started, 3)
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    return round(timeout_sec, 3)


def restart_stack(profile: ScenarioBProfile, *, disable_db_write: bool, build_images: bool) -> None:
    env_overrides = {
        "BROKER_TYPE": profile.broker,
        "DISABLE_DB_WRITE": "true" if disable_db_write else "false",
        "MQTT_QOS": str(profile.qos or 0),
        "KAFKA_ACKS": str(profile.acks or "1"),
        "KAFKA_TOPIC_PARTITIONS": str(profile.topic_partitions or 1),
        "PUBLISH_QUEUE_MAX_SIZE": "200000",
        "PUBLISH_WORKER_COUNT": "8",
        "OFFLINE_BUFFER_MAX_SIZE": "200000",
        "DISCONNECTED_RETRY_DELAY_MS": "250",
    }
    max_attempts = 3
    up_cmd = ["docker", "compose", "up", "-d"]
    if build_images:
        up_cmd.append("--build")

    for attempt in range(1, max_attempts + 1):
        run_cmd(["docker", "compose", "down", "--remove-orphans"], env_overrides=env_overrides, timeout=180, check=False)
        cleanup_stack_residue()
        time.sleep(3)
        try:
            run_cmd(up_cmd, env_overrides=env_overrides, timeout=900)
            wait_for_services()
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_attempts or not is_transient_stack_error(exc):
                raise
            print(f"Transient stack startup issue on attempt {attempt}/{max_attempts}: {exc}", file=sys.stderr)
            cleanup_stack_residue()
            time.sleep(5)


def fetch_ingestion_text(path: str, timeout: int = 10) -> str:
    script = (
        "import urllib.request; "
        f"print(urllib.request.urlopen('http://localhost:8000{path}', timeout={timeout}).read().decode('utf-8'))"
    )
    result = run_cmd(
        ["docker", "exec", "data-ingestion", "python", "-c", script],
        timeout=timeout + 20,
    )
    return result.stdout


def fetch_ingestion_json(path: str, timeout: int = 10) -> Dict[str, object]:
    return json.loads(fetch_ingestion_text(path, timeout=timeout))


def ingestion_api_request(path: str, method: str = "GET", timeout: int = 20) -> Dict[str, object]:
    script = (
        "import urllib.request; "
        f"req = urllib.request.Request('http://localhost:8000{path}', method='{method}'); "
        f"print(urllib.request.urlopen(req, timeout={timeout}).read().decode('utf-8'))"
    )
    result = run_cmd(
        ["docker", "exec", "data-ingestion", "python", "-c", script],
        timeout=timeout + 20,
    )
    output = result.stdout.strip()
    return json.loads(output) if output else {}


def ingestion_api_request_with_retries(
    path: str,
    method: str = "GET",
    retries: int = 5,
    delay_sec: float = 1.0,
) -> Dict[str, object]:
    last_error = None
    for attempt in range(retries):
        try:
            return ingestion_api_request(path, method=method)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries - 1:
                time.sleep(delay_sec)
    raise last_error  # type: ignore[misc]


def get_tool_sent_messages(profile: ScenarioBProfile, publisher: Dict[str, object]) -> int:
    logs = docker_logs(str(publisher["container_name"]))
    if profile.broker == "mqtt":
        return parse_mqtt_progress_records(logs)
    return parse_kafka_progress_records(logs)


def get_runtime_snapshot(profile: ScenarioBProfile, publisher: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    retries = 5
    delay_sec = 1.0
    last_error = None

    for attempt in range(retries):
        try:
            storage_metrics = fetch_text(f"{STORAGE_URL}/metrics")
            analytics_metrics = fetch_text(f"{ANALYTICS_URL}/metrics")
            break
        except (*TRANSIENT_HTTP_EXCEPTIONS, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(delay_sec)
    else:
        raise last_error  # type: ignore[misc]

    if profile.mode == "app_buffered":
        ingestion_metrics = fetch_ingestion_text("/metrics")
        status = fetch_ingestion_json("/status")
        broker_label = {"broker_type": profile.broker}
        return {
            "timestamp": round(time.time(), 3),
            "generated_messages": int(parse_prometheus_counter(ingestion_metrics, "ingestion_messages_generated_total")),
            "sent_messages": int(parse_prometheus_counter(ingestion_metrics, "ingestion_messages_sent_total")),
            "dropped_messages": int(parse_prometheus_counter(ingestion_metrics, "ingestion_messages_dropped_total")),
            "send_errors": int(parse_prometheus_counter(ingestion_metrics, "ingestion_send_errors_total")),
            "publish_queue_depth": int(
                find_metric_value(ingestion_metrics, "ingestion_publish_queue_depth", broker_label)
            ),
            "offline_buffer_depth": int(
                find_metric_value(ingestion_metrics, "ingestion_offline_buffer_depth", broker_label)
            ),
            "storage_received_messages": int(parse_prometheus_counter(storage_metrics, "storage_messages_received_total")),
            "analytics_processed_messages": int(
                parse_prometheus_counter(analytics_metrics, "analytics_messages_processed_total")
            ),
            "simulation_running": bool(status.get("simulation_running")),
            "broker_ready": bool(status.get("broker_ready")),
        }

    if publisher is None:
        raise ValueError("tool_benchmark mode requires publisher state")

    sent_messages = get_tool_sent_messages(profile, publisher)
    return {
        "timestamp": round(time.time(), 3),
        "generated_messages": sent_messages,
        "sent_messages": sent_messages,
        "dropped_messages": 0,
        "send_errors": 0,
        "publish_queue_depth": 0,
        "offline_buffer_depth": 0,
        "storage_received_messages": int(parse_prometheus_counter(storage_metrics, "storage_messages_received_total")),
        "analytics_processed_messages": int(parse_prometheus_counter(analytics_metrics, "analytics_messages_processed_total")),
        "simulation_running": docker_container_running(str(publisher["container_name"])),
        "broker_ready": docker_container_running(str(publisher["container_name"])),
    }


def diff_snapshots(end: Dict[str, object], start: Dict[str, object]) -> Dict[str, object]:
    numeric_keys = [
        "generated_messages",
        "sent_messages",
        "dropped_messages",
        "send_errors",
        "storage_received_messages",
        "analytics_processed_messages",
    ]
    delta = {key: int(end[key]) - int(start[key]) for key in numeric_keys}
    delta["publish_queue_depth_end"] = int(end["publish_queue_depth"])
    delta["offline_buffer_depth_end"] = int(end["offline_buffer_depth"])
    return delta


def start_app_simulation(profile: ScenarioBProfile) -> None:
    ingestion_api_request_with_retries(
        f"/scenario/a/start?devices={profile.devices}&interval={profile.interval_sec}",
        method="POST",
    )


def stop_app_simulation() -> None:
    ingestion_api_request_with_retries("/scenario/a/stop", method="POST")


def start_tool_publisher(profile: ScenarioBProfile, payload_dir: Path) -> Dict[str, object]:
    container_name = profile.simulator_container_name
    publisher: Dict[str, object] = {
        "container_name": container_name,
        "planned_messages": profile.planned_messages,
        "latency_source": None,
    }

    if profile.broker == "mqtt":
        probe = start_mqtt_latency_probe(profile)
        connect_rate = max(1, min(profile.devices, 1000))
        interval_ms = max(1, round(profile.interval_sec * 1000))
        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--network",
            "iot_network",
            "-v",
            f"{payload_dir.resolve()}:/payloads:ro",
            MQTT_TOOL_IMAGE,
            "pub",
            "-A",
            "true",
            "-h",
            "mqtt-broker",
            "-p",
            "1883",
            "-V",
            "4",
            "-c",
            str(profile.devices),
            "-R",
            str(connect_rate),
            "-I",
            str(interval_ms),
            "-t",
            MQTT_TOPIC,
            "-q",
            str(profile.qos or 0),
            "-L",
            str(profile.planned_messages),
            "-w",
            "true",
            "--payload-hdrs",
            "ts",
            "-m",
            "template:///payloads/mqtt_payload_template.json",
        ]
        if (profile.qos or 0) > 0:
            cmd.extend(["-F", "20"])
        run_cmd(cmd, timeout=120)
        publisher["probe"] = probe
        publisher["latency_source"] = "emqtt-bench_e2e_latency_histogram"
        return publisher

    throughput = max(1, round(profile.target_throughput))
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        container_name,
        "--network",
        "iot_network",
        "-v",
        f"{payload_dir.resolve()}:/payloads:ro",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic",
        KAFKA_TOPIC,
        "--num-records",
        str(profile.planned_messages),
        "--throughput",
        str(throughput),
        "--payload-file",
        "/payloads/kafka_payloads.txt",
        "--producer-props",
        "bootstrap.servers=kafka-broker:29092",
        f"acks={profile.acks}",
        "linger.ms=0",
        "batch.size=16384",
        "retries=2147483647",
        "delivery.timeout.ms=180000",
        "request.timeout.ms=30000",
        "max.block.ms=180000",
        "reconnect.backoff.ms=1000",
        "reconnect.backoff.max.ms=5000",
    ]
    run_cmd(cmd, timeout=120)
    publisher["latency_source"] = "kafka-producer-perf-test_summary"
    return publisher


def wait_for_tool_completion(profile: ScenarioBProfile, publisher: Dict[str, object]) -> Dict[str, object]:
    container_name = str(publisher["container_name"])
    wait_timeout = max(180, profile.planned_publish_window_sec * 8)
    wait_result = run_cmd(["docker", "wait", container_name], timeout=wait_timeout, check=False)
    exit_code = int(wait_result.stdout.strip() or "1")
    logs_text = docker_logs(container_name)

    result: Dict[str, object] = {
        "messages_sent": 0,
        "tool_stdout": logs_text,
        "tool_stderr": "",
        "latency_source": publisher.get("latency_source"),
        "completed_cleanly": exit_code == 0,
        "container_exit_code": exit_code,
    }

    try:
        if profile.broker == "mqtt":
            messages_sent = parse_mqtt_progress_records(logs_text)
            probe = publisher.get("probe")
            probe_wait = {
                "received_messages": 0,
                "settled": False,
                "settle_reason": "probe_missing",
                "completion_sec": 0.0,
            }
            latency_summary = {
                "avg_latency_ms": None,
                "p95_latency_ms": None,
                "max_latency_ms": None,
                "observations": 0,
            }
            if probe:
                probe_wait = wait_for_probe_receipts(
                    port=int(probe["port"]),
                    expected_messages=messages_sent,
                    timeout_sec=max(60, profile.planned_publish_window_sec * 4),
                )
                probe_metrics_text = fetch_text(f"http://localhost:{int(probe['port'])}/metrics", timeout=10)
                latency_summary = compute_histogram_latency_summary(parse_histogram(probe_metrics_text, "e2e_latency"))
            result.update(
                {
                    "messages_sent": messages_sent,
                    "probe_received_messages": probe_wait["received_messages"],
                    "probe_settled": probe_wait["settled"],
                    "probe_settle_reason": probe_wait["settle_reason"],
                    "probe_completion_sec": probe_wait["completion_sec"],
                    "latency_summary": latency_summary,
                    "max_latency_is_histogram_upper_bound": True,
                }
            )
        else:
            summary = parse_kafka_summary(logs_text)
            messages_sent = int(summary.get("records") or parse_kafka_progress_records(logs_text))
            result.update(
                {
                    "messages_sent": messages_sent,
                    "tool_summary": summary,
                }
            )
    finally:
        if profile.broker == "mqtt" and publisher.get("probe"):
            stop_container(str(publisher["probe"]["container_name"]))

    return result


def disconnect_container_from_network(container_name: str) -> None:
    run_cmd(["docker", "network", "disconnect", "iot_network", container_name], timeout=120)


def reconnect_container_to_network(container_name: str) -> None:
    run_cmd(["docker", "network", "connect", "iot_network", container_name], timeout=120)


def parse_kafka_consumer_group(output: str, group_id: str) -> Dict[str, object]:
    partitions: List[Dict[str, int]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("Consumer group") or line.startswith("GROUP"):
            continue
        parts = line.split()
        if len(parts) < 6 or parts[0] != group_id or parts[1] != KAFKA_TOPIC:
            continue
        try:
            partitions.append(
                {
                    "partition": int(parts[2]),
                    "current_offset": int(parts[3]),
                    "log_end_offset": int(parts[4]),
                    "lag": int(parts[5]),
                }
            )
        except ValueError:
            continue
    partitions.sort(key=lambda item: item["partition"])
    return {
        "group_id": group_id,
        "topic": KAFKA_TOPIC,
        "current_offset": sum(item["current_offset"] for item in partitions),
        "log_end_offset": sum(item["log_end_offset"] for item in partitions),
        "lag": sum(item["lag"] for item in partitions),
        "partitions": partitions,
    }


def collect_kafka_consumer_lag(group_id: str) -> Dict[str, object]:
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "iot_network",
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "kafka-broker:29092",
            "--describe",
            "--group",
            group_id,
        ],
        timeout=120,
    )
    return parse_kafka_consumer_group(result.stdout, group_id)


def collect_kafka_lag_groups() -> Dict[str, object]:
    return {group_id: collect_kafka_consumer_lag(group_id) for group_id in KAFKA_LAG_GROUPS}


def capture_timeline(
    *,
    profile: ScenarioBProfile,
    duration_sec: int,
    include_kafka_lag: bool,
    publisher: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    timeline: List[Dict[str, object]] = []
    started = time.time()
    for second in range(duration_sec):
        snapshot = get_runtime_snapshot(profile, publisher=publisher)
        entry = {
            "elapsed_sec": round(time.time() - started, 3),
            **snapshot,
        }
        if include_kafka_lag and profile.broker == "kafka":
            entry["consumer_lag_groups"] = collect_kafka_lag_groups()
        timeline.append(entry)
        if second < duration_sec - 1:
            time.sleep(1)
    return timeline


def wait_for_tool_progress_resume(
    profile: ScenarioBProfile,
    publisher: Dict[str, object],
    baseline_sent: int,
    timeout_sec: int = 180,
) -> float:
    started = time.time()
    deadline = started + timeout_sec
    while time.time() < deadline:
        current_sent = get_tool_sent_messages(profile, publisher)
        if current_sent > baseline_sent:
            return round(time.time() - started, 3)
        time.sleep(1)
    return round(timeout_sec, 3)


def wait_for_pipeline_and_buffers_settle(
    profile: ScenarioBProfile,
    *,
    publisher: Optional[Dict[str, object]] = None,
    timeout_sec: int = 180,
    stable_polls: int = 3,
) -> Dict[str, object]:
    deadline = time.time() + timeout_sec
    last_key = None
    stable_count = 0
    started = time.time()

    while time.time() < deadline:
        snapshot = get_runtime_snapshot(profile, publisher=publisher)
        key = (
            snapshot["sent_messages"],
            snapshot["storage_received_messages"],
            snapshot["analytics_processed_messages"],
            snapshot["publish_queue_depth"],
            snapshot["offline_buffer_depth"],
            snapshot["simulation_running"],
        )
        if key == last_key:
            stable_count += 1
        else:
            stable_count = 0
            last_key = key

        publish_buffers_empty = (
            snapshot["publish_queue_depth"] == 0
            and snapshot["offline_buffer_depth"] == 0
        )

        if publish_buffers_empty and not snapshot["simulation_running"] and stable_count >= stable_polls:
            return {
                "settled": True,
                "snapshot": snapshot,
                "completion_sec": round(time.time() - started, 3),
            }
        time.sleep(1)

    return {
        "settled": False,
        "snapshot": get_runtime_snapshot(profile, publisher=publisher),
        "completion_sec": round(time.time() - started, 3),
    }


def summarize_kafka_lag_timeline(timeline: List[Dict[str, object]]) -> Dict[str, object]:
    if not timeline:
        return {}

    summary: Dict[str, object] = {}
    for group_id in KAFKA_LAG_GROUPS:
        lag_values: List[int] = []
        time_to_zero_after_nonzero = None
        seen_nonzero = False
        for entry in timeline:
            lag_groups = entry.get("consumer_lag_groups", {})
            lag_info = lag_groups.get(group_id, {})
            lag = int(lag_info.get("lag", 0))
            lag_values.append(lag)
            if lag > 0:
                seen_nonzero = True
            if seen_nonzero and lag == 0 and time_to_zero_after_nonzero is None:
                time_to_zero_after_nonzero = entry["elapsed_sec"]

        final_groups = timeline[-1].get("consumer_lag_groups", {})
        summary[group_id] = {
            "max_lag": max(lag_values) if lag_values else 0,
            "time_to_zero_lag_sec": time_to_zero_after_nonzero,
            "final": final_groups.get(group_id),
        }
    return summary


def first_counter_recovery_sec(
    reconnect_start_snapshot: Dict[str, object],
    recovery_timeline: List[Dict[str, object]],
    key: str,
) -> Optional[float]:
    baseline = int(reconnect_start_snapshot[key])
    for entry in recovery_timeline:
        if int(entry[key]) > baseline:
            return float(entry["elapsed_sec"])
    return None


def throughput_from_phase(phase: Dict[str, object], duration_sec: float) -> Dict[str, float]:
    duration = max(float(duration_sec), 0.001)
    return {
        "publish": round(int(phase["sent_messages"]) / duration, 3),
        "storage": round(int(phase["storage_received_messages"]) / duration, 3),
        "analytics": round(int(phase["analytics_processed_messages"]) / duration, 3),
    }


class ResourceSampler:
    def __init__(self, container_name: str, sample_interval_sec: float = 1.0) -> None:
        self.container_name = container_name
        self.sample_interval_sec = sample_interval_sec
        self.samples: List[Dict[str, float]] = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=10)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                metrics_text = fetch_text(f"{RESOURCE_MONITOR_URL}/metrics", timeout=5)
                labels = {"container_name": self.container_name}
                self.samples.append(
                    {
                        "timestamp": time.time(),
                        "cpu_pct": find_metric_value(metrics_text, "docker_container_cpu_percent", labels),
                        "memory_bytes": find_metric_value(metrics_text, "docker_container_memory_usage_bytes", labels),
                        "network_rx_bytes": find_metric_value(metrics_text, "docker_container_network_rx_bytes", labels),
                        "network_tx_bytes": find_metric_value(metrics_text, "docker_container_network_tx_bytes", labels),
                    }
                )
            except (*TRANSIENT_HTTP_EXCEPTIONS, ValueError):
                pass
            time.sleep(self.sample_interval_sec)

    def summary(self) -> Dict[str, Optional[float]]:
        if not self.samples:
            return {
                "cpu_pct": None,
                "cpu_pct_avg": None,
                "ram_mb": None,
                "ram_mb_avg": None,
                "network_mb": None,
                "network_rx_mb": None,
                "network_tx_mb": None,
                "sample_count": 0,
            }

        cpu_values = [sample["cpu_pct"] for sample in self.samples]
        memory_values = [sample["memory_bytes"] for sample in self.samples]
        first_sample = self.samples[0]
        last_sample = self.samples[-1]
        network_rx_delta = max(0.0, last_sample["network_rx_bytes"] - first_sample["network_rx_bytes"])
        network_tx_delta = max(0.0, last_sample["network_tx_bytes"] - first_sample["network_tx_bytes"])

        return {
            "cpu_pct": round(max(cpu_values), 3),
            "cpu_pct_avg": round(sum(cpu_values) / len(cpu_values), 3),
            "ram_mb": round(max(memory_values) / (1024 * 1024), 3),
            "ram_mb_avg": round((sum(memory_values) / len(memory_values)) / (1024 * 1024), 3),
            "network_mb": round((network_rx_delta + network_tx_delta) / (1024 * 1024), 3),
            "network_rx_mb": round(network_rx_delta / (1024 * 1024), 3),
            "network_tx_mb": round(network_tx_delta / (1024 * 1024), 3),
            "sample_count": len(self.samples),
        }


def execute_profile(
    profile: ScenarioBProfile,
    *,
    payload_dir: Path,
    disable_db_write: bool,
    build_images: bool,
) -> Dict[str, object]:
    restart_stack(profile, disable_db_write=disable_db_write, build_images=build_images)
    resource_sampler = ResourceSampler(BROKER_CONTAINER_NAME[profile.broker])
    benchmark_started_at = time.time()
    publisher: Optional[Dict[str, object]] = None
    tool_result: Optional[Dict[str, object]] = None

    resource_sampler.start()
    try:
        if profile.mode == "tool_benchmark":
            publisher = start_tool_publisher(profile, payload_dir)
        else:
            start_app_simulation(profile)

        time.sleep(profile.warmup_sec)
        warmup_end = get_runtime_snapshot(profile, publisher=publisher)

        disconnect_target = str(publisher["container_name"]) if publisher else "data-ingestion"
        disconnect_container_from_network(disconnect_target)
        outage_timeline = capture_timeline(
            profile=profile,
            duration_sec=profile.outage_sec,
            include_kafka_lag=False,
            publisher=publisher,
        )
        outage_end = outage_timeline[-1] if outage_timeline else get_runtime_snapshot(profile, publisher=publisher)

        reconnect_container_to_network(disconnect_target)
        if profile.mode == "tool_benchmark":
            reconnect_ready_sec = wait_for_tool_progress_resume(
                profile,
                publisher or {},
                baseline_sent=int(outage_end["sent_messages"]),
            )
        else:
            reconnect_ready_sec = wait_for_ingestion_ready()

        reconnect_start = get_runtime_snapshot(profile, publisher=publisher)
        recovery_timeline = capture_timeline(
            profile=profile,
            duration_sec=profile.post_reconnect_run_sec,
            include_kafka_lag=profile.broker == "kafka",
            publisher=publisher,
        )
        reconnect_end = recovery_timeline[-1] if recovery_timeline else get_runtime_snapshot(profile, publisher=publisher)

        if profile.mode == "tool_benchmark":
            tool_result = wait_for_tool_completion(profile, publisher or {})
        else:
            stop_app_simulation()

        settle_result = wait_for_pipeline_and_buffers_settle(profile, publisher=publisher)
        final_snapshot = settle_result["snapshot"]

        analytics_metrics = fetch_text(f"{ANALYTICS_URL}/metrics")
    finally:
        if publisher and profile.mode == "tool_benchmark":
            stop_container(str(publisher["container_name"]))
            if publisher.get("probe"):
                stop_container(str(publisher["probe"]["container_name"]))
        resource_sampler.stop()

    warmup_phase = diff_snapshots(
        warmup_end,
        {
            "generated_messages": 0,
            "sent_messages": 0,
            "dropped_messages": 0,
            "send_errors": 0,
            "storage_received_messages": 0,
            "analytics_processed_messages": 0,
            "publish_queue_depth": 0,
            "offline_buffer_depth": 0,
        },
    )
    outage_phase = diff_snapshots(outage_end, warmup_end)
    recovery_phase = diff_snapshots(reconnect_end, reconnect_start)
    drain_phase = diff_snapshots(final_snapshot, reconnect_end)

    total_generated = int(final_snapshot["generated_messages"])
    total_sent = int(final_snapshot["sent_messages"])
    total_dropped = int(final_snapshot["dropped_messages"])
    total_received = int(final_snapshot["storage_received_messages"])
    total_processed = int(final_snapshot["analytics_processed_messages"])

    if profile.mode == "tool_benchmark" and tool_result is not None:
        total_sent = int(tool_result.get("messages_sent") or total_sent)
        total_generated = total_sent
        total_dropped = max(0, profile.planned_messages - total_sent)
        final_snapshot["generated_messages"] = total_generated
        final_snapshot["sent_messages"] = total_sent
        final_snapshot["dropped_messages"] = total_dropped

    total_duration_sec = round(time.time() - benchmark_started_at, 3)
    throughput_summary = {
        "warmup": throughput_from_phase(warmup_phase, profile.warmup_sec),
        "outage": throughput_from_phase(outage_phase, profile.outage_sec),
        "recovery": throughput_from_phase(recovery_phase, profile.post_reconnect_run_sec),
        "drain": throughput_from_phase(drain_phase, float(settle_result["completion_sec"])),
        "total": throughput_from_phase(
            {
                "sent_messages": total_sent,
                "storage_received_messages": total_received,
                "analytics_processed_messages": total_processed,
            },
            total_duration_sec,
        ),
    }

    latency_summary = parse_latency_summary(analytics_metrics, profile.broker)
    latency_source = "analytics_message_e2e_latency_histogram"
    if profile.mode == "tool_benchmark" and tool_result is not None:
        if profile.broker == "mqtt":
            latency_summary = dict(tool_result.get("latency_summary") or latency_summary)
            latency_source = str(tool_result.get("latency_source") or latency_source)
        else:
            tool_summary = tool_result.get("tool_summary") or {}
            latency_summary = {
                "avg_latency_ms": round(tool_summary.get("avg_latency_ms"), 3)
                if "avg_latency_ms" in tool_summary
                else None,
                "p95_latency_ms": round(tool_summary.get("p95_latency_ms"), 3)
                if "p95_latency_ms" in tool_summary
                else None,
                "max_latency_ms": round(tool_summary.get("max_latency_ms"), 3)
                if "max_latency_ms" in tool_summary
                else None,
                "latency_sample_count": int(tool_summary.get("records", 0)),
            }
            latency_source = str(tool_result.get("latency_source") or latency_source)

    resource_summary = resource_sampler.summary()
    loss_messages = max(0, total_sent - total_received)
    loss_pct = round((loss_messages / total_sent * 100.0), 3) if total_sent else 0.0

    validation_issues: List[str] = []
    if final_snapshot["analytics_processed_messages"] > final_snapshot["storage_received_messages"]:
        validation_issues.append("analytics_processed_exceeded_storage_received")
    if profile.mode == "app_buffered" and final_snapshot["publish_queue_depth"] > 0:
        validation_issues.append("publish_queue_not_empty_after_settle")
    if profile.mode == "app_buffered" and final_snapshot["offline_buffer_depth"] > 0:
        validation_issues.append("offline_buffer_not_empty_after_settle")
    if not settle_result["settled"]:
        validation_issues.append("pipeline_did_not_settle_after_stop")
    if total_received > total_sent:
        validation_issues.append("storage_received_exceeded_successful_publish")
    if total_processed > total_sent:
        validation_issues.append("analytics_processed_exceeded_successful_publish")
    if profile.mode == "tool_benchmark" and tool_result is not None and not tool_result.get("completed_cleanly"):
        validation_issues.append("tool_publisher_did_not_exit_cleanly")
    if latency_summary["p95_latency_ms"] is None:
        validation_issues.append("latency_p95_missing")

    kafka_lag_summary = None
    final_kafka_lag_groups = None
    if profile.broker == "kafka":
        kafka_lag_summary = summarize_kafka_lag_timeline(recovery_timeline)
        final_kafka_lag_groups = collect_kafka_lag_groups()
        if any(group.get("lag", 0) != 0 for group in final_kafka_lag_groups.values()):
            validation_issues.append("kafka_consumer_lag_nonzero_after_settle")

    result = {
        "scenario": "B",
        "mode": profile.mode,
        "config_name": profile.config_name,
        "broker": profile.broker,
        "broker_value": profile.broker_value,
        "devices": profile.devices,
        "interval_sec": profile.interval_sec,
        "warmup_sec": profile.warmup_sec,
        "outage_sec": profile.outage_sec,
        "post_reconnect_run_sec": profile.post_reconnect_run_sec,
        "db_write_disabled": disable_db_write,
        "qos": profile.qos if profile.broker == "mqtt" else None,
        "acks": profile.acks if profile.broker == "kafka" else None,
        "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
        "planned_messages": profile.planned_messages,
        "warmup_phase": warmup_phase,
        "outage_phase": outage_phase,
        "recovery_phase": recovery_phase,
        "drain_phase": drain_phase,
        "total_generated_messages": total_generated,
        "total_successful_publish_messages": total_sent,
        "total_dropped_messages": total_dropped,
        "total_storage_received_messages": total_received,
        "total_analytics_processed_messages": total_processed,
        "loss_messages": loss_messages,
        "loss_pct": loss_pct,
        "publish_success_rate_vs_generated_pct": round((total_sent / total_generated * 100.0), 3)
        if total_generated
        else 0.0,
        "storage_capture_rate_vs_generated_pct": round((total_received / total_generated * 100.0), 3)
        if total_generated
        else 0.0,
        "storage_capture_rate_vs_successful_publish_pct": round((total_received / total_sent * 100.0), 3)
        if total_sent
        else 0.0,
        "publish_throughput_msg_s": throughput_summary["total"]["publish"],
        "storage_throughput_msg_s": throughput_summary["total"]["storage"],
        "analytics_throughput_msg_s": throughput_summary["total"]["analytics"],
        "throughput_msg_s": throughput_summary,
        "avg_latency_ms": latency_summary["avg_latency_ms"],
        "p95_latency_ms": latency_summary["p95_latency_ms"],
        "max_latency_ms": latency_summary["max_latency_ms"],
        "latency_sample_count": latency_summary.get("latency_sample_count")
        or latency_summary.get("observations", 0),
        "latency_source": latency_source,
        "cpu_pct": resource_summary["cpu_pct"],
        "cpu_pct_avg": resource_summary["cpu_pct_avg"],
        "ram_mb": resource_summary["ram_mb"],
        "ram_mb_avg": resource_summary["ram_mb_avg"],
        "network_mb": resource_summary["network_mb"],
        "network_rx_mb": resource_summary["network_rx_mb"],
        "network_tx_mb": resource_summary["network_tx_mb"],
        "resource_sample_count": resource_summary["sample_count"],
        "recovery_sec_to_source_ready": reconnect_ready_sec,
        "recovery_sec_to_ingestion_ready": reconnect_ready_sec if profile.mode == "app_buffered" else None,
        "recovery_sec_to_first_storage_message": first_counter_recovery_sec(
            reconnect_start, recovery_timeline, "storage_received_messages"
        ),
        "recovery_sec_to_first_analytics_message": first_counter_recovery_sec(
            reconnect_start, recovery_timeline, "analytics_processed_messages"
        ),
        "pipeline_settled_after_stop": bool(settle_result["settled"]),
        "pipeline_completion_sec_after_stop": settle_result["completion_sec"],
        "final_snapshot": final_snapshot,
        "timeline_outage": outage_timeline,
        "timeline_recovery": recovery_timeline,
        "validation_issues": validation_issues,
        "kafka_consumer_lag_summary": kafka_lag_summary,
        "kafka_consumer_lag_final": final_kafka_lag_groups,
        "tool_result": tool_result,
    }

    return result


def format_float(value: Optional[float], decimals: int = 3) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def render_performance_table(results: List[Dict[str, object]]) -> str:
    headers = [
        "Mode",
        "Broker",
        "Config",
        "Partitions",
        "Publish msg/s",
        "Storage msg/s",
        "p95 ms",
        "CPU %",
        "RAM MB",
        "Loss %",
        "Ready s",
        "First Analytics s",
        "Max Lag",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    def sort_key(item: Dict[str, object]) -> tuple:
        return (
            str(item.get("mode")),
            str(item.get("broker")),
            int(item.get("topic_partitions") or 0),
            str(item.get("broker_value")),
        )

    for result in sorted((item for item in results if not item.get("error")), key=sort_key):
        kafka_summary = result.get("kafka_consumer_lag_summary") or {}
        max_lag = "-"
        if kafka_summary:
            lag_values = [int(group.get("max_lag", 0)) for group in kafka_summary.values()]
            max_lag = str(max(lag_values) if lag_values else 0)

        lines.append(
            "| "
            + " | ".join(
                [
                    str(result.get("mode")),
                    str(result.get("broker")),
                    str(result.get("config_name")),
                    str(result.get("topic_partitions") or "-"),
                    format_float(result.get("publish_throughput_msg_s")),
                    format_float(result.get("storage_throughput_msg_s")),
                    format_float(result.get("p95_latency_ms")),
                    format_float(result.get("cpu_pct")),
                    format_float(result.get("ram_mb")),
                    format_float(result.get("loss_pct")),
                    format_float(result.get("recovery_sec_to_source_ready")),
                    format_float(result.get("recovery_sec_to_first_analytics_message")),
                    max_lag,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def average_or_none(values: List[Optional[float]]) -> Optional[float]:
    actual = [value for value in values if value is not None]
    if not actual:
        return None
    return sum(actual) / len(actual)


def render_analysis(results: List[Dict[str, object]], results_file: Path) -> str:
    completed = [item for item in results if not item.get("error")]
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Scenario B Analysis",
        "",
        f"Generated from `{results_file.name}` on {generated_at}.",
        "",
        "This document summarizes the executed Scenario B runs and is intended to feed the written report.",
        "",
    ]

    for mode in ("tool_benchmark", "app_buffered"):
        mode_results = [item for item in completed if item.get("mode") == mode]
        if not mode_results:
            continue
        lines.append(f"## Mode: `{mode}`")
        lines.append("")

        mqtt_results = [item for item in mode_results if item.get("broker") == "mqtt"]
        if mqtt_results:
            q0 = [item for item in mqtt_results if item.get("qos") == 0]
            q12 = [item for item in mqtt_results if item.get("qos") in {1, 2}]
            q0_loss = average_or_none([float(item.get("loss_pct") or 0.0) for item in q0])
            q12_loss = average_or_none([float(item.get("loss_pct") or 0.0) for item in q12])
            q0_ready = average_or_none([item.get("recovery_sec_to_source_ready") for item in q0])
            q12_ready = average_or_none([item.get("recovery_sec_to_source_ready") for item in q12])
            q0_p95 = average_or_none([item.get("p95_latency_ms") for item in q0])
            q12_p95 = average_or_none([item.get("p95_latency_ms") for item in q12])
            lines.append("### MQTT")
            lines.append("")
            lines.append(
                f"- QoS 0 average loss after outage: `{format_float(q0_loss)}`%; average source recovery time: `{format_float(q0_ready)}` s; average p95 latency: `{format_float(q0_p95)}` ms."
            )
            lines.append(
                f"- QoS 1/2 average loss after outage: `{format_float(q12_loss)}`%; average source recovery time: `{format_float(q12_ready)}` s; average p95 latency: `{format_float(q12_p95)}` ms."
            )
            lines.append(
                "- Interpretation: QoS 0 shows the lowest protocol overhead, while QoS 1/2 trade extra handshake cost for stronger delivery guarantees after reconnect."
            )
            lines.append("")

        kafka_results = [item for item in mode_results if item.get("broker") == "kafka"]
        if kafka_results:
            avg_cpu = average_or_none([item.get("cpu_pct") for item in kafka_results])
            avg_ram = average_or_none([item.get("ram_mb") for item in kafka_results])
            avg_p95 = average_or_none([item.get("p95_latency_ms") for item in kafka_results])
            avg_lag = average_or_none(
                [
                    max(
                        int(group.get("max_lag", 0))
                        for group in (item.get("kafka_consumer_lag_summary") or {}).values()
                    )
                    if item.get("kafka_consumer_lag_summary")
                    else 0
                    for item in kafka_results
                ]
            )
            partition_groups = sorted({int(item.get("topic_partitions") or 0) for item in kafka_results if item.get("topic_partitions")})
            partition_notes: List[str] = []
            for partition_count in partition_groups:
                bucket = [item for item in kafka_results if int(item.get("topic_partitions") or 0) == partition_count]
                if not bucket:
                    continue
                part_throughput = average_or_none([item.get("storage_throughput_msg_s") for item in bucket])
                part_lag = average_or_none(
                    [
                        max(
                            int(group.get("max_lag", 0))
                            for group in (item.get("kafka_consumer_lag_summary") or {}).values()
                        )
                        if item.get("kafka_consumer_lag_summary")
                        else 0
                        for item in bucket
                    ]
                )
                partition_notes.append(
                    f"- Partitions `{partition_count}`: average storage throughput `{format_float(part_throughput)}` msg/s; average peak lag `{format_float(part_lag)}` messages."
                )
            lines.append("### Kafka")
            lines.append("")
            lines.append(
                f"- Average broker CPU footprint: `{format_float(avg_cpu)}`%; average RAM footprint: `{format_float(avg_ram)}` MB; average p95 latency: `{format_float(avg_p95)}` ms."
            )
            lines.append(
                f"- Average peak consumer lag across executed Kafka runs: `{format_float(avg_lag)}` messages."
            )
            lines.append(
                "- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability."
            )
            lines.extend(partition_notes)
            lines.append("")

        if mqtt_results and kafka_results:
            mqtt_cpu = average_or_none([item.get("cpu_pct") for item in mqtt_results])
            kafka_cpu = average_or_none([item.get("cpu_pct") for item in kafka_results])
            mqtt_ram = average_or_none([item.get("ram_mb") for item in mqtt_results])
            kafka_ram = average_or_none([item.get("ram_mb") for item in kafka_results])
            mqtt_p95 = average_or_none([item.get("p95_latency_ms") for item in mqtt_results])
            kafka_p95 = average_or_none([item.get("p95_latency_ms") for item in kafka_results])
            lines.append("### MQTT vs Kafka")
            lines.append("")
            lines.append(
                f"- MQTT average broker footprint in this mode: `{format_float(mqtt_cpu)}`% CPU / `{format_float(mqtt_ram)}` MB RAM; Kafka: `{format_float(kafka_cpu)}`% CPU / `{format_float(kafka_ram)}` MB RAM."
            )
            lines.append(
                f"- MQTT average p95 latency in this mode: `{format_float(mqtt_p95)}` ms; Kafka average p95 latency: `{format_float(kafka_p95)}` ms."
            )
            if mode == "tool_benchmark":
                lines.append(
                    "- Tool-benchmark mode isolates broker-level recovery, so these numbers are the cleanest basis for the broker comparison chapter."
                )
            else:
                lines.append(
                    "- App-buffered mode includes the ingestion service offline queue, so these numbers show end-to-end operational behavior rather than broker-only behavior."
                )
            lines.append("")

    lines.append("## Report Implications")
    lines.append("")
    lines.append(
        "- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs, especially in tool-benchmark mode where application buffering is removed from the picture."
    )
    lines.append(
        "- Kafka cloud suitability can be argued from explicit lag/offset visibility and the partition scaling data, at the cost of larger CPU/RAM usage."
    )
    lines.append(
        "- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report, while this analysis file can seed the narrative answers to the engineering questions."
    )
    lines.append("")
    return "\n".join(lines)


def write_supporting_artifacts(results_file: Path, results: Dict[str, object]) -> Dict[str, str]:
    tests = list(results.get("tests") or [])
    table_path = results_file.with_name(f"{results_file.stem}_performance_table.md")
    analysis_path = results_file.with_name(f"{results_file.stem}_analysis.md")

    table_path.write_text(render_performance_table(tests), encoding="utf-8")
    analysis_path.write_text(render_analysis(tests, results_file), encoding="utf-8")

    return {
        "performance_table": str(table_path),
        "analysis_report": str(analysis_path),
    }


def build_profiles(args: argparse.Namespace) -> List[ScenarioBProfile]:
    profiles: List[ScenarioBProfile] = []
    for mode in args.modes:
        if args.broker in {"mqtt", "both"}:
            for qos in args.mqtt_qos:
                profiles.append(
                    ScenarioBProfile(
                        broker="mqtt",
                        mode=mode,
                        qos=int(qos),
                        devices=args.devices,
                        interval_sec=args.interval_sec,
                        warmup_sec=args.warmup_sec,
                        outage_sec=args.outage_sec,
                        post_reconnect_run_sec=args.post_reconnect_run_sec,
                    )
                )
        if args.broker in {"kafka", "both"}:
            for acks in args.kafka_acks:
                for partitions in args.kafka_partitions:
                    profiles.append(
                        ScenarioBProfile(
                            broker="kafka",
                            mode=mode,
                            acks=str(acks),
                            topic_partitions=int(partitions),
                            devices=args.devices,
                            interval_sec=args.interval_sec,
                            warmup_sec=args.warmup_sec,
                            outage_sec=args.outage_sec,
                            post_reconnect_run_sec=args.post_reconnect_run_sec,
                        )
                    )
    return profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runs dedicated Scenario B outage/recovery benchmark.")
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--modes", nargs="+", choices=["tool_benchmark", "app_buffered"], default=["tool_benchmark", "app_buffered"])
    parser.add_argument("--mqtt-qos", nargs="+", default=["0", "1", "2"])
    parser.add_argument("--kafka-acks", nargs="+", default=["0", "1", "all"])
    parser.add_argument("--kafka-partitions", nargs="+", default=["1", "4", "8"])
    parser.add_argument("--devices", type=int, default=100)
    parser.add_argument("--interval-sec", type=float, default=0.5)
    parser.add_argument("--warmup-sec", type=int, default=5)
    parser.add_argument("--outage-sec", type=int, default=30)
    parser.add_argument("--post-reconnect-run-sec", type=int, default=15)
    parser.add_argument("--db-write-enabled", action="store_true")
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--results-file", default=str(RESULTS_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results_file = Path(args.results_file)
    results_file.parent.mkdir(parents=True, exist_ok=True)
    payload_tmp = build_payload_dir()

    aggregated_results = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scenario": "B",
        "tests": [],
        "artifacts": {},
    }

    profiles = build_profiles(args)
    try:
        for index, profile in enumerate(profiles, start=1):
            print("\n" + "=" * 80)
            print(f"[{index}/{len(profiles)}] Running {profile.config_name}")
            print("=" * 80)
            try:
                aggregated_results["tests"].append(
                    execute_profile(
                        profile,
                        payload_dir=Path(payload_tmp.name),
                        disable_db_write=not args.db_write_enabled,
                        build_images=args.build_images and index == 1,
                    )
                )
            except Exception as exc:
                failure = {
                    "scenario": "B",
                    "mode": profile.mode,
                    "config_name": profile.config_name,
                    "broker": profile.broker,
                    "broker_value": profile.broker_value,
                    "devices": profile.devices,
                    "qos": profile.qos if profile.broker == "mqtt" else None,
                    "acks": profile.acks if profile.broker == "kafka" else None,
                    "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
                    "error": str(exc),
                    "validation_issues": ["benchmark_execution_failed"],
                }
                aggregated_results["tests"].append(failure)
                print(f"FAILED {profile.config_name}: {exc}", file=sys.stderr)

            aggregated_results["artifacts"] = write_supporting_artifacts(results_file, aggregated_results)
            results_file.write_text(json.dumps(aggregated_results, indent=2), encoding="utf-8")
    finally:
        payload_tmp.cleanup()

    print("\n" + "=" * 80)
    print(f"Scenario B benchmark completed. Results saved to {results_file}")
    if aggregated_results.get("artifacts"):
        print(f"Performance table: {aggregated_results['artifacts'].get('performance_table')}")
        print(f"Analysis report: {aggregated_results['artifacts'].get('analysis_report')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
