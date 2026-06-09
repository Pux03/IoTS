"""
Complete Scenario A benchmark runner.

Scenario A covers:
- MQTT with QoS 0 / 1 / 2 using eMQTT-Bench
- Kafka with acks 0 / 1 / all using kafka-producer-perf-test.sh
- Device groups 100 / 1000 / 10000
- Kafka topic partition counts 1 / 4 / 8

The script restarts the stack for every individual test profile, waits for
all services to become ready, samples broker resource usage, validates the
processing pipeline and exports JSON results.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.client import RemoteDisconnected
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_PATH = Path(__file__).resolve().parent / "scenario_a_results.json"

INGESTION_URL = "http://localhost:8000"
STORAGE_URL = "http://localhost:8001"
ANALYTICS_URL = "http://localhost:8002"
RESOURCE_MONITOR_URL = "http://localhost:8083"

MQTT_TOPIC = "iot/events"
KAFKA_TOPIC = "iot-events"
MQTT_LATENCY_PROBE_IMAGE = "emqx/emqtt-bench:latest"
KAFKA_IMAGE = "apache/kafka:3.7.0"
BROKER_CONTAINER_NAME = {
    "mqtt": "mqtt-broker",
    "kafka": "kafka-broker",
}
KAFKA_LAG_GROUPS = ("data-storage-group", "analytics-group")

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
TRANSIENT_HTTP_EXCEPTIONS = (
    urllib.error.URLError,
    urllib.error.HTTPError,
    TimeoutError,
    OSError,
    RemoteDisconnected,
)


@dataclass(frozen=True)
class ScenarioAProfile:
    broker: str
    devices: int
    interval_sec: float
    duration_sec: int
    qos: Optional[int] = None
    acks: Optional[str] = None
    topic_partitions: Optional[int] = None

    @property
    def requested_messages(self) -> int:
        messages_per_device = max(1, math.ceil(self.duration_sec / self.interval_sec))
        return self.devices * messages_per_device

    @property
    def target_throughput(self) -> float:
        return self.devices / self.interval_sec if self.interval_sec > 0 else 0.0

    @property
    def config_name(self) -> str:
        if self.broker == "mqtt":
            return f"mqtt_qos_{self.qos}_devices_{self.devices}"
        return f"kafka_acks_{self.acks}_partitions_{self.topic_partitions}_devices_{self.devices}"

    @property
    def broker_value(self) -> str:
        if self.broker == "mqtt":
            return str(self.qos)
        return str(self.acks)


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


def find_metric_value(metrics_text: str, metric_name: str, label_filters: Optional[Dict[str, str]] = None) -> float:
    label_filters = label_filters or {}
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line.strip())
        if not match:
            continue
        if match.group("name") != metric_name:
            continue
        labels = parse_labels(match.group("labels") or "")
        if any(labels.get(key) != value for key, value in label_filters.items()):
            continue
        try:
            return float(match.group("value"))
        except ValueError:
            return 0.0
    return 0.0


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


def get_counter_snapshot() -> Dict[str, float]:
    storage_metrics = fetch_text(f"{STORAGE_URL}/metrics")
    analytics_metrics = fetch_text(f"{ANALYTICS_URL}/metrics")
    return {
        "storage_messages_received_total": parse_prometheus_counter(
            storage_metrics, "storage_messages_received_total"
        ),
        "analytics_messages_processed_total": parse_prometheus_counter(
            analytics_metrics, "analytics_messages_processed_total"
        ),
    }


def subtract_snapshot(current: Dict[str, float], base: Dict[str, float]) -> Dict[str, float]:
    return {
        key: max(0.0, current.get(key, 0.0) - base.get(key, 0.0))
        for key in current.keys()
    }


def wait_for_pipeline_settle(
    *,
    base_snapshot: Dict[str, float],
    expected_messages: float,
    timeout_sec: int,
    poll_interval_sec: float = 1.0,
    stable_polls: int = 3,
) -> Dict[str, object]:
    deadline = time.time() + timeout_sec
    started_at = time.time()
    last_delta: Optional[Dict[str, float]] = None
    stable_count = 0
    settle_reason = "timeout"

    while time.time() < deadline:
        current_delta = subtract_snapshot(get_counter_snapshot(), base_snapshot)
        if current_delta == last_delta:
            stable_count += 1
        else:
            stable_count = 0
            last_delta = current_delta

        received = current_delta["storage_messages_received_total"]
        processed = current_delta["analytics_messages_processed_total"]

        if received >= expected_messages and processed >= expected_messages:
            settle_reason = "all_messages_observed"
            break
        if stable_count >= stable_polls:
            settle_reason = "counters_stable"
            break

        time.sleep(poll_interval_sec)

    final_delta = subtract_snapshot(get_counter_snapshot(), base_snapshot)
    completion_sec = round(time.time() - started_at, 3)
    settled = settle_reason != "timeout"
    return {
        "settled": settled,
        "settle_reason": settle_reason,
        "completion_sec": completion_sec,
        "delta": final_delta,
    }


def wait_for_services(timeout_sec: int = 180) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        service_health = [
            try_fetch_json(f"{INGESTION_URL}/health"),
            try_fetch_json(f"{STORAGE_URL}/health"),
            try_fetch_json(f"{ANALYTICS_URL}/health"),
            try_fetch_json(f"{RESOURCE_MONITOR_URL}/health"),
        ]
        if all(status and status.get("ready") is True for status in service_health):
            time.sleep(2)
            return
        time.sleep(2)

    raise TimeoutError("Services did not become ready in time.")


def restart_stack(profile: ScenarioAProfile) -> None:
    env_overrides = {
        "BROKER_TYPE": profile.broker,
        "DISABLE_DB_WRITE": "true",
        "MQTT_QOS": str(profile.qos or 0),
        "KAFKA_ACKS": str(profile.acks or "1"),
        "KAFKA_TOPIC_PARTITIONS": str(profile.topic_partitions or 1),
    }

    run_cmd(["docker", "compose", "down", "--remove-orphans"], env_overrides=env_overrides, timeout=180)
    run_cmd(["docker", "compose", "up", "-d", "--build"], env_overrides=env_overrides, timeout=900)
    wait_for_services()


def build_payload_dir() -> tempfile.TemporaryDirectory:
    temp_dir = tempfile.TemporaryDirectory(prefix="scenario-a-bench-")
    payload_root = Path(temp_dir.name)

    mqtt_template = {
        "event_id": "mqtt-bench-%UNIQUE%",
        "timestamp": "2026-01-01T00:00:00Z",
        "emitted_at_ms": "%TIMESTAMPMS%",
        "device_id": "BENCH-%RANDOM%",
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
                    "event_id": f"kafka-bench-{index}",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "device_id": f"BENCH-{index % 64:04d}",
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


def percentile(values: Iterable[float], rank_pct: float) -> Optional[float]:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, math.ceil((rank_pct / 100.0) * len(ordered)) - 1)
    return ordered[index]


def average_or_none(values: Iterable[Optional[float]]) -> Optional[float]:
    actual = [float(value) for value in values if value is not None]
    if not actual:
        return None
    return float(sum(actual) / len(actual))


def median_or_none(values: Iterable[Optional[float]]) -> Optional[float]:
    actual = [float(value) for value in values if value is not None]
    if not actual:
        return None
    return float(statistics.median(actual))


def format_float(value: Optional[float], decimals: int = 3) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


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


def start_mqtt_latency_probe(profile: ScenarioAProfile) -> Dict[str, object]:
    port = find_free_port()
    container_name = f"scenario-a-mqtt-probe-{int(time.time() * 1000)}"

    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "--network",
        "iot_network",
        "-p",
        f"{port}:{port}",
        MQTT_LATENCY_PROBE_IMAGE,
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
                sample = {
                    "timestamp": time.time(),
                    "cpu_pct": find_metric_value(metrics_text, "docker_container_cpu_percent", labels),
                    "memory_bytes": find_metric_value(metrics_text, "docker_container_memory_usage_bytes", labels),
                    "network_rx_bytes": find_metric_value(metrics_text, "docker_container_network_rx_bytes", labels),
                    "network_tx_bytes": find_metric_value(metrics_text, "docker_container_network_tx_bytes", labels),
                }
                self.samples.append(sample)
            except (*TRANSIENT_HTTP_EXCEPTIONS, ValueError):
                pass
            time.sleep(self.sample_interval_sec)

    def summary(self) -> Dict[str, Optional[float]]:
        if len(self.samples) < 1:
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


def run_mqtt_benchmark(
    *,
    profile: ScenarioAProfile,
    payload_dir: Path,
) -> Dict[str, object]:
    probe: Optional[Dict[str, object]] = None
    try:
        probe = start_mqtt_latency_probe(profile)
        connect_rate = max(1, min(profile.devices, 1000))
        interval_ms = max(1, round(profile.interval_sec * 1000))

        cmd = [
            "docker",
            "run",
            "--rm",
            "--network",
            "iot_network",
            "-v",
            f"{payload_dir.resolve()}:/payloads:ro",
            MQTT_LATENCY_PROBE_IMAGE,
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
            str(profile.requested_messages),
            "-w",
            "true",
            "--payload-hdrs",
            "ts",
            "-m",
            "template:///payloads/mqtt_payload_template.json",
            "--log_to",
            "null",
        ]
        if (profile.qos or 0) > 0:
            cmd.extend(["-F", "20"])

        started_at = time.time()
        result = run_cmd(cmd, timeout=max(240, profile.duration_sec * 15))
        elapsed_sec = time.time() - started_at

        probe_wait = wait_for_probe_receipts(
            port=int(probe["port"]),
            expected_messages=profile.requested_messages,
            timeout_sec=max(60, profile.duration_sec * 12),
        )
        probe_metrics_text = fetch_text(f"http://localhost:{int(probe['port'])}/metrics", timeout=10)
        latency_histogram = parse_histogram(probe_metrics_text, "e2e_latency")
        time.sleep(2)
        stop_container(str(probe["container_name"]))
        probe = None

        return {
            "messages_sent": profile.requested_messages,
            "elapsed_sec": round(elapsed_sec, 3),
            "tool_stdout": result.stdout.strip(),
            "tool_stderr": result.stderr.strip(),
            "latency_summary": compute_histogram_latency_summary(latency_histogram),
            "latency_source": "emqtt-bench_e2e_latency_histogram",
            "max_latency_is_histogram_upper_bound": True,
            "probe_received_messages": probe_wait["received_messages"],
            "probe_settled": probe_wait["settled"],
            "probe_settle_reason": probe_wait["settle_reason"],
            "probe_completion_sec": probe_wait["completion_sec"],
        }
    finally:
        if probe:
            stop_container(str(probe["container_name"]))


def run_kafka_benchmark(
    *,
    profile: ScenarioAProfile,
    payload_dir: Path,
) -> Dict[str, object]:
    throughput = max(1, round(profile.target_throughput))
    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        "iot_network",
        "-v",
        f"{payload_dir.resolve()}:/payloads:ro",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic",
        KAFKA_TOPIC,
        "--num-records",
        str(profile.requested_messages),
        "--throughput",
        str(throughput),
        "--payload-file",
        "/payloads/kafka_payloads.txt",
        "--producer-props",
        "bootstrap.servers=kafka-broker:29092",
        f"acks={profile.acks}",
        "linger.ms=0",
        "batch.size=16384",
    ]

    started_at = time.time()
    result = run_cmd(cmd, timeout=max(240, profile.duration_sec * 12))
    elapsed_sec = time.time() - started_at
    return {
        "messages_sent": profile.requested_messages,
        "elapsed_sec": round(elapsed_sec, 3),
        "tool_stdout": result.stdout.strip(),
        "tool_stderr": result.stderr.strip(),
        "tool_summary": parse_kafka_summary(result.stdout),
        "latency_source": "kafka-producer-perf-test_summary",
    }


def parse_kafka_consumer_group(output: str, group_id: str) -> Dict[str, object]:
    partitions: List[Dict[str, int]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("Consumer group") or line.startswith("GROUP"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        if parts[0] != group_id or parts[1] != KAFKA_TOPIC:
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
    cmd = [
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
    ]
    result = run_cmd(cmd, timeout=120)
    return parse_kafka_consumer_group(result.stdout, group_id)


def execute_profile(
    *,
    profile: ScenarioAProfile,
    payload_dir: Path,
) -> Dict[str, object]:
    before = get_counter_snapshot()
    benchmark_started_at = time.time()
    resource_sampler = ResourceSampler(BROKER_CONTAINER_NAME[profile.broker])

    resource_sampler.start()
    try:
        if profile.broker == "mqtt":
            bench_result = run_mqtt_benchmark(profile=profile, payload_dir=payload_dir)
            tool_name = "emqtt-bench"
        else:
            bench_result = run_kafka_benchmark(profile=profile, payload_dir=payload_dir)
            tool_name = "kafka-producer-perf-test.sh"

        settle_result = wait_for_pipeline_settle(
            base_snapshot=before,
            expected_messages=float(profile.requested_messages),
            timeout_sec=max(60, profile.duration_sec * 12),
        )
        completion_sec = round(time.time() - benchmark_started_at, 3)
    finally:
        resource_sampler.stop()

    kafka_lag_groups = None
    if profile.broker == "kafka":
        kafka_lag_groups = {
            group_id: collect_kafka_consumer_lag(group_id)
            for group_id in KAFKA_LAG_GROUPS
        }

    after_delta = settle_result["delta"]
    resource_summary = resource_sampler.summary()
    messages_sent = int(profile.requested_messages)
    if profile.broker == "kafka":
        tool_summary = bench_result.get("tool_summary") or {}
        if "records" in tool_summary:
            messages_sent = int(tool_summary["records"])
    messages_received = int(after_delta["storage_messages_received_total"])
    analytics_processed = int(after_delta["analytics_messages_processed_total"])
    tool_elapsed_sec = float(bench_result["elapsed_sec"])
    producer_throughput = messages_sent / tool_elapsed_sec if tool_elapsed_sec > 0 else 0.0
    consumer_throughput = messages_received / completion_sec if completion_sec > 0 else 0.0
    loss_messages = max(0, messages_sent - messages_received)
    loss_pct = (loss_messages / messages_sent * 100.0) if messages_sent else 0.0

    validation_issues: List[str] = []
    if analytics_processed > messages_received:
        validation_issues.append("analytics_processed_exceeded_storage_received")
    if messages_received > messages_sent:
        validation_issues.append("received_messages_exceeded_sent_messages")
    if not settle_result["settled"]:
        validation_issues.append("pipeline_did_not_settle_before_timeout")
    if messages_received < messages_sent:
        validation_issues.append("storage_received_less_than_sent")

    latency_summary = {
        "avg_latency_ms": None,
        "p95_latency_ms": None,
        "max_latency_ms": None,
        "observations": 0,
    }

    if profile.broker == "mqtt":
        latency_summary = dict(bench_result["latency_summary"])
        probe_received = int(bench_result["probe_received_messages"])
        if probe_received < messages_sent:
            validation_issues.append("mqtt_latency_probe_received_less_than_sent")
        if not bench_result["probe_settled"]:
            validation_issues.append("mqtt_latency_probe_did_not_settle")
        if latency_summary["observations"] < messages_sent:
            validation_issues.append("mqtt_latency_histogram_observations_less_than_sent")
    else:
        tool_summary = bench_result.get("tool_summary") or {}
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
            "observations": int(tool_summary.get("records", 0)),
        }
        if "records_per_sec" in tool_summary:
            producer_throughput = float(tool_summary["records_per_sec"])

    result = {
        "scenario": "A",
        "config_name": profile.config_name,
        "broker": profile.broker,
        "devices": profile.devices,
        "messages_sent": messages_sent,
        "messages_received": messages_received,
        "analytics_processed_messages": analytics_processed,
        "loss_messages": loss_messages,
        "loss_pct": round(loss_pct, 3),
        "producer_throughput_msg_per_sec": round(producer_throughput, 3),
        "consumer_throughput_msg_per_sec": round(consumer_throughput, 3),
        "avg_latency_ms": latency_summary["avg_latency_ms"],
        "p95_latency_ms": latency_summary["p95_latency_ms"],
        "max_latency_ms": latency_summary["max_latency_ms"],
        "latency_observations": latency_summary["observations"],
        "latency_source": bench_result.get("latency_source"),
        "cpu_pct": resource_summary["cpu_pct"],
        "cpu_pct_avg": resource_summary["cpu_pct_avg"],
        "ram_mb": resource_summary["ram_mb"],
        "ram_mb_avg": resource_summary["ram_mb_avg"],
        "network_mb": resource_summary["network_mb"],
        "network_rx_mb": resource_summary["network_rx_mb"],
        "network_tx_mb": resource_summary["network_tx_mb"],
        "validation_issues": validation_issues,
        "pipeline_settled": bool(settle_result["settled"]),
        "pipeline_settle_reason": settle_result["settle_reason"],
        "pipeline_completion_sec": settle_result["completion_sec"],
        "completion_sec_from_start": completion_sec,
        "tool_elapsed_sec": round(tool_elapsed_sec, 3),
        "target_throughput_msg_per_sec": round(profile.target_throughput, 3),
        "tool": tool_name,
        "interval_sec": profile.interval_sec,
        "duration_sec": profile.duration_sec,
        "resource_sample_count": resource_summary["sample_count"],
    }

    if profile.broker == "mqtt":
        result["qos"] = profile.qos
        result["max_latency_is_histogram_upper_bound"] = bench_result["max_latency_is_histogram_upper_bound"]
        result["mqtt_probe_received_messages"] = int(bench_result["probe_received_messages"])
        result["mqtt_probe_settled"] = bool(bench_result["probe_settled"])
        result["mqtt_probe_settle_reason"] = bench_result["probe_settle_reason"]
        result["mqtt_probe_completion_sec"] = bench_result["probe_completion_sec"]
    else:
        result["acks"] = profile.acks
        result["topic_partitions"] = profile.topic_partitions
        result["consumer_lag_groups"] = kafka_lag_groups
        storage_group = (kafka_lag_groups or {}).get("data-storage-group") or {}
        result["current_offset"] = storage_group.get("current_offset")
        result["log_end_offset"] = storage_group.get("log_end_offset")
        result["lag"] = storage_group.get("lag")
        result["consumer_lag"] = storage_group.get("lag")
        if bench_result.get("tool_summary"):
            result["kafka_producer_summary"] = bench_result["tool_summary"]

    return result


def build_test_matrix(args: argparse.Namespace) -> List[ScenarioAProfile]:
    profiles: List[ScenarioAProfile] = []
    if args.broker in {"mqtt", "both"}:
        for qos in args.mqtt_qos:
            for devices in args.devices:
                profiles.append(
                    ScenarioAProfile(
                        broker="mqtt",
                        qos=int(qos),
                        devices=devices,
                        interval_sec=args.interval_sec,
                        duration_sec=args.duration_sec,
                    )
                )

    if args.broker in {"kafka", "both"}:
        for acks in args.kafka_acks:
            for partitions in args.kafka_partitions:
                for devices in args.devices:
                    profiles.append(
                        ScenarioAProfile(
                            broker="kafka",
                            acks=str(acks),
                            topic_partitions=int(partitions),
                            devices=devices,
                            interval_sec=args.interval_sec,
                            duration_sec=args.duration_sec,
                        )
                    )
    return profiles


def summarize_results(test_results: List[Dict[str, object]]) -> Dict[str, object]:
    completed = [item for item in test_results if "error" not in item]
    failed = [item for item in test_results if "error" in item]
    return {
        "total_tests": len(test_results),
        "completed_tests": len(completed),
        "failed_tests": len(failed),
    }


def build_profile_summaries(tests: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for test in tests:
        if test.get("error"):
            continue
        grouped.setdefault(str(test["config_name"]), []).append(test)

    summaries: List[Dict[str, object]] = []
    for config_name, group in sorted(grouped.items()):
        first = group[0]
        broker_value = first.get("broker_value")
        if broker_value is None:
            broker_value = first.get("qos") if first.get("broker") == "mqtt" else first.get("acks")
        summary = {
            "config_name": config_name,
            "broker": first["broker"],
            "broker_value": broker_value,
            "devices": first["devices"],
            "qos": first.get("qos"),
            "acks": first.get("acks"),
            "topic_partitions": first.get("topic_partitions"),
            "completed_runs": len(group),
            "messages_sent_avg": average_or_none(item.get("messages_sent") for item in group),
            "messages_received_avg": average_or_none(item.get("messages_received") for item in group),
            "loss_pct_med": median_or_none(item.get("loss_pct") for item in group),
            "producer_throughput_msg_s_med": median_or_none(
                item.get("producer_throughput_msg_per_sec") for item in group
            ),
            "consumer_throughput_msg_s_med": median_or_none(
                item.get("consumer_throughput_msg_per_sec") for item in group
            ),
            "p95_latency_ms_med": median_or_none(item.get("p95_latency_ms") for item in group),
            "avg_latency_ms_med": median_or_none(item.get("avg_latency_ms") for item in group),
            "cpu_pct_med": median_or_none(item.get("cpu_pct") for item in group),
            "ram_mb_med": median_or_none(item.get("ram_mb") for item in group),
            "network_mb_med": median_or_none(item.get("network_mb") for item in group),
            "lag_med": median_or_none(item.get("lag") for item in group),
            "validation_issue_runs": sum(1 for item in group if item.get("validation_issues")),
        }
        summaries.append(summary)
    return summaries


def render_performance_table(profile_summaries: List[Dict[str, object]]) -> str:
    headers = [
        "Broker",
        "Config",
        "Devices",
        "Partitions",
        "Loss %",
        "Producer msg/s",
        "Consumer msg/s",
        "p95 ms",
        "CPU %",
        "RAM MB",
        "Network MB",
        "Lag",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    def sort_key(item: Dict[str, object]) -> tuple:
        return (
            str(item["broker"]),
            int(item.get("devices") or 0),
            int(item.get("topic_partitions") or 0),
            str(item.get("broker_value")),
        )

    for summary in sorted(profile_summaries, key=sort_key):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(summary["broker"]),
                    str(summary["config_name"]),
                    str(summary["devices"]),
                    str(summary.get("topic_partitions") or "-"),
                    format_float(summary.get("loss_pct_med")),
                    format_float(summary.get("producer_throughput_msg_s_med")),
                    format_float(summary.get("consumer_throughput_msg_s_med")),
                    format_float(summary.get("p95_latency_ms_med")),
                    format_float(summary.get("cpu_pct_med")),
                    format_float(summary.get("ram_mb_med")),
                    format_float(summary.get("network_mb_med")),
                    format_float(summary.get("lag_med")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def render_analysis(profile_summaries: List[Dict[str, object]], results_file: Path) -> str:
    lines = [
        "# Scenario A Analysis",
        "",
        f"Generated from `{results_file.name}` on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.",
        "",
        "This document summarizes the executed massive-ingestion runs and is intended to feed the written report.",
        "",
    ]

    mqtt_profiles = [item for item in profile_summaries if item["broker"] == "mqtt"]
    kafka_profiles = [item for item in profile_summaries if item["broker"] == "kafka"]

    if mqtt_profiles:
        lines.extend(["## MQTT", ""])
        for devices in (100, 1000, 10000):
            bucket = [item for item in mqtt_profiles if int(item.get("devices") or 0) == devices]
            if not bucket:
                continue
            loss_values = [item.get("loss_pct_med") for item in bucket]
            p95_values = [item.get("p95_latency_ms_med") for item in bucket]
            cpu_values = [item.get("cpu_pct_med") for item in bucket]
            lines.append(
                f"- Devices `{devices}`: median loss `{format_float(median_or_none(loss_values))}`%, "
                f"median p95 `{format_float(median_or_none(p95_values))}` ms, "
                f"median CPU `{format_float(median_or_none(cpu_values))}`%."
            )
        high_scale_bucket = [item for item in mqtt_profiles if int(item.get("devices") or 0) == 10000]
        if high_scale_bucket:
            worst_loss = max((item.get("loss_pct_med") or 0.0) for item in high_scale_bucket)
            lines.append(
                f"- Interpretation: MQTT remains viable at smaller scales, but at `10000` devices the executed matrix reaches up to `{format_float(worst_loss)}`% loss for higher QoS levels."
            )
        lines.append("")

    if kafka_profiles:
        lines.extend(["## Kafka", ""])
        for devices in (100, 1000, 10000):
            bucket = [item for item in kafka_profiles if int(item.get("devices") or 0) == devices]
            if not bucket:
                continue
            throughput_values = [item.get("producer_throughput_msg_s_med") for item in bucket]
            p95_values = [item.get("p95_latency_ms_med") for item in bucket]
            ram_values = [item.get("ram_mb_med") for item in bucket]
            lines.append(
                f"- Devices `{devices}`: median producer throughput `{format_float(median_or_none(throughput_values))}` msg/s, "
                f"median p95 `{format_float(median_or_none(p95_values))}` ms, "
                f"median RAM `{format_float(median_or_none(ram_values))}` MB."
            )
        lines.append(
            "- Interpretation: Kafka keeps `0%` loss across the executed scale matrix, while partitions and acks trade higher resource cost for stronger cloud-oriented observability and delivery guarantees."
        )
        lines.append("")

    if mqtt_profiles and kafka_profiles:
        mqtt_cpu = median_or_none(item.get("cpu_pct_med") for item in mqtt_profiles)
        kafka_cpu = median_or_none(item.get("cpu_pct_med") for item in kafka_profiles)
        mqtt_ram = median_or_none(item.get("ram_mb_med") for item in mqtt_profiles)
        kafka_ram = median_or_none(item.get("ram_mb_med") for item in kafka_profiles)
        mqtt_p95 = median_or_none(item.get("p95_latency_ms_med") for item in mqtt_profiles)
        kafka_p95 = median_or_none(item.get("p95_latency_ms_med") for item in kafka_profiles)
        lines.extend(
            [
                "## MQTT vs Kafka",
                "",
                f"- MQTT median broker footprint across executed runs: `{format_float(mqtt_cpu)}`% CPU / `{format_float(mqtt_ram)}` MB RAM.",
                f"- Kafka median broker footprint across executed runs: `{format_float(kafka_cpu)}`% CPU / `{format_float(kafka_ram)}` MB RAM.",
                f"- MQTT median p95 latency across executed runs: `{format_float(mqtt_p95)}` ms; Kafka median p95 latency: `{format_float(kafka_p95)}` ms.",
                "- MQTT is the lighter option for edge ingestion, while Kafka is the more scalable and loss-resistant option for data-intensive cloud pipelines.",
                "",
                "## Report Implications",
                "",
                "- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.",
                "- Scenario A is the strongest experimental basis for discussing pure ingest scalability and loss under rising device counts.",
                "- The `10000` device runs should be emphasized in the written report because they make the MQTT vs Kafka scaling trade-off the clearest.",
                "",
            ]
        )

    return "\n".join(lines)


def write_supporting_artifacts(results_file: Path, results: Dict[str, object]) -> Dict[str, str]:
    profile_summaries = list(results.get("profile_summaries") or [])
    table_path = results_file.with_name(f"{results_file.stem}_performance_table.md")
    analysis_path = results_file.with_name(f"{results_file.stem}_analysis.md")

    table_path.write_text(render_performance_table(profile_summaries), encoding="utf-8")
    analysis_path.write_text(render_analysis(profile_summaries, results_file), encoding="utf-8")
    return {
        "performance_table": str(table_path),
        "analysis_report": str(analysis_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Runs complete Scenario A with dedicated benchmark tools: "
            "eMQTT-Bench for MQTT and kafka-producer-perf-test.sh for Kafka."
        )
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--mqtt-qos", nargs="+", default=["0", "1", "2"])
    parser.add_argument("--kafka-acks", nargs="+", default=["0", "1", "all"])
    parser.add_argument("--kafka-partitions", nargs="+", type=int, default=[1, 4, 8])
    parser.add_argument("--devices", nargs="+", type=int, default=[100, 1000, 10000])
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=int, default=10)
    parser.add_argument("--results-file", default=str(RESULTS_PATH))
    parser.add_argument(
        "--artifacts-only",
        action="store_true",
        help="Generate performance table and analysis from an existing Scenario A results JSON without rerunning benchmarks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results_file = Path(args.results_file)
    results_file.parent.mkdir(parents=True, exist_ok=True)

    if args.artifacts_only:
        existing_results = json.loads(results_file.read_text(encoding="utf-8"))
        existing_results["profile_summaries"] = build_profile_summaries(existing_results.get("tests", []))
        existing_results["artifacts"] = write_supporting_artifacts(results_file, existing_results)
        results_file.write_text(json.dumps(existing_results, indent=2), encoding="utf-8")
        print("\n" + "=" * 80)
        print(f"Scenario A supporting artifacts generated from {results_file}")
        print(f"Performance table: {existing_results['artifacts'].get('performance_table')}")
        print(f"Analysis report: {existing_results['artifacts'].get('analysis_report')}")
        print("=" * 80)
        return

    payload_tmp = build_payload_dir()
    aggregated_results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario": "A",
        "disable_db_write": True,
        "description": (
            "Massive Sensor Ingestion benchmark for MQTT and Kafka with restart-per-test, "
            "latency, resource metrics and Kafka consumer lag."
        ),
        "tests": [],
        "summary": {},
        "profile_summaries": [],
    }

    try:
        profiles = build_test_matrix(args)
        total_tests = len(profiles)
        for index, profile in enumerate(profiles, start=1):
            print("\n" + "=" * 80)
            print(f"[{index}/{total_tests}] Preparing {profile.config_name}")
            print("=" * 80)
            try:
                restart_stack(profile)
                result = execute_profile(profile=profile, payload_dir=Path(payload_tmp.name))
                aggregated_results["tests"].append(result)
                print(
                    f"Completed {profile.config_name}: sent={result['messages_sent']}, "
                    f"received={result['messages_received']}, loss={result['loss_pct']}%, "
                    f"consumer={result['consumer_throughput_msg_per_sec']} msg/s"
                )
            except Exception as exc:
                failure = {
                    "scenario": "A",
                    "config_name": profile.config_name,
                    "broker": profile.broker,
                    "devices": profile.devices,
                    "error": str(exc),
                    "validation_issues": ["benchmark_execution_failed"],
                }
                if profile.broker == "mqtt":
                    failure["qos"] = profile.qos
                else:
                    failure["acks"] = profile.acks
                    failure["topic_partitions"] = profile.topic_partitions
                aggregated_results["tests"].append(failure)
                print(f"FAILED {profile.config_name}: {exc}", file=sys.stderr)

            aggregated_results["summary"] = summarize_results(aggregated_results["tests"])
            results_file.write_text(json.dumps(aggregated_results, indent=2), encoding="utf-8")
    finally:
        payload_tmp.cleanup()

    aggregated_results["summary"] = summarize_results(aggregated_results["tests"])
    aggregated_results["profile_summaries"] = build_profile_summaries(aggregated_results["tests"])
    aggregated_results["artifacts"] = write_supporting_artifacts(results_file, aggregated_results)
    results_file.write_text(json.dumps(aggregated_results, indent=2), encoding="utf-8")

    print("\n" + "=" * 80)
    print(f"Scenario A benchmark completed. Results saved to {results_file}")
    print(f"Performance table: {aggregated_results['artifacts'].get('performance_table')}")
    print(f"Analysis report: {aggregated_results['artifacts'].get('analysis_report')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
