"""
Dedicated Scenario C runner.

Scenario C simulates a burst load transition from 50 msg/s to 5000 msg/s for a
short period, then returns the system to the 50 msg/s baseline. The benchmark:
- uses dedicated tools only (`emqtt-bench` for MQTT and
  `kafka-producer-perf-test.sh` for Kafka)
- measures backlog formation and drain time
- tracks broker CPU / RAM / network usage
- captures Kafka consumer lag and partition behavior
- exports JSON plus Markdown artifacts ready for the report
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import run_scenario_b as common


RESULTS_PATH = Path(__file__).resolve().parent / "scenario_c_results.json"
MQTT_PUB_OVERRUN_RE = re.compile(r"pub_overrun total=(?P<count>\d+)")


@dataclass(frozen=True)
class ScenarioCProfile:
    broker: str
    warmup_rate: int
    burst_rate: int
    warmup_sec: int
    burst_sec: int
    recovery_sec: int
    repeat_index: int
    qos: Optional[int] = None
    acks: Optional[str] = None
    topic_partitions: Optional[int] = None

    @property
    def total_duration_sec(self) -> int:
        return self.warmup_sec + self.burst_sec + self.recovery_sec

    @property
    def burst_extra_rate(self) -> int:
        return max(0, self.burst_rate - self.warmup_rate)

    @property
    def baseline_messages(self) -> int:
        return self.warmup_rate * self.total_duration_sec

    @property
    def burst_messages(self) -> int:
        return self.burst_extra_rate * self.burst_sec

    @property
    def config_name(self) -> str:
        if self.broker == "mqtt":
            return f"mqtt_qos_{self.qos}"
        return f"kafka_acks_{self.acks}_partitions_{self.topic_partitions}"

    @property
    def run_name(self) -> str:
        return f"{self.config_name}_repeat_{self.repeat_index}"

    @property
    def broker_value(self) -> str:
        return str(self.qos) if self.broker == "mqtt" else str(self.acks)


def cleanup_scenario_c_tool_containers() -> None:
    result = common.run_cmd(["docker", "ps", "-aq", "--filter", "name=scenario-c-"], timeout=60, check=False)
    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    for container_id in container_ids:
        common.run_cmd(["docker", "rm", "-f", container_id], timeout=60, check=False)


def cleanup_stack_residue() -> None:
    common.cleanup_stack_residue()
    cleanup_scenario_c_tool_containers()


def restart_stack(profile: ScenarioCProfile, *, disable_db_write: bool, build_images: bool) -> None:
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
    up_cmd = ["docker", "compose", "up", "-d"]
    if build_images:
        up_cmd.append("--build")

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        common.run_cmd(
            ["docker", "compose", "down", "--remove-orphans"],
            env_overrides=env_overrides,
            timeout=180,
            check=False,
        )
        cleanup_stack_residue()
        time.sleep(3)
        try:
            common.run_cmd(up_cmd, env_overrides=env_overrides, timeout=900)
            common.wait_for_services()
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_attempts or not common.is_transient_stack_error(exc):
                raise
            print(
                f"Transient stack startup issue on attempt {attempt}/{max_attempts}: {exc}",
                file=sys.stderr,
            )
            cleanup_stack_residue()
            time.sleep(5)


def parse_kafka_progress_records(output: str) -> int:
    matches = list(common.KAFKA_SUMMARY_RE.finditer(output))
    if not matches:
        return 0
    try:
        return int(matches[-1].group("records"))
    except ValueError:
        return 0


def parse_mqtt_overrun_records(output: str) -> int:
    matches = [int(match.group("count")) for match in MQTT_PUB_OVERRUN_RE.finditer(output)]
    return matches[-1] if matches else 0


def median_or_none(values: Iterable[Optional[float]]) -> Optional[float]:
    actual = [value for value in values if value is not None]
    if not actual:
        return None
    return float(statistics.median(actual))


def choose_mqtt_rate_shape(rate: int) -> Tuple[int, int, float]:
    if rate <= 0:
        return 1, 1000, 1.0

    best = None
    max_connections = min(max(rate, 1), 500)
    for connections in range(1, max_connections + 1):
        interval_ms = max(1, round((connections * 1000) / rate))
        actual_rate = connections * 1000.0 / interval_ms
        error = abs(actual_rate - rate)
        candidate = (error, connections, interval_ms, actual_rate)
        if best is None or candidate < best:
            best = candidate

    assert best is not None
    _, connections, interval_ms, actual_rate = best
    return connections, interval_ms, actual_rate


def start_mqtt_latency_probe(profile: ScenarioCProfile) -> Dict[str, object]:
    port = common.find_free_port()
    container_name = f"scenario-c-mqtt-probe-{profile.qos}-{profile.repeat_index}-{int(time.time() * 1000)}"

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
        common.MQTT_TOOL_IMAGE,
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
        common.MQTT_TOPIC,
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
    common.run_cmd(cmd, timeout=120)
    common.wait_for_probe_metrics(port)
    return {
        "container_name": container_name,
        "port": port,
    }


def start_mqtt_publisher(
    *,
    profile: ScenarioCProfile,
    payload_dir: Path,
    rate: int,
    total_messages: int,
    kind: str,
) -> Dict[str, object]:
    connections, interval_ms, actual_rate = choose_mqtt_rate_shape(rate)
    container_name = f"scenario-c-mqtt-{kind}-{profile.qos}-{profile.repeat_index}-{int(time.time() * 1000)}"
    connect_rate = max(1, min(connections, 1000))

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
        common.MQTT_TOOL_IMAGE,
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
        str(connections),
        "-R",
        str(connect_rate),
        "-I",
        str(interval_ms),
        "-t",
        common.MQTT_TOPIC,
        "-q",
        str(profile.qos or 0),
        "-L",
        str(total_messages),
        "-w",
        "true",
        "--payload-hdrs",
        "ts",
        "-m",
        "template:///payloads/mqtt_payload_template.json",
    ]
    if (profile.qos or 0) > 0:
        cmd.extend(["-F", "20"])

    common.run_cmd(cmd, timeout=120)
    return {
        "kind": kind,
        "container_name": container_name,
        "target_rate": rate,
        "actual_configured_rate": round(actual_rate, 3),
        "planned_messages": total_messages,
        "connections": connections,
        "interval_ms": interval_ms,
    }


def start_kafka_publisher(
    *,
    profile: ScenarioCProfile,
    payload_dir: Path,
    rate: int,
    total_messages: int,
    kind: str,
) -> Dict[str, object]:
    container_name = (
        f"scenario-c-kafka-{kind}-{profile.acks}-{profile.topic_partitions}-"
        f"{profile.repeat_index}-{int(time.time() * 1000)}"
    )
    throughput = max(1, rate)

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
        common.KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic",
        common.KAFKA_TOPIC,
        "--num-records",
        str(total_messages),
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
    common.run_cmd(cmd, timeout=120)
    return {
        "kind": kind,
        "container_name": container_name,
        "target_rate": rate,
        "actual_configured_rate": float(throughput),
        "planned_messages": total_messages,
    }


def read_publisher_progress(profile: ScenarioCProfile, publisher: Dict[str, object]) -> Dict[str, object]:
    logs_text = common.docker_logs(str(publisher["container_name"]))
    if profile.broker == "mqtt":
        sent_messages = common.parse_mqtt_progress_records(logs_text)
        return {
            "messages_sent": sent_messages,
            "publish_overrun_messages": parse_mqtt_overrun_records(logs_text),
            "tool_summary": None,
            "logs_text": logs_text,
            "running": common.docker_container_running(str(publisher["container_name"])),
        }

    return {
        "messages_sent": parse_kafka_progress_records(logs_text),
        "publish_overrun_messages": 0,
        "tool_summary": common.parse_kafka_summary(logs_text),
        "logs_text": logs_text,
        "running": common.docker_container_running(str(publisher["container_name"])),
    }


def get_runtime_snapshot(
    *,
    profile: ScenarioCProfile,
    publishers: List[Dict[str, object]],
    benchmark_started_at: float,
    phase_name: str,
    phase_started_at: float,
    cached_kafka_lag: Optional[Dict[str, object]],
    force_kafka_lag: bool,
) -> Tuple[Dict[str, object], Optional[Dict[str, object]]]:
    storage_metrics = common.fetch_text(f"{common.STORAGE_URL}/metrics")
    analytics_metrics = common.fetch_text(f"{common.ANALYTICS_URL}/metrics")

    publisher_snapshots: List[Dict[str, object]] = []
    sent_messages = 0
    publish_overrun_messages = 0
    running_publishers = 0
    for publisher in publishers:
        progress = read_publisher_progress(profile, publisher)
        sent_messages += int(progress["messages_sent"])
        publish_overrun_messages += int(progress["publish_overrun_messages"])
        if progress["running"]:
            running_publishers += 1
        publisher_snapshots.append(
            {
                "kind": publisher["kind"],
                "container_name": publisher["container_name"],
                "messages_sent": int(progress["messages_sent"]),
                "publish_overrun_messages": int(progress["publish_overrun_messages"]),
                "running": bool(progress["running"]),
                "target_rate": int(publisher["target_rate"]),
                "configured_rate": publisher["actual_configured_rate"],
                "planned_messages": int(publisher["planned_messages"]),
            }
        )

    storage_received = int(common.parse_prometheus_counter(storage_metrics, "storage_messages_received_total"))
    analytics_processed = int(common.parse_prometheus_counter(analytics_metrics, "analytics_messages_processed_total"))
    storage_queue_depth = int(
        common.find_metric_value(
            storage_metrics,
            "storage_batch_queue_depth",
            {"broker_type": profile.broker},
        )
    )
    analytics_window_depth = int(
        common.find_metric_value(
            analytics_metrics,
            "analytics_window_event_queue_depth",
            {"broker_type": profile.broker},
        )
    )

    kafka_lag_groups = cached_kafka_lag
    if profile.broker == "kafka" and (force_kafka_lag or kafka_lag_groups is None):
        kafka_lag_groups = {
            group_id: common.collect_kafka_consumer_lag(group_id)
            for group_id in common.KAFKA_LAG_GROUPS
        }

    max_kafka_lag = 0
    if kafka_lag_groups:
        max_kafka_lag = max(
            int(group.get("lag", 0))
            for group in kafka_lag_groups.values()
        )

    snapshot = {
        "timestamp": round(time.time(), 3),
        "elapsed_sec": round(time.time() - benchmark_started_at, 3),
        "phase": phase_name,
        "phase_elapsed_sec": round(time.time() - phase_started_at, 3),
        "sent_messages": sent_messages,
        "storage_received_messages": storage_received,
        "analytics_processed_messages": analytics_processed,
        "storage_batch_queue_depth": storage_queue_depth,
        "analytics_window_queue_depth": analytics_window_depth,
        "pipeline_backlog_messages": max(0, storage_received - analytics_processed),
        "mqtt_publish_overrun_messages": publish_overrun_messages,
        "publisher_running_count": running_publishers,
        "publisher_snapshots": publisher_snapshots,
        "kafka_consumer_lag_groups": kafka_lag_groups,
        "max_kafka_lag": max_kafka_lag,
    }
    return snapshot, kafka_lag_groups


def capture_phase(
    *,
    profile: ScenarioCProfile,
    phase_name: str,
    duration_sec: int,
    benchmark_started_at: float,
    publishers: List[Dict[str, object]],
    sample_interval_sec: float,
    kafka_lag_sample_interval_sec: float,
    cached_kafka_lag: Optional[Dict[str, object]],
) -> Tuple[Dict[str, object], Optional[Dict[str, object]]]:
    phase_started_at = time.time()
    last_kafka_lag_collect = 0.0
    timeline: List[Dict[str, object]] = []

    start_snapshot, cached_kafka_lag = get_runtime_snapshot(
        profile=profile,
        publishers=publishers,
        benchmark_started_at=benchmark_started_at,
        phase_name=phase_name,
        phase_started_at=phase_started_at,
        cached_kafka_lag=cached_kafka_lag,
        force_kafka_lag=profile.broker == "kafka",
    )
    if profile.broker == "kafka":
        last_kafka_lag_collect = time.time()
    timeline.append(start_snapshot)

    while True:
        elapsed = time.time() - phase_started_at
        if elapsed >= duration_sec:
            break
        sleep_for = min(sample_interval_sec, max(duration_sec - elapsed, 0.0))
        if sleep_for > 0:
            time.sleep(sleep_for)

        force_kafka_lag = False
        if profile.broker == "kafka" and (time.time() - last_kafka_lag_collect) >= kafka_lag_sample_interval_sec:
            force_kafka_lag = True
            last_kafka_lag_collect = time.time()

        snapshot, cached_kafka_lag = get_runtime_snapshot(
            profile=profile,
            publishers=publishers,
            benchmark_started_at=benchmark_started_at,
            phase_name=phase_name,
            phase_started_at=phase_started_at,
            cached_kafka_lag=cached_kafka_lag,
            force_kafka_lag=force_kafka_lag,
        )
        timeline.append(snapshot)

    end_snapshot, cached_kafka_lag = get_runtime_snapshot(
        profile=profile,
        publishers=publishers,
        benchmark_started_at=benchmark_started_at,
        phase_name=phase_name,
        phase_started_at=phase_started_at,
        cached_kafka_lag=cached_kafka_lag,
        force_kafka_lag=profile.broker == "kafka",
    )
    if end_snapshot["timestamp"] != timeline[-1]["timestamp"]:
        timeline.append(end_snapshot)

    return {
        "name": phase_name,
        "duration_sec": round(end_snapshot["elapsed_sec"] - start_snapshot["elapsed_sec"], 3),
        "start_snapshot": start_snapshot,
        "end_snapshot": end_snapshot,
        "timeline": timeline,
    }, cached_kafka_lag


def wait_for_publishers_completion(
    profile: ScenarioCProfile,
    publishers: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    for publisher in publishers:
        container_name = str(publisher["container_name"])
        target_rate = max(int(publisher["target_rate"]), 1)
        planned_messages = int(publisher["planned_messages"])
        expected_duration = max(60, int((planned_messages / target_rate) * 6))
        wait_result = common.run_cmd(["docker", "wait", container_name], timeout=expected_duration, check=False)
        exit_code = int(wait_result.stdout.strip() or "1")
        logs_text = common.docker_logs(container_name)

        result = {
            "kind": publisher["kind"],
            "container_name": container_name,
            "target_rate": publisher["target_rate"],
            "configured_rate": publisher["actual_configured_rate"],
            "planned_messages": publisher["planned_messages"],
            "completed_cleanly": exit_code == 0,
            "container_exit_code": exit_code,
            "tool_stdout": logs_text,
        }

        if profile.broker == "mqtt":
            result["messages_sent"] = common.parse_mqtt_progress_records(logs_text)
            result["publish_overrun_messages"] = parse_mqtt_overrun_records(logs_text)
        else:
            summary = common.parse_kafka_summary(logs_text)
            result["messages_sent"] = int(summary.get("records") or parse_kafka_progress_records(logs_text))
            result["publish_overrun_messages"] = 0
            result["tool_summary"] = summary

        results.append(result)

    return results


def finalize_mqtt_probe(
    probe: Dict[str, object],
    *,
    expected_messages: int,
    timeout_sec: int,
) -> Dict[str, object]:
    probe_wait = common.wait_for_probe_receipts(
        port=int(probe["port"]),
        expected_messages=expected_messages,
        timeout_sec=timeout_sec,
    )
    probe_metrics_text = common.fetch_text(f"http://localhost:{int(probe['port'])}/metrics", timeout=10)
    latency_histogram = common.parse_histogram(probe_metrics_text, "e2e_latency")
    return {
        "probe_received_messages": int(probe_wait["received_messages"]),
        "probe_settled": bool(probe_wait["settled"]),
        "probe_settle_reason": probe_wait["settle_reason"],
        "probe_completion_sec": probe_wait["completion_sec"],
        "latency_summary": common.compute_histogram_latency_summary(latency_histogram),
        "max_latency_is_histogram_upper_bound": True,
    }


def aggregate_kafka_latency(publisher_results: List[Dict[str, object]]) -> Dict[str, Optional[float]]:
    summaries = [result.get("tool_summary") for result in publisher_results if result.get("tool_summary")]
    if not summaries:
        return {
            "avg_latency_ms": None,
            "p95_latency_ms": None,
            "max_latency_ms": None,
            "observations": 0,
        }

    total_records = sum(int(summary.get("records", 0)) for summary in summaries)
    weighted_avg = 0.0
    if total_records > 0:
        weighted_avg = sum(
            float(summary.get("avg_latency_ms", 0.0)) * int(summary.get("records", 0))
            for summary in summaries
        ) / total_records

    p95_candidates = [float(summary["p95_latency_ms"]) for summary in summaries if "p95_latency_ms" in summary]
    max_candidates = [float(summary["max_latency_ms"]) for summary in summaries if "max_latency_ms" in summary]
    return {
        "avg_latency_ms": round(weighted_avg, 3) if total_records else None,
        "p95_latency_ms": round(max(p95_candidates), 3) if p95_candidates else None,
        "max_latency_ms": round(max(max_candidates), 3) if max_candidates else None,
        "observations": total_records,
    }


def is_pipeline_drained(profile: ScenarioCProfile, snapshot: Dict[str, object]) -> bool:
    if int(snapshot["publisher_running_count"]) > 0:
        return False
    if int(snapshot["storage_batch_queue_depth"]) > 0:
        return False
    if int(snapshot["analytics_window_queue_depth"]) > 0:
        return False
    if int(snapshot["pipeline_backlog_messages"]) > 0:
        return False
    if profile.broker == "kafka" and int(snapshot.get("max_kafka_lag") or 0) > 0:
        return False
    return True


def wait_for_drain(
    *,
    profile: ScenarioCProfile,
    benchmark_started_at: float,
    publishers: List[Dict[str, object]],
    sample_interval_sec: float,
    kafka_lag_sample_interval_sec: float,
    cached_kafka_lag: Optional[Dict[str, object]],
    timeout_sec: int,
    stable_polls: int = 3,
) -> Tuple[Dict[str, object], Optional[Dict[str, object]]]:
    phase_started_at = time.time()
    timeline: List[Dict[str, object]] = []
    last_kafka_lag_collect = 0.0
    clear_stable_count = 0

    while True:
        force_kafka_lag = False
        if profile.broker == "kafka" and (time.time() - last_kafka_lag_collect) >= kafka_lag_sample_interval_sec:
            force_kafka_lag = True
            last_kafka_lag_collect = time.time()

        snapshot, cached_kafka_lag = get_runtime_snapshot(
            profile=profile,
            publishers=publishers,
            benchmark_started_at=benchmark_started_at,
            phase_name="drain",
            phase_started_at=phase_started_at,
            cached_kafka_lag=cached_kafka_lag,
            force_kafka_lag=force_kafka_lag or profile.broker == "kafka",
        )
        timeline.append(snapshot)

        if is_pipeline_drained(profile, snapshot):
            clear_stable_count += 1
        else:
            clear_stable_count = 0

        if clear_stable_count >= stable_polls:
            settle_reason = "pipeline_drained"
            break
        if (time.time() - phase_started_at) >= timeout_sec:
            settle_reason = "timeout"
            break
        time.sleep(sample_interval_sec)

    start_snapshot = timeline[0]
    end_snapshot = timeline[-1]
    return {
        "name": "drain",
        "duration_sec": round(end_snapshot["elapsed_sec"] - start_snapshot["elapsed_sec"], 3),
        "start_snapshot": start_snapshot,
        "end_snapshot": end_snapshot,
        "timeline": timeline,
        "settled": settle_reason == "pipeline_drained",
        "settle_reason": settle_reason,
    }, cached_kafka_lag


def delta_between_snapshots(start: Dict[str, object], end: Dict[str, object], key: str) -> int:
    return max(0, int(end[key]) - int(start[key]))


def max_in_timeline(entries: Iterable[Dict[str, object]], key: str) -> int:
    values = [int(entry.get(key, 0)) for entry in entries]
    return max(values) if values else 0


def compute_phase_summary(phase: Dict[str, object]) -> Dict[str, object]:
    start_snapshot = phase["start_snapshot"]
    end_snapshot = phase["end_snapshot"]
    duration = max(float(phase["duration_sec"]), 0.001)
    timeline = phase["timeline"]

    sent_delta = delta_between_snapshots(start_snapshot, end_snapshot, "sent_messages")
    storage_delta = delta_between_snapshots(start_snapshot, end_snapshot, "storage_received_messages")
    analytics_delta = delta_between_snapshots(start_snapshot, end_snapshot, "analytics_processed_messages")

    return {
        "duration_sec": round(duration, 3),
        "messages_sent": sent_delta,
        "storage_received_messages": storage_delta,
        "analytics_processed_messages": analytics_delta,
        "publish_throughput_msg_s": round(sent_delta / duration, 3),
        "storage_throughput_msg_s": round(storage_delta / duration, 3),
        "analytics_throughput_msg_s": round(analytics_delta / duration, 3),
        "peak_pipeline_backlog_messages": max_in_timeline(timeline, "pipeline_backlog_messages"),
        "peak_storage_batch_queue_depth": max_in_timeline(timeline, "storage_batch_queue_depth"),
        "peak_analytics_window_queue_depth": max_in_timeline(timeline, "analytics_window_queue_depth"),
        "peak_mqtt_publish_overrun_messages": max_in_timeline(timeline, "mqtt_publish_overrun_messages"),
        "peak_consumer_lag": max_in_timeline(timeline, "max_kafka_lag"),
    }


def flatten_entries(phases: List[Dict[str, object]]) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for phase in phases:
        entries.extend(phase["timeline"])
    return entries


def first_recovery_sec_to_backlog_zero(
    *,
    profile: ScenarioCProfile,
    recovery_start_elapsed_sec: float,
    entries: List[Dict[str, object]],
) -> Optional[float]:
    for entry in entries:
        if float(entry["elapsed_sec"]) < recovery_start_elapsed_sec:
            continue
        if is_pipeline_drained(profile, entry):
            return round(float(entry["elapsed_sec"]) - recovery_start_elapsed_sec, 3)
    return None


def time_to_peak_backlog_from_burst_start(
    burst_start_elapsed_sec: float,
    entries: List[Dict[str, object]],
) -> Optional[float]:
    burst_entries = [entry for entry in entries if float(entry["elapsed_sec"]) >= burst_start_elapsed_sec]
    if not burst_entries:
        return None
    peak_value = max(int(entry["pipeline_backlog_messages"]) for entry in burst_entries)
    for entry in burst_entries:
        if int(entry["pipeline_backlog_messages"]) == peak_value:
            return round(float(entry["elapsed_sec"]) - burst_start_elapsed_sec, 3)
    return None


def first_recovery_sec_to_baseline_storage_throughput(
    *,
    recovery_start_elapsed_sec: float,
    entries: List[Dict[str, object]],
    baseline_storage_throughput: float,
    threshold_pct: float = 0.95,
    trailing_window_entries: int = 3,
) -> Optional[float]:
    if baseline_storage_throughput <= 0:
        return None
    eligible = [entry for entry in entries if float(entry["elapsed_sec"]) >= recovery_start_elapsed_sec]
    if len(eligible) < 2:
        return None

    for index in range(1, len(eligible)):
        start_index = max(0, index - trailing_window_entries)
        start_entry = eligible[start_index]
        end_entry = eligible[index]
        duration = float(end_entry["elapsed_sec"]) - float(start_entry["elapsed_sec"])
        if duration <= 0:
            continue
        throughput = (
            int(end_entry["storage_received_messages"]) - int(start_entry["storage_received_messages"])
        ) / duration
        if throughput >= (baseline_storage_throughput * threshold_pct):
            return round(float(end_entry["elapsed_sec"]) - recovery_start_elapsed_sec, 3)
    return None


def summarize_publishers(publisher_results: List[Dict[str, object]]) -> Dict[str, object]:
    baseline = next((item for item in publisher_results if item["kind"] == "baseline"), None)
    burst = next((item for item in publisher_results if item["kind"] == "burst"), None)
    return {
        "baseline": baseline,
        "burst": burst,
    }


def execute_profile(
    *,
    profile: ScenarioCProfile,
    payload_dir: Path,
    disable_db_write: bool,
    build_images: bool,
    sample_interval_sec: float,
    kafka_lag_sample_interval_sec: float,
    drain_timeout_sec: int,
) -> Dict[str, object]:
    restart_stack(profile, disable_db_write=disable_db_write, build_images=build_images)

    resource_sampler = common.ResourceSampler(common.BROKER_CONTAINER_NAME[profile.broker])
    resource_sampler.start()
    probe: Optional[Dict[str, object]] = None
    publishers: List[Dict[str, object]] = []
    cached_kafka_lag: Optional[Dict[str, object]] = None

    try:
        if profile.broker == "mqtt":
            probe = start_mqtt_latency_probe(profile)

        benchmark_started_at = time.time()
        if profile.broker == "mqtt":
            baseline_publisher = start_mqtt_publisher(
                profile=profile,
                payload_dir=payload_dir,
                rate=profile.warmup_rate,
                total_messages=profile.baseline_messages,
                kind="baseline",
            )
        else:
            baseline_publisher = start_kafka_publisher(
                profile=profile,
                payload_dir=payload_dir,
                rate=profile.warmup_rate,
                total_messages=profile.baseline_messages,
                kind="baseline",
            )
        publishers.append(baseline_publisher)

        warmup_phase, cached_kafka_lag = capture_phase(
            profile=profile,
            phase_name="warmup",
            duration_sec=profile.warmup_sec,
            benchmark_started_at=benchmark_started_at,
            publishers=publishers,
            sample_interval_sec=sample_interval_sec,
            kafka_lag_sample_interval_sec=kafka_lag_sample_interval_sec,
            cached_kafka_lag=cached_kafka_lag,
        )

        if profile.burst_extra_rate > 0:
            if profile.broker == "mqtt":
                burst_publisher = start_mqtt_publisher(
                    profile=profile,
                    payload_dir=payload_dir,
                    rate=profile.burst_extra_rate,
                    total_messages=profile.burst_messages,
                    kind="burst",
                )
            else:
                burst_publisher = start_kafka_publisher(
                    profile=profile,
                    payload_dir=payload_dir,
                    rate=profile.burst_extra_rate,
                    total_messages=profile.burst_messages,
                    kind="burst",
                )
            publishers.append(burst_publisher)

        burst_phase, cached_kafka_lag = capture_phase(
            profile=profile,
            phase_name="burst",
            duration_sec=profile.burst_sec,
            benchmark_started_at=benchmark_started_at,
            publishers=publishers,
            sample_interval_sec=sample_interval_sec,
            kafka_lag_sample_interval_sec=kafka_lag_sample_interval_sec,
            cached_kafka_lag=cached_kafka_lag,
        )

        recovery_phase, cached_kafka_lag = capture_phase(
            profile=profile,
            phase_name="recovery",
            duration_sec=profile.recovery_sec,
            benchmark_started_at=benchmark_started_at,
            publishers=publishers,
            sample_interval_sec=sample_interval_sec,
            kafka_lag_sample_interval_sec=kafka_lag_sample_interval_sec,
            cached_kafka_lag=cached_kafka_lag,
        )

        publisher_results = wait_for_publishers_completion(profile, publishers)
        drain_phase, cached_kafka_lag = wait_for_drain(
            profile=profile,
            benchmark_started_at=benchmark_started_at,
            publishers=publishers,
            sample_interval_sec=sample_interval_sec,
            kafka_lag_sample_interval_sec=kafka_lag_sample_interval_sec,
            cached_kafka_lag=cached_kafka_lag,
            timeout_sec=drain_timeout_sec,
        )
        total_sent = sum(int(result.get("messages_sent", 0)) for result in publisher_results)
        publisher_summary = summarize_publishers(publisher_results)
        mqtt_probe_result = None
        latency_summary = {
            "avg_latency_ms": None,
            "p95_latency_ms": None,
            "max_latency_ms": None,
            "observations": 0,
        }
        latency_source = None

        if profile.broker == "mqtt" and probe:
            mqtt_probe_result = finalize_mqtt_probe(
                probe,
                expected_messages=total_sent,
                timeout_sec=max(60, profile.total_duration_sec * 4),
            )
            latency_summary = mqtt_probe_result["latency_summary"]
            latency_source = "emqtt-bench_e2e_latency_histogram"
        elif profile.broker == "kafka":
            latency_summary = aggregate_kafka_latency(publisher_results)
            latency_source = "kafka-producer-perf-test_summary_aggregate"

        phases = [warmup_phase, burst_phase, recovery_phase, drain_phase]
        all_entries = flatten_entries(phases)
        warmup_summary = compute_phase_summary(warmup_phase)
        burst_summary = compute_phase_summary(burst_phase)
        recovery_summary = compute_phase_summary(recovery_phase)
        drain_summary = compute_phase_summary(drain_phase)
        resource_summary = resource_sampler.summary()

        final_snapshot = drain_phase["end_snapshot"]
        total_received = int(final_snapshot["storage_received_messages"])
        total_processed = int(final_snapshot["analytics_processed_messages"])
        loss_messages = max(0, total_sent - total_received)
        loss_pct = (loss_messages / total_sent * 100.0) if total_sent else 0.0
        duplicate_storage_messages = max(0, total_received - total_sent)
        duplicate_analytics_messages = max(0, total_processed - total_sent)
        peak_pipeline_backlog = max_in_timeline(all_entries, "pipeline_backlog_messages")
        peak_storage_queue = max_in_timeline(all_entries, "storage_batch_queue_depth")
        peak_analytics_queue = max_in_timeline(all_entries, "analytics_window_queue_depth")
        peak_kafka_lag = max_in_timeline(all_entries, "max_kafka_lag")
        peak_mqtt_overrun = max_in_timeline(all_entries, "mqtt_publish_overrun_messages")

        recovery_start_elapsed = float(recovery_phase["start_snapshot"]["elapsed_sec"])
        burst_start_elapsed = float(burst_phase["start_snapshot"]["elapsed_sec"])
        recovery_sec_to_backlog_zero = first_recovery_sec_to_backlog_zero(
            profile=profile,
            recovery_start_elapsed_sec=recovery_start_elapsed,
            entries=all_entries,
        )
        recovery_sec_to_baseline_storage = first_recovery_sec_to_baseline_storage_throughput(
            recovery_start_elapsed_sec=recovery_start_elapsed,
            entries=all_entries,
            baseline_storage_throughput=float(warmup_summary["storage_throughput_msg_s"]),
        )
        time_to_peak_backlog_sec = time_to_peak_backlog_from_burst_start(
            burst_start_elapsed_sec=burst_start_elapsed,
            entries=all_entries,
        )

        final_kafka_lag_groups = cached_kafka_lag if profile.broker == "kafka" else None
        validation_issues: List[str] = []
        if total_processed > total_received:
            validation_issues.append("analytics_processed_exceeded_storage_received")
        if not drain_phase["settled"]:
            validation_issues.append("pipeline_did_not_drain_before_timeout")
        if profile.broker == "kafka" and final_kafka_lag_groups:
            if any(int(group.get("lag", 0)) > 0 for group in final_kafka_lag_groups.values()):
                validation_issues.append("kafka_consumer_lag_nonzero_after_drain")
        if mqtt_probe_result:
            if not mqtt_probe_result["probe_settled"]:
                validation_issues.append("mqtt_latency_probe_did_not_settle")
            if int(mqtt_probe_result["probe_received_messages"]) < total_sent:
                validation_issues.append("mqtt_latency_probe_received_less_than_sent")
        if duplicate_storage_messages > 0:
            validation_issues.append("storage_received_exceeded_sent_possible_duplicates")
        if duplicate_analytics_messages > 0:
            validation_issues.append("analytics_processed_exceeded_sent_possible_duplicates")

        return {
            "scenario": "C",
            "config_name": profile.config_name,
            "run_name": profile.run_name,
            "repeat_index": profile.repeat_index,
            "broker": profile.broker,
            "broker_value": profile.broker_value,
            "qos": profile.qos if profile.broker == "mqtt" else None,
            "acks": profile.acks if profile.broker == "kafka" else None,
            "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
            "warmup_rate_msg_s": profile.warmup_rate,
            "burst_rate_msg_s": profile.burst_rate,
            "burst_extra_rate_msg_s": profile.burst_extra_rate,
            "warmup_sec": profile.warmup_sec,
            "burst_sec": profile.burst_sec,
            "recovery_sec": profile.recovery_sec,
            "baseline_planned_messages": profile.baseline_messages,
            "burst_planned_messages": profile.burst_messages,
            "messages_sent": total_sent,
            "messages_received": total_received,
            "analytics_processed_messages": total_processed,
            "loss_messages": loss_messages,
            "loss_pct": round(loss_pct, 3),
            "duplicate_storage_messages_estimate": duplicate_storage_messages,
            "duplicate_analytics_messages_estimate": duplicate_analytics_messages,
            "avg_latency_ms": latency_summary["avg_latency_ms"],
            "p95_latency_ms": latency_summary["p95_latency_ms"],
            "max_latency_ms": latency_summary["max_latency_ms"],
            "latency_observations": latency_summary["observations"],
            "latency_source": latency_source,
            "cpu_pct": resource_summary["cpu_pct"],
            "cpu_pct_avg": resource_summary["cpu_pct_avg"],
            "ram_mb": resource_summary["ram_mb"],
            "ram_mb_avg": resource_summary["ram_mb_avg"],
            "network_mb": resource_summary["network_mb"],
            "network_rx_mb": resource_summary["network_rx_mb"],
            "network_tx_mb": resource_summary["network_tx_mb"],
            "resource_sample_count": resource_summary["sample_count"],
            "warmup_phase": warmup_summary,
            "burst_phase": burst_summary,
            "recovery_phase": recovery_summary,
            "drain_phase": {
                **drain_summary,
                "settled": drain_phase["settled"],
                "settle_reason": drain_phase["settle_reason"],
            },
            "peak_pipeline_backlog_messages": peak_pipeline_backlog,
            "peak_storage_batch_queue_depth": peak_storage_queue,
            "peak_analytics_window_queue_depth": peak_analytics_queue,
            "peak_consumer_lag": peak_kafka_lag,
            "peak_mqtt_publish_overrun_messages": peak_mqtt_overrun,
            "time_to_peak_backlog_sec": time_to_peak_backlog_sec,
            "recovery_sec_to_backlog_zero": recovery_sec_to_backlog_zero,
            "recovery_sec_to_baseline_storage_throughput": recovery_sec_to_baseline_storage,
            "drain_time_sec": drain_phase["duration_sec"],
            "validation_issues": validation_issues,
            "publisher_results": publisher_summary,
            "mqtt_probe_result": mqtt_probe_result,
            "kafka_consumer_lag_final": final_kafka_lag_groups,
            "timeline_warmup": warmup_phase["timeline"],
            "timeline_burst": burst_phase["timeline"],
            "timeline_recovery": recovery_phase["timeline"],
            "timeline_drain": drain_phase["timeline"],
        }
    finally:
        resource_sampler.stop()
        for publisher in publishers:
            common.stop_container(str(publisher["container_name"]))
        if probe:
            common.stop_container(str(probe["container_name"]))


def summarize_results(tests: List[Dict[str, object]]) -> Dict[str, object]:
    completed = [item for item in tests if not item.get("error")]
    failed = [item for item in tests if item.get("error")]
    return {
        "total_tests": len(tests),
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
        summary = {
            "config_name": config_name,
            "broker": first["broker"],
            "broker_value": first["broker_value"],
            "qos": first.get("qos"),
            "acks": first.get("acks"),
            "topic_partitions": first.get("topic_partitions"),
            "completed_repeats": len(group),
            "loss_pct_med": round(median_or_none([float(item["loss_pct"]) for item in group]) or 0.0, 3),
            "p95_latency_ms_med": median_or_none([item.get("p95_latency_ms") for item in group]),
            "cpu_pct_med": median_or_none([item.get("cpu_pct") for item in group]),
            "ram_mb_med": median_or_none([item.get("ram_mb") for item in group]),
            "network_mb_med": median_or_none([item.get("network_mb") for item in group]),
            "warmup_storage_throughput_msg_s_med": median_or_none(
                [item["warmup_phase"]["storage_throughput_msg_s"] for item in group]
            ),
            "burst_storage_throughput_msg_s_med": median_or_none(
                [item["burst_phase"]["storage_throughput_msg_s"] for item in group]
            ),
            "burst_publish_throughput_msg_s_med": median_or_none(
                [item["burst_phase"]["publish_throughput_msg_s"] for item in group]
            ),
            "peak_pipeline_backlog_messages_med": median_or_none(
                [float(item["peak_pipeline_backlog_messages"]) for item in group]
            ),
            "peak_storage_batch_queue_depth_med": median_or_none(
                [float(item["peak_storage_batch_queue_depth"]) for item in group]
            ),
            "peak_analytics_window_queue_depth_med": median_or_none(
                [float(item["peak_analytics_window_queue_depth"]) for item in group]
            ),
            "peak_consumer_lag_med": median_or_none(
                [float(item["peak_consumer_lag"]) for item in group]
            ),
            "peak_mqtt_publish_overrun_messages_med": median_or_none(
                [float(item["peak_mqtt_publish_overrun_messages"]) for item in group]
            ),
            "recovery_sec_to_backlog_zero_med": median_or_none(
                [item.get("recovery_sec_to_backlog_zero") for item in group]
            ),
            "recovery_sec_to_baseline_storage_throughput_med": median_or_none(
                [item.get("recovery_sec_to_baseline_storage_throughput") for item in group]
            ),
            "validation_issue_runs": sum(1 for item in group if item.get("validation_issues")),
        }
        summaries.append(summary)
    return summaries


def format_float(value: Optional[float], decimals: int = 3) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def render_performance_table(profile_summaries: List[Dict[str, object]]) -> str:
    headers = [
        "Broker",
        "Config",
        "Partitions",
        "Warmup Storage msg/s",
        "Burst Storage msg/s",
        "p95 ms",
        "CPU %",
        "RAM MB",
        "Peak Backlog",
        "Recovery to Zero s",
        "Peak Lag",
        "Loss %",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    def sort_key(item: Dict[str, object]) -> tuple:
        return (
            str(item["broker"]),
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
                    str(summary.get("topic_partitions") or "-"),
                    format_float(summary.get("warmup_storage_throughput_msg_s_med")),
                    format_float(summary.get("burst_storage_throughput_msg_s_med")),
                    format_float(summary.get("p95_latency_ms_med")),
                    format_float(summary.get("cpu_pct_med")),
                    format_float(summary.get("ram_mb_med")),
                    format_float(summary.get("peak_pipeline_backlog_messages_med")),
                    format_float(summary.get("recovery_sec_to_backlog_zero_med")),
                    format_float(summary.get("peak_consumer_lag_med")),
                    format_float(summary.get("loss_pct_med")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def render_analysis(profile_summaries: List[Dict[str, object]], results_file: Path) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Scenario C Analysis",
        "",
        f"Generated from `{results_file.name}` on {generated_at}.",
        "",
        "This document summarizes the executed burst-load runs and is intended to feed the written report.",
        "",
    ]

    mqtt_profiles = [item for item in profile_summaries if item["broker"] == "mqtt"]
    kafka_profiles = [item for item in profile_summaries if item["broker"] == "kafka"]

    if mqtt_profiles:
        lines.append("## MQTT")
        lines.append("")
        for qos in (0, 1, 2):
            bucket = [item for item in mqtt_profiles if item.get("qos") == qos]
            if not bucket:
                continue
            avg_backlog = median_or_none([item.get("peak_pipeline_backlog_messages_med") for item in bucket])
            avg_p95 = median_or_none([item.get("p95_latency_ms_med") for item in bucket])
            avg_recovery = median_or_none([item.get("recovery_sec_to_backlog_zero_med") for item in bucket])
            avg_cpu = median_or_none([item.get("cpu_pct_med") for item in bucket])
            lines.append(
                f"- QoS `{qos}`: median peak backlog `{format_float(avg_backlog)}` messages, "
                f"median p95 `{format_float(avg_p95)}` ms, median recovery `{format_float(avg_recovery)}` s, "
                f"median CPU `{format_float(avg_cpu)}`%."
            )
        lines.append(
            "- Interpretation: higher MQTT QoS values should reduce delivery risk during the burst, but they tend to increase latency, overrun pressure and recovery cost."
        )
        lines.append("")

    if kafka_profiles:
        lines.append("## Kafka")
        lines.append("")
        for partitions in (1, 4, 8):
            bucket = [item for item in kafka_profiles if int(item.get("topic_partitions") or 0) == partitions]
            if not bucket:
                continue
            avg_burst = median_or_none([item.get("burst_storage_throughput_msg_s_med") for item in bucket])
            avg_lag = median_or_none([item.get("peak_consumer_lag_med") for item in bucket])
            avg_ram = median_or_none([item.get("ram_mb_med") for item in bucket])
            lines.append(
                f"- Partitions `{partitions}`: median burst storage throughput `{format_float(avg_burst)}` msg/s, "
                f"median peak lag `{format_float(avg_lag)}` messages, median RAM `{format_float(avg_ram)}` MB."
            )
        lines.append(
            "- Interpretation: Kafka exposes burst pressure directly through consumer lag and offset drift, while partitions trade memory/CPU for parallelism and smoother backlog drainage."
        )
        lines.append("")

    if mqtt_profiles and kafka_profiles:
        mqtt_cpu = median_or_none([item.get("cpu_pct_med") for item in mqtt_profiles])
        kafka_cpu = median_or_none([item.get("cpu_pct_med") for item in kafka_profiles])
        mqtt_ram = median_or_none([item.get("ram_mb_med") for item in mqtt_profiles])
        kafka_ram = median_or_none([item.get("ram_mb_med") for item in kafka_profiles])
        mqtt_p95 = median_or_none([item.get("p95_latency_ms_med") for item in mqtt_profiles])
        kafka_p95 = median_or_none([item.get("p95_latency_ms_med") for item in kafka_profiles])
        lines.append("## MQTT vs Kafka")
        lines.append("")
        lines.append(
            f"- MQTT median broker footprint: `{format_float(mqtt_cpu)}`% CPU / `{format_float(mqtt_ram)}` MB RAM; "
            f"Kafka median broker footprint: `{format_float(kafka_cpu)}`% CPU / `{format_float(kafka_ram)}` MB RAM."
        )
        lines.append(
            f"- MQTT median p95 latency across executed burst runs: `{format_float(mqtt_p95)}` ms; "
            f"Kafka median p95 latency: `{format_float(kafka_p95)}` ms."
        )
        lines.append(
            "- MQTT remains the lighter option for edge publication, while Kafka gives stronger backlog observability and partition scaling at a noticeably higher resource cost."
        )
        lines.append("")

    lines.append("## Report Implications")
    lines.append("")
    lines.append(
        "- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter."
    )
    lines.append(
        "- MQTT edge suitability can be argued from its smaller footprint and simpler broker stack, while its burst behavior becomes less attractive when we need replayable, lag-aware historical analytics."
    )
    lines.append(
        "- Kafka cloud suitability can be argued from its lag visibility, partition scaling and backlog control, while the price is substantially higher CPU/RAM usage."
    )
    lines.append("")
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


def build_profiles(args: argparse.Namespace) -> List[ScenarioCProfile]:
    profiles: List[ScenarioCProfile] = []
    for repeat_index in range(1, args.repeats + 1):
        if args.broker in {"mqtt", "both"}:
            for qos in args.mqtt_qos:
                profiles.append(
                    ScenarioCProfile(
                        broker="mqtt",
                        qos=int(qos),
                        warmup_rate=args.warmup_rate,
                        burst_rate=args.burst_rate,
                        warmup_sec=args.warmup_sec,
                        burst_sec=args.burst_sec,
                        recovery_sec=args.recovery_sec,
                        repeat_index=repeat_index,
                    )
                )
        if args.broker in {"kafka", "both"}:
            for acks in args.kafka_acks:
                for partitions in args.kafka_partitions:
                    profiles.append(
                        ScenarioCProfile(
                            broker="kafka",
                            acks=str(acks),
                            topic_partitions=int(partitions),
                            warmup_rate=args.warmup_rate,
                            burst_rate=args.burst_rate,
                            warmup_sec=args.warmup_sec,
                            burst_sec=args.burst_sec,
                            recovery_sec=args.recovery_sec,
                            repeat_index=repeat_index,
                        )
                    )
    return profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runs dedicated Scenario C burst-load benchmark.")
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--mqtt-qos", nargs="+", default=["0", "1", "2"])
    parser.add_argument("--kafka-acks", nargs="+", default=["0", "1", "all"])
    parser.add_argument("--kafka-partitions", nargs="+", default=["1", "4", "8"])
    parser.add_argument("--warmup-rate", type=int, default=50)
    parser.add_argument("--burst-rate", type=int, default=5000)
    parser.add_argument("--warmup-sec", type=int, default=20)
    parser.add_argument("--burst-sec", type=int, default=5)
    parser.add_argument("--recovery-sec", type=int, default=20)
    parser.add_argument("--drain-timeout-sec", type=int, default=180)
    parser.add_argument("--sample-interval-sec", type=float, default=1.0)
    parser.add_argument("--kafka-lag-sample-interval-sec", type=float, default=3.0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--db-write-enabled", action="store_true")
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--results-file", default=str(RESULTS_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results_file = Path(args.results_file)
    results_file.parent.mkdir(parents=True, exist_ok=True)
    payload_tmp: tempfile.TemporaryDirectory = common.build_payload_dir()

    aggregated_results = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scenario": "C",
        "description": "Burst Event Load benchmark with dedicated tools, backlog tracking and recovery metrics.",
        "tests": [],
        "profile_summaries": [],
        "summary": {},
        "artifacts": {},
    }

    profiles = build_profiles(args)
    try:
        for index, profile in enumerate(profiles, start=1):
            print("\n" + "=" * 80)
            print(f"[{index}/{len(profiles)}] Running {profile.run_name}")
            print("=" * 80)
            try:
                result = execute_profile(
                    profile=profile,
                    payload_dir=Path(payload_tmp.name),
                    disable_db_write=not args.db_write_enabled,
                    build_images=args.build_images and index == 1,
                    sample_interval_sec=args.sample_interval_sec,
                    kafka_lag_sample_interval_sec=args.kafka_lag_sample_interval_sec,
                    drain_timeout_sec=args.drain_timeout_sec,
                )
                aggregated_results["tests"].append(result)
                print(
                    f"Completed {profile.run_name}: sent={result['messages_sent']}, "
                    f"received={result['messages_received']}, "
                    f"peak_backlog={result['peak_pipeline_backlog_messages']}, "
                    f"recovery={result['recovery_sec_to_backlog_zero']}"
                )
            except Exception as exc:
                failure = {
                    "scenario": "C",
                    "config_name": profile.config_name,
                    "run_name": profile.run_name,
                    "repeat_index": profile.repeat_index,
                    "broker": profile.broker,
                    "broker_value": profile.broker_value,
                    "qos": profile.qos if profile.broker == "mqtt" else None,
                    "acks": profile.acks if profile.broker == "kafka" else None,
                    "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
                    "error": str(exc),
                    "validation_issues": ["benchmark_execution_failed"],
                }
                aggregated_results["tests"].append(failure)
                print(f"FAILED {profile.run_name}: {exc}", file=sys.stderr)

            aggregated_results["profile_summaries"] = build_profile_summaries(aggregated_results["tests"])
            aggregated_results["summary"] = summarize_results(aggregated_results["tests"])
            aggregated_results["artifacts"] = write_supporting_artifacts(results_file, aggregated_results)
            results_file.write_text(json.dumps(aggregated_results, indent=2), encoding="utf-8")
    finally:
        payload_tmp.cleanup()

    print("\n" + "=" * 80)
    print(f"Scenario C benchmark completed. Results saved to {results_file}")
    print(f"Performance table: {aggregated_results['artifacts'].get('performance_table')}")
    print(f"Analysis report: {aggregated_results['artifacts'].get('analysis_report')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
