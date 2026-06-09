"""
Dedicated Scenario D runner.

Scenario D measures alert end-to-end latency from the moment a critical value
is generated until the analytics service emits the alert for the corresponding
tumbling window.

The runner:
- uses dedicated tools only (`emqtt-bench` for MQTT and
  `kafka-producer-perf-test.sh` for Kafka)
- executes MQTT QoS 0 / 1 / 2 and Kafka acks 0 / 1 / all
- supports two window placements:
  - `early`: publish right after a new tumbling window starts
  - `late`: publish shortly before the current tumbling window closes
- restarts the stack per test profile
- samples broker CPU / RAM / network usage
- captures Kafka consumer lag snapshots
- exports JSON plus Markdown artifacts ready for the report
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import run_scenario_b as common


RESULTS_PATH = Path(__file__).resolve().parent / "scenario_d_results.json"
COMPOSE_CORE_SERVICES = [
    "db",
    "mqtt-broker",
    "kafka-broker",
    "data-ingestion",
    "data-storage",
    "analytics-service",
    "resource-monitor",
]
TRANSIENT_STACK_ERROR_MARKERS = (
    "dependency failed to start: container kafka-broker is unhealthy",
    "dependency failed to start: container kafka-broker exited",
    "conflict. the container name",
    "name is already in use",
    "no such container",
)


@dataclass(frozen=True)
class ScenarioDProfile:
    broker: str
    window_mode: str
    repeat_index: int
    critical_count: int
    qos: Optional[int] = None
    acks: Optional[str] = None
    topic_partitions: Optional[int] = None

    @property
    def config_name(self) -> str:
        if self.broker == "mqtt":
            return f"mqtt_qos_{self.qos}_window_{self.window_mode}"
        return f"kafka_acks_{self.acks}_partitions_{self.topic_partitions}_window_{self.window_mode}"

    @property
    def run_name(self) -> str:
        return f"{self.config_name}_repeat_{self.repeat_index}"

    @property
    def broker_value(self) -> str:
        return str(self.qos) if self.broker == "mqtt" else str(self.acks)


def cleanup_scenario_d_tool_containers() -> None:
    result = common.run_cmd(["docker", "ps", "-aq", "--filter", "name=scenario-d-"], timeout=60, check=False)
    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    for container_id in container_ids:
        common.run_cmd(["docker", "rm", "-f", container_id], timeout=60, check=False)


def cleanup_stack_residue() -> None:
    common.cleanup_stack_residue()
    cleanup_scenario_d_tool_containers()


def is_transient_stack_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if common.is_transient_stack_error(exc):
        return True
    return any(marker in message for marker in TRANSIENT_STACK_ERROR_MARKERS)


def restart_stack(profile: ScenarioDProfile, *, disable_db_write: bool, build_images: bool) -> None:
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
    up_cmd.extend(COMPOSE_CORE_SERVICES)

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
            if attempt >= max_attempts or not is_transient_stack_error(exc):
                raise
            print(
                f"Transient stack startup issue on attempt {attempt}/{max_attempts}: {exc}",
                file=sys.stderr,
            )
            cleanup_stack_residue()
            time.sleep(5)


def percentile(values: Iterable[float], rank_pct: float) -> Optional[float]:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, math.ceil((rank_pct / 100.0) * len(ordered)) - 1)
    return float(ordered[index])


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


def to_iso8601(ms_since_epoch: int) -> str:
    return datetime.fromtimestamp(ms_since_epoch / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_payload_dir(
    profile: ScenarioDProfile,
    *,
    run_id: str,
    planned_send_at_ms: int,
) -> tempfile.TemporaryDirectory:
    temp_dir = tempfile.TemporaryDirectory(prefix="scenario-d-bench-")
    payload_root = Path(temp_dir.name)

    mqtt_template = {
        "event_id": f"{run_id}-%UNIQUE%",
        "timestamp": to_iso8601(planned_send_at_ms),
        "run_id": run_id,
        "scenario": "scenario_d",
        "window_mode": profile.window_mode,
        "critical": True,
        "benchmark_sent_at_ms": "%TIMESTAMPMS%",
        "planned_send_at_ms": planned_send_at_ms,
        "device_id": "D-ALERT-000",
        "card_uid": "AA:BB:CC:DD",
        "access_granted": True,
        "door_id": "SERVER_ROOM",
        "zone": "SECOND_FLOOR",
        "signal_strength": -47,
        "battery_voltage": 3.96,
        "response_time_ms": 14,
        "event_type": "ENTRY",
        "temperature": 64.5,
    }
    (payload_root / "mqtt_alert_payload_template.json").write_text(
        json.dumps(mqtt_template, separators=(",", ":")),
        encoding="utf-8",
    )

    kafka_lines: List[str] = []
    for index in range(profile.critical_count):
        kafka_lines.append(
            json.dumps(
                {
                    "event_id": f"{run_id}-{index}",
                    "timestamp": to_iso8601(planned_send_at_ms),
                    "run_id": run_id,
                    "scenario": "scenario_d",
                    "window_mode": profile.window_mode,
                    "critical": True,
                    "benchmark_sent_at_ms": planned_send_at_ms,
                    "planned_send_at_ms": planned_send_at_ms,
                    "device_id": f"D-ALERT-{index:03d}",
                    "card_uid": f"AA:BB:CC:{index:02d}",
                    "access_granted": True,
                    "door_id": "SERVER_ROOM",
                    "zone": "SECOND_FLOOR",
                    "signal_strength": -45,
                    "battery_voltage": 3.99,
                    "response_time_ms": 12 + index,
                    "event_type": "ENTRY",
                    "temperature": 64.5 + (index * 0.1),
                },
                separators=(",", ":"),
            )
        )
    (payload_root / "kafka_alert_payloads.txt").write_text("\n".join(kafka_lines), encoding="utf-8")
    return temp_dir


def wait_for_window_state(timeout_sec: int = 60) -> Dict[str, object]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        state = common.try_fetch_json(f"{common.ANALYTICS_URL}/window-state", timeout=5)
        if state and "next_window_flush_at_ms" in state:
            return state
        time.sleep(1)
    raise TimeoutError("Analytics window-state endpoint did not become available in time.")


def compute_publish_schedule(
    *,
    window_state: Dict[str, object],
    window_mode: str,
    early_after_flush_ms: int,
    late_before_flush_ms: int,
    min_launch_lead_ms: int,
) -> Dict[str, int]:
    now_ms = int(window_state["now_ms"])
    next_flush_at_ms = int(window_state["next_window_flush_at_ms"])
    window_duration_ms = int(window_state["window_duration_ms"])

    if window_mode == "late":
        planned_send_at_ms = next_flush_at_ms - late_before_flush_ms
    else:
        planned_send_at_ms = next_flush_at_ms + early_after_flush_ms

    while planned_send_at_ms - now_ms < min_launch_lead_ms:
        planned_send_at_ms += window_duration_ms

    return {
        "planned_send_at_ms": planned_send_at_ms,
        "window_duration_ms": window_duration_ms,
        "window_reference_flush_at_ms": next_flush_at_ms,
        "launch_lead_ms": planned_send_at_ms - now_ms,
    }


def start_mqtt_alert_publisher(
    *,
    profile: ScenarioDProfile,
    payload_dir: Path,
    planned_send_at_ms: int,
) -> Dict[str, object]:
    now_ms = int(time.time() * 1000)
    sleep_seconds = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
    container_name = f"scenario-d-mqtt-alert-{profile.qos}-{profile.window_mode}-{profile.repeat_index}-{now_ms}"

    shell_command = (
        f"sleep {sleep_seconds:.3f}; "
        "exec /emqtt_bench/bin/emqtt_bench pub "
        "-A true "
        "-h mqtt-broker "
        "-p 1883 "
        "-V 4 "
        "-c 1 "
        "-R 1 "
        "-I 20 "
        f"-t {common.MQTT_TOPIC} "
        f"-q {profile.qos or 0} "
        f"-L {profile.critical_count} "
        "-w true "
        "-m template:///payloads/mqtt_alert_payload_template.json "
        "--log_to null"
    )
    if (profile.qos or 0) > 0:
        shell_command += " -F 20"

    common.run_cmd(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--network",
            "iot_network",
            "--entrypoint",
            "sh",
            "-v",
            f"{payload_dir.resolve()}:/payloads:ro",
            common.MQTT_TOOL_IMAGE,
            "-lc",
            shell_command,
        ],
        timeout=120,
    )
    return {
        "container_name": container_name,
        "planned_messages": profile.critical_count,
        "planned_send_at_ms": planned_send_at_ms,
        "scheduled_sleep_sec": round(sleep_seconds, 3),
    }


def start_kafka_alert_publisher(
    *,
    profile: ScenarioDProfile,
    payload_dir: Path,
    planned_send_at_ms: int,
) -> Dict[str, object]:
    now_ms = int(time.time() * 1000)
    sleep_seconds = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
    container_name = (
        f"scenario-d-kafka-alert-{profile.acks}-{profile.topic_partitions}-"
        f"{profile.window_mode}-{profile.repeat_index}-{now_ms}"
    )

    shell_command = (
        f"sleep {sleep_seconds:.3f}; "
        "exec /opt/kafka/bin/kafka-producer-perf-test.sh "
        f"--topic {common.KAFKA_TOPIC} "
        f"--num-records {profile.critical_count} "
        "--throughput 1000 "
        "--payload-file /payloads/kafka_alert_payloads.txt "
        "--producer-props "
        "bootstrap.servers=kafka-broker:29092 "
        f"acks={profile.acks} "
        "linger.ms=0 "
        "batch.size=16384 "
        "retries=2147483647 "
        "delivery.timeout.ms=180000 "
        "request.timeout.ms=30000 "
        "max.block.ms=180000 "
        "reconnect.backoff.ms=1000 "
        "reconnect.backoff.max.ms=5000"
    )

    common.run_cmd(
        [
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
            "sh",
            "-lc",
            shell_command,
        ],
        timeout=120,
    )
    return {
        "container_name": container_name,
        "planned_messages": profile.critical_count,
        "planned_send_at_ms": planned_send_at_ms,
        "scheduled_sleep_sec": round(sleep_seconds, 3),
    }


def wait_for_publisher_completion(
    profile: ScenarioDProfile,
    publisher: Dict[str, object],
) -> Dict[str, object]:
    container_name = str(publisher["container_name"])
    planned_messages = int(publisher["planned_messages"])
    scheduled_sleep_sec = float(publisher["scheduled_sleep_sec"])
    timeout_sec = max(120, int(scheduled_sleep_sec) + 90)

    wait_result = common.run_cmd(["docker", "wait", container_name], timeout=timeout_sec, check=False)
    exit_code = int(wait_result.stdout.strip() or "1")
    logs_text = common.docker_logs(container_name)

    result = {
        "container_name": container_name,
        "planned_messages": planned_messages,
        "planned_send_at_ms": publisher["planned_send_at_ms"],
        "scheduled_sleep_sec": scheduled_sleep_sec,
        "completed_cleanly": exit_code == 0,
        "container_exit_code": exit_code,
        "tool_stdout": logs_text,
    }

    if profile.broker == "mqtt":
        sent_messages = common.parse_mqtt_progress_records(logs_text)
        if sent_messages <= 0 and exit_code == 0:
            sent_messages = planned_messages
        result["messages_sent"] = sent_messages
    else:
        summary = common.parse_kafka_summary(logs_text)
        sent_messages = int(summary.get("records") or 0)
        if sent_messages <= 0 and exit_code == 0:
            sent_messages = planned_messages
        result["messages_sent"] = sent_messages
        result["tool_summary"] = summary
    return result


def wait_for_alert(
    *,
    run_id: str,
    window_mode: str,
    timeout_sec: int = 45,
) -> Optional[Dict[str, object]]:
    deadline = time.time() + timeout_sec
    query = urllib.parse.urlencode({"run_id": run_id, "window_mode": window_mode})
    while time.time() < deadline:
        response = common.try_fetch_json(f"{common.ANALYTICS_URL}/alerts/latest?{query}", timeout=5)
        if response and response.get("found") and response.get("alert"):
            return response["alert"]
        time.sleep(1)
    return None


def collect_service_counters() -> Dict[str, int]:
    storage_metrics = common.fetch_text(f"{common.STORAGE_URL}/metrics")
    analytics_metrics = common.fetch_text(f"{common.ANALYTICS_URL}/metrics")
    return {
        "storage_received_messages": int(
            common.parse_prometheus_counter(storage_metrics, "storage_messages_received_total")
        ),
        "analytics_processed_messages": int(
            common.parse_prometheus_counter(analytics_metrics, "analytics_messages_processed_total")
        ),
        "storage_queue_depth": int(
            common.find_metric_value(storage_metrics, "storage_batch_queue_depth", {"broker_type": common.fetch_json(f"{common.STORAGE_URL}/health").get("broker_type", "")})
        ),
        "analytics_window_queue_depth": int(
            common.find_metric_value(analytics_metrics, "analytics_window_event_queue_depth", {"broker_type": common.fetch_json(f"{common.ANALYTICS_URL}/health").get("broker_type", "")})
        ),
    }


def collect_kafka_lag_groups_safe(retries: int = 5, delay_sec: float = 2.0) -> Dict[str, object]:
    last_error: Optional[Exception] = None
    for _ in range(retries):
        try:
            return {group_id: common.collect_kafka_consumer_lag(group_id) for group_id in common.KAFKA_LAG_GROUPS}
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(delay_sec)
    if last_error is not None:
        raise last_error
    return {}


def wait_for_kafka_lag_clear(timeout_sec: int = 60) -> Dict[str, object]:
    deadline = time.time() + timeout_sec
    last_snapshot = collect_kafka_lag_groups_safe()
    while time.time() < deadline:
        if all(int(group.get("lag", 0)) == 0 for group in last_snapshot.values()):
            return last_snapshot
        time.sleep(1)
        last_snapshot = collect_kafka_lag_groups_safe()
    return last_snapshot


def execute_profile(
    *,
    profile: ScenarioDProfile,
    disable_db_write: bool,
    build_images: bool,
    early_after_flush_ms: int,
    late_before_flush_ms: int,
    min_launch_lead_ms: int,
) -> Dict[str, object]:
    restart_stack(profile, disable_db_write=disable_db_write, build_images=build_images)
    resource_sampler = common.ResourceSampler(common.BROKER_CONTAINER_NAME[profile.broker])
    publisher: Optional[Dict[str, object]] = None
    payload_dir_handle: Optional[tempfile.TemporaryDirectory] = None
    kafka_lag_before = None
    kafka_lag_after_alert = None
    kafka_lag_final = None
    alert_record = None

    resource_sampler.start()
    try:
        if profile.broker == "kafka":
            kafka_lag_before = collect_kafka_lag_groups_safe()

        window_state = wait_for_window_state()
        schedule = compute_publish_schedule(
            window_state=window_state,
            window_mode=profile.window_mode,
            early_after_flush_ms=early_after_flush_ms,
            late_before_flush_ms=late_before_flush_ms,
            min_launch_lead_ms=min_launch_lead_ms,
        )

        payload_dir_handle = build_payload_dir(
            profile,
            run_id=profile.run_name,
            planned_send_at_ms=int(schedule["planned_send_at_ms"]),
        )
        payload_dir = Path(payload_dir_handle.name)

        if profile.broker == "mqtt":
            publisher = start_mqtt_alert_publisher(
                profile=profile,
                payload_dir=payload_dir,
                planned_send_at_ms=int(schedule["planned_send_at_ms"]),
            )
        else:
            publisher = start_kafka_alert_publisher(
                profile=profile,
                payload_dir=payload_dir,
                planned_send_at_ms=int(schedule["planned_send_at_ms"]),
            )

        publisher_result = wait_for_publisher_completion(profile, publisher)
        alert_record = wait_for_alert(run_id=profile.run_name, window_mode=profile.window_mode)
        if profile.broker == "kafka":
            kafka_lag_after_alert = collect_kafka_lag_groups_safe()
            kafka_lag_final = wait_for_kafka_lag_clear()

        resource_summary = resource_sampler.summary()
        service_counters = collect_service_counters()
        validation_issues: List[str] = []

        if not publisher_result["completed_cleanly"]:
          validation_issues.append("publisher_exit_code_nonzero")
        if alert_record is None:
          validation_issues.append("alert_not_observed")
        if (
            alert_record
            and alert_record.get("primary_run_id") not in (profile.run_name, None)
        ):
          validation_issues.append("alert_primary_run_id_mismatch")
        if alert_record and alert_record.get("window_mode") not in (profile.window_mode, "mixed"):
          validation_issues.append("alert_window_mode_mismatch")
        if (
            alert_record
            and isinstance(alert_record.get("alert_latency_first_ms"), (int, float))
            and isinstance(alert_record.get("alert_latency_last_ms"), (int, float))
            and float(alert_record["alert_latency_first_ms"]) < float(alert_record["alert_latency_last_ms"])
        ):
          validation_issues.append("alert_first_latency_less_than_last_latency")
        if service_counters["analytics_processed_messages"] > service_counters["storage_received_messages"]:
          validation_issues.append("analytics_processed_exceeded_storage_received")
        if profile.broker == "kafka" and kafka_lag_final:
          if any(int(group.get("lag", 0)) > 0 for group in kafka_lag_final.values()):
            validation_issues.append("kafka_consumer_lag_nonzero_after_settle")

        alert_latency_first_ms = alert_record.get("alert_latency_first_ms") if alert_record else None
        alert_latency_last_ms = alert_record.get("alert_latency_last_ms") if alert_record else None
        first_sent_at_ms = alert_record.get("first_critical_sent_at_ms") if alert_record else None
        last_sent_at_ms = alert_record.get("last_critical_sent_at_ms") if alert_record else None
        alert_emitted_at_ms = alert_record.get("alert_emitted_at_ms") if alert_record else None

        scheduled_to_first_send_ms = None
        if isinstance(first_sent_at_ms, (int, float)):
          scheduled_to_first_send_ms = round(float(first_sent_at_ms) - float(schedule["planned_send_at_ms"]), 3)

        scheduled_to_alert_ms = None
        if isinstance(alert_emitted_at_ms, (int, float)):
          scheduled_to_alert_ms = round(float(alert_emitted_at_ms) - float(schedule["planned_send_at_ms"]), 3)

        peak_consumer_lag = 0
        if profile.broker == "kafka":
          lag_snapshots = [snapshot for snapshot in (kafka_lag_before, kafka_lag_after_alert, kafka_lag_final) if snapshot]
          for snapshot in lag_snapshots:
            peak_consumer_lag = max(
              peak_consumer_lag,
              max((int(group.get("lag", 0)) for group in snapshot.values()), default=0),
            )

        return {
            "scenario": "D",
            "config_name": profile.config_name,
            "run_name": profile.run_name,
            "repeat_index": profile.repeat_index,
            "broker": profile.broker,
            "broker_value": profile.broker_value,
            "window_mode": profile.window_mode,
            "qos": profile.qos if profile.broker == "mqtt" else None,
            "acks": profile.acks if profile.broker == "kafka" else None,
            "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
            "critical_count": profile.critical_count,
            "planned_send_at_ms": schedule["planned_send_at_ms"],
            "window_reference_flush_at_ms": schedule["window_reference_flush_at_ms"],
            "launch_lead_ms": schedule["launch_lead_ms"],
            "publisher_result": publisher_result,
            "messages_sent": int(publisher_result.get("messages_sent", 0)),
            "alert_found": alert_record is not None,
            "alerts_emitted": 1 if alert_record else 0,
            "alert_latency_first_ms": alert_latency_first_ms,
            "alert_latency_last_ms": alert_latency_last_ms,
            "first_critical_sent_at_ms": first_sent_at_ms,
            "last_critical_sent_at_ms": last_sent_at_ms,
            "alert_emitted_at_ms": alert_emitted_at_ms,
            "scheduled_to_first_send_ms": scheduled_to_first_send_ms,
            "scheduled_to_alert_ms": scheduled_to_alert_ms,
            "cpu_pct": resource_summary["cpu_pct"],
            "cpu_pct_avg": resource_summary["cpu_pct_avg"],
            "ram_mb": resource_summary["ram_mb"],
            "ram_mb_avg": resource_summary["ram_mb_avg"],
            "network_mb": resource_summary["network_mb"],
            "network_rx_mb": resource_summary["network_rx_mb"],
            "network_tx_mb": resource_summary["network_tx_mb"],
            "resource_sample_count": resource_summary["sample_count"],
            "storage_received_messages": service_counters["storage_received_messages"],
            "analytics_processed_messages": service_counters["analytics_processed_messages"],
            "storage_queue_depth": service_counters["storage_queue_depth"],
            "analytics_window_queue_depth": service_counters["analytics_window_queue_depth"],
            "peak_consumer_lag": peak_consumer_lag,
            "kafka_consumer_lag_before": kafka_lag_before,
            "kafka_consumer_lag_after_alert": kafka_lag_after_alert,
            "kafka_consumer_lag_final": kafka_lag_final,
            "alert_record": alert_record,
            "validation_issues": validation_issues,
        }
    finally:
        resource_sampler.stop()
        if publisher:
            common.stop_container(str(publisher["container_name"]))
        if payload_dir_handle:
            payload_dir_handle.cleanup()


def summarize_results(tests: List[Dict[str, object]]) -> Dict[str, object]:
    failed = [item for item in tests if item.get("error")]
    completed = [item for item in tests if not item.get("error")]
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
        first_latencies = [float(item["alert_latency_first_ms"]) for item in group if item.get("alert_latency_first_ms") is not None]
        last_latencies = [float(item["alert_latency_last_ms"]) for item in group if item.get("alert_latency_last_ms") is not None]
        summary = {
            "config_name": config_name,
            "broker": first["broker"],
            "broker_value": first["broker_value"],
            "window_mode": first["window_mode"],
            "qos": first.get("qos"),
            "acks": first.get("acks"),
            "topic_partitions": first.get("topic_partitions"),
            "completed_repeats": len(group),
            "successful_alert_repeats": sum(1 for item in group if item.get("alert_found")),
            "alert_latency_first_avg_ms": round(average_or_none(first_latencies) or 0.0, 3) if first_latencies else None,
            "alert_latency_first_p95_ms": round(percentile(first_latencies, 95.0) or 0.0, 3) if first_latencies else None,
            "alert_latency_first_max_ms": round(max(first_latencies), 3) if first_latencies else None,
            "alert_latency_last_avg_ms": round(average_or_none(last_latencies) or 0.0, 3) if last_latencies else None,
            "alert_latency_last_p95_ms": round(percentile(last_latencies, 95.0) or 0.0, 3) if last_latencies else None,
            "alert_latency_last_max_ms": round(max(last_latencies), 3) if last_latencies else None,
            "cpu_pct_med": median_or_none(item.get("cpu_pct") for item in group),
            "ram_mb_med": median_or_none(item.get("ram_mb") for item in group),
            "network_mb_med": median_or_none(item.get("network_mb") for item in group),
            "peak_consumer_lag_med": median_or_none(item.get("peak_consumer_lag") for item in group),
            "validation_issue_runs": sum(1 for item in group if item.get("validation_issues")),
        }
        summaries.append(summary)
    return summaries


def render_performance_table(profile_summaries: List[Dict[str, object]]) -> str:
    headers = [
        "Broker",
        "Config",
        "Mode",
        "Partitions",
        "Alert Avg ms",
        "Alert p95 ms",
        "Alert Max ms",
        "CPU %",
        "RAM MB",
        "Network MB",
        "Peak Lag",
        "Successful Repeats",
    ]
    rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for summary in profile_summaries:
        rows.append(
            "| " + " | ".join(
                [
                    summary["broker"],
                    summary["config_name"],
                    str(summary["window_mode"]),
                    str(summary["topic_partitions"] if summary["topic_partitions"] is not None else "-"),
                    format_float(summary.get("alert_latency_first_avg_ms")),
                    format_float(summary.get("alert_latency_first_p95_ms")),
                    format_float(summary.get("alert_latency_first_max_ms")),
                    format_float(summary.get("cpu_pct_med")),
                    format_float(summary.get("ram_mb_med")),
                    format_float(summary.get("network_mb_med")),
                    format_float(summary.get("peak_consumer_lag_med")),
                    f"{summary.get('successful_alert_repeats', 0)}/{summary.get('completed_repeats', 0)}",
                ]
            ) + " |"
        )
    return "\n".join(rows) + "\n"


def render_analysis(profile_summaries: List[Dict[str, object]], results_file: Path) -> str:
    lines = [
        "# Scenario D Analysis",
        "",
        f"Generated from `{results_file.name}` on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.",
        "",
        "This document summarizes the alert-latency benchmark runs and is intended to feed the written report.",
        "",
    ]

    mqtt_summaries = [item for item in profile_summaries if item["broker"] == "mqtt"]
    kafka_summaries = [item for item in profile_summaries if item["broker"] == "kafka"]

    if mqtt_summaries:
        lines.extend(["## MQTT", ""])
        for summary in mqtt_summaries:
            lines.append(
                f"- `{summary['config_name']}`: alert avg `{format_float(summary.get('alert_latency_first_avg_ms'))}` ms, "
                f"p95 `{format_float(summary.get('alert_latency_first_p95_ms'))}` ms, "
                f"CPU `{format_float(summary.get('cpu_pct_med'))}`%, "
                f"RAM `{format_float(summary.get('ram_mb_med'))}` MB."
            )
        lines.append("")

    if kafka_summaries:
        lines.extend(["## Kafka", ""])
        for summary in kafka_summaries:
            lines.append(
                f"- `{summary['config_name']}`: alert avg `{format_float(summary.get('alert_latency_first_avg_ms'))}` ms, "
                f"p95 `{format_float(summary.get('alert_latency_first_p95_ms'))}` ms, "
                f"peak lag `{format_float(summary.get('peak_consumer_lag_med'))}` messages, "
                f"RAM `{format_float(summary.get('ram_mb_med'))}` MB."
            )
        lines.append("")

    if mqtt_summaries and kafka_summaries:
        mqtt_cpu = average_or_none(item.get("cpu_pct_med") for item in mqtt_summaries)
        kafka_cpu = average_or_none(item.get("cpu_pct_med") for item in kafka_summaries)
        mqtt_ram = average_or_none(item.get("ram_mb_med") for item in mqtt_summaries)
        kafka_ram = average_or_none(item.get("ram_mb_med") for item in kafka_summaries)
        mqtt_p95 = average_or_none(item.get("alert_latency_first_p95_ms") for item in mqtt_summaries)
        kafka_p95 = average_or_none(item.get("alert_latency_first_p95_ms") for item in kafka_summaries)

        lines.extend(
            [
                "## MQTT vs Kafka",
                "",
                f"- MQTT average alert-latency p95 across profile summaries: `{format_float(mqtt_p95)}` ms.",
                f"- Kafka average alert-latency p95 across profile summaries: `{format_float(kafka_p95)}` ms.",
                f"- MQTT median broker footprint trend: `{format_float(mqtt_cpu)}`% CPU / `{format_float(mqtt_ram)}` MB RAM.",
                f"- Kafka median broker footprint trend: `{format_float(kafka_cpu)}`% CPU / `{format_float(kafka_ram)}` MB RAM.",
                "",
                "## Report Implications",
                "",
                "- Use the performance table directly in the real-time alerting comparison chapter.",
                "- Use `early` vs `late` window placement to explain how the 10-second tumbling window dominates end-user alert delay.",
                "- Use Kafka lag snapshots to discuss observability and replay-oriented cloud-side processing.",
                "",
            ]
        )

    return "\n".join(lines)


def write_supporting_artifacts(
    *,
    results_file: Path,
    profile_summaries: List[Dict[str, object]],
) -> Dict[str, str]:
    performance_table_path = results_file.with_name(f"{results_file.stem}_performance_table.md")
    analysis_path = results_file.with_name(f"{results_file.stem}_analysis.md")

    performance_table_path.write_text(render_performance_table(profile_summaries), encoding="utf-8")
    analysis_path.write_text(render_analysis(profile_summaries, results_file), encoding="utf-8")

    return {
        "performance_table": str(performance_table_path.relative_to(common.REPO_ROOT)),
        "analysis_report": str(analysis_path.relative_to(common.REPO_ROOT)),
    }


def build_profiles(args: argparse.Namespace) -> List[ScenarioDProfile]:
    profiles: List[ScenarioDProfile] = []
    repeats = int(args.repeats)
    if repeats < 1 or repeats > 3:
        raise ValueError("--repeats must be between 1 and 3 for Scenario D.")

    for repeat_index in range(1, repeats + 1):
        if args.broker in ("mqtt", "both"):
            for window_mode in args.window_modes:
                for qos in args.mqtt_qos:
                    profiles.append(
                        ScenarioDProfile(
                            broker="mqtt",
                            qos=qos,
                            window_mode=window_mode,
                            repeat_index=repeat_index,
                            critical_count=args.critical_count,
                        )
                    )
        if args.broker in ("kafka", "both"):
            for window_mode in args.window_modes:
                for acks in args.kafka_acks:
                    for partitions in args.kafka_partitions:
                        profiles.append(
                            ScenarioDProfile(
                                broker="kafka",
                                acks=acks,
                                topic_partitions=partitions,
                                window_mode=window_mode,
                                repeat_index=repeat_index,
                                critical_count=args.critical_count,
                            )
                        )
    return profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Scenario D alert-latency benchmark.")
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--mqtt-qos", nargs="+", type=int, default=[0, 1, 2])
    parser.add_argument("--kafka-acks", nargs="+", default=["0", "1", "all"])
    parser.add_argument("--kafka-partitions", nargs="+", type=int, default=[1])
    parser.add_argument("--window-modes", nargs="+", choices=["early", "late"], default=["late", "early"])
    parser.add_argument("--critical-count", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--early-after-flush-ms", type=int, default=150)
    parser.add_argument("--late-before-flush-ms", type=int, default=1200)
    parser.add_argument("--min-launch-lead-ms", type=int, default=5000)
    parser.add_argument("--db-write-enabled", action="store_true")
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--results-file", type=Path, default=RESULTS_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        profiles = build_profiles(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    results_file = args.results_file if args.results_file.is_absolute() else (common.REPO_ROOT / args.results_file)
    results_file.parent.mkdir(parents=True, exist_ok=True)

    tests: List[Dict[str, object]] = []
    try:
        for index, profile in enumerate(profiles, start=1):
            print("\n" + "=" * 80)
            print(f"[{index}/{len(profiles)}] Running {profile.run_name}")
            print("=" * 80)
            try:
                result = execute_profile(
                    profile=profile,
                    disable_db_write=not args.db_write_enabled,
                    build_images=args.build_images,
                    early_after_flush_ms=args.early_after_flush_ms,
                    late_before_flush_ms=args.late_before_flush_ms,
                    min_launch_lead_ms=args.min_launch_lead_ms,
                )
                tests.append(result)
                print(
                    f"Completed {profile.run_name}: alert_found={result['alert_found']}, "
                    f"alert_latency_first_ms={result.get('alert_latency_first_ms')}, "
                    f"cpu_pct={result.get('cpu_pct')}"
                )
            except Exception as exc:  # noqa: BLE001
                tests.append(
                    {
                        "scenario": "D",
                        "config_name": profile.config_name,
                        "run_name": profile.run_name,
                        "repeat_index": profile.repeat_index,
                        "broker": profile.broker,
                        "broker_value": profile.broker_value,
                        "window_mode": profile.window_mode,
                        "qos": profile.qos if profile.broker == "mqtt" else None,
                        "acks": profile.acks if profile.broker == "kafka" else None,
                        "topic_partitions": profile.topic_partitions if profile.broker == "kafka" else None,
                        "critical_count": profile.critical_count,
                        "error": str(exc),
                    }
                )
                print(f"Failed {profile.run_name}: {exc}", file=sys.stderr)
    finally:
        common.run_cmd(["docker", "compose", "down", "--remove-orphans"], timeout=180, check=False)
        cleanup_stack_residue()

    profile_summaries = build_profile_summaries(tests)
    artifacts = write_supporting_artifacts(results_file=results_file, profile_summaries=profile_summaries)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario": "D",
        "description": "Dedicated real-time alert latency benchmark with window-aligned critical events.",
        "tests": tests,
        "profile_summaries": profile_summaries,
        "summary": summarize_results(tests),
        "artifacts": artifacts,
    }
    results_file.write_text(json.dumps(output, indent=2), encoding="utf-8")

    print("\n" + "=" * 80)
    print(f"Scenario D benchmark completed. Results saved to {results_file.relative_to(common.REPO_ROOT)}")
    print(f"Performance table: {artifacts['performance_table']}")
    print(f"Analysis report: {artifacts['analysis_report']}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
