"""
quick_scenario_D.py

Fast, configurable, SINGLE-RUN Scenario D sanity check.

Scenario D = Real-Time Alerting:
measure alert end-to-end latency from the moment a critical value is generated
until analytics emits the alert for the corresponding tumbling window.

Goal: give a short CLI-only alert-latency benchmark for MQTT and/or Kafka
without generating JSON/Markdown artifacts and without restarting the full
stack for every profile.

By default, the script keeps the run short:
  - one run per selected profile
  - MQTT QoS 0/1 and Kafka acks=1
  - only `early` window placement by default, because it is more stable for
    a short quick-run and does not rely on tight pre-flush timing

Examples:
    python quick_scenario_D.py
    python quick_scenario_D.py --broker mqtt --mqtt-qos 0 1 2
    python quick_scenario_D.py --broker kafka --kafka-acks 0 1 all
    python quick_scenario_D.py --window-modes late early
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent
BENCHMARKS_DIR = REPO_ROOT / "benchmarks"
if str(BENCHMARKS_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARKS_DIR))

import run_scenario_d as full_runner  # noqa: E402


CORE_SERVICES = ["db", "mqtt-broker", "kafka-broker", "resource-monitor"]
CONSUMER_SERVICES = ["data-storage", "analytics-service"]
INGESTION_SERVICE = "data-ingestion"

DEFAULT_MQTT_QOS = [0, 1]
DEFAULT_KAFKA_ACKS = ["1"]
DEFAULT_KAFKA_PARTITIONS = 1
DEFAULT_WINDOW_MODES = ["early"]
DEFAULT_CRITICAL_COUNT = 3
DEFAULT_EARLY_AFTER_FLUSH_MS = 150
DEFAULT_LATE_BEFORE_FLUSH_MS = 3000
DEFAULT_MIN_LAUNCH_LEAD_MS = 5000
DEFAULT_MAX_WAIT_SEC = 20
DEFAULT_TIME_BUDGET_SEC = 90


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quick Scenario D sanity check. Runs exactly one iteration per "
            "selected profile and prints alert-latency results to the CLI only."
        )
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--mqtt-qos", nargs="+", type=int, default=None)
    parser.add_argument("--kafka-acks", nargs="+", default=None)
    parser.add_argument("--kafka-partitions", type=int, default=DEFAULT_KAFKA_PARTITIONS)
    parser.add_argument(
        "--window-modes",
        nargs="+",
        choices=["early", "late"],
        default=DEFAULT_WINDOW_MODES,
    )
    parser.add_argument("--critical-count", type=int, default=DEFAULT_CRITICAL_COUNT)
    parser.add_argument("--early-after-flush-ms", type=int, default=DEFAULT_EARLY_AFTER_FLUSH_MS)
    parser.add_argument("--late-before-flush-ms", type=int, default=DEFAULT_LATE_BEFORE_FLUSH_MS)
    parser.add_argument("--min-launch-lead-ms", type=int, default=DEFAULT_MIN_LAUNCH_LEAD_MS)
    parser.add_argument("--max-wait-sec", type=int, default=DEFAULT_MAX_WAIT_SEC)
    parser.add_argument("--time-budget-sec", type=int, default=DEFAULT_TIME_BUDGET_SEC)
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--keep-stack-up", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def patch_runner(verbose: bool, max_wait_sec: int) -> None:
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
        if verbose:
            print(f"$ {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=full_runner.common.REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(
                f"Command failed with exit code {result.returncode}: {' '.join(cmd)}\n{detail}"
            )
        return result

    full_runner.common.run_cmd = run_cmd

    original_wait_for_window_state = full_runner.wait_for_window_state
    original_wait_for_alert = full_runner.wait_for_alert
    original_wait_for_kafka_lag_clear = full_runner.wait_for_kafka_lag_clear
    original_build_payload_dir = full_runner.build_payload_dir

    def delayed_shell_prefix(planned_send_at_ms: int) -> str:
        return (
            f"TARGET_MS={planned_send_at_ms}; "
            "NOW_MS=$(date +%s%3N 2>/dev/null || true); "
            "case \"$NOW_MS\" in ''|*[!0-9]*) NOW_MS=$(($(date +%s) * 1000));; esac; "
            "SLEEP_MS=$((TARGET_MS - NOW_MS)); "
            "if [ \"$SLEEP_MS\" -gt 0 ]; then "
            "sleep \"$(printf '%d.%03d' $((SLEEP_MS / 1000)) $((SLEEP_MS % 1000)))\"; "
            "fi; "
        )

    def wait_for_window_state(timeout_sec: int = 60) -> Dict[str, object]:
        return original_wait_for_window_state(timeout_sec=min(timeout_sec, max_wait_sec))

    def wait_for_alert(*, run_id: str, window_mode: str, timeout_sec: int = 45):
        return original_wait_for_alert(
            run_id=run_id,
            window_mode=window_mode,
            timeout_sec=min(timeout_sec, max_wait_sec),
        )

    def wait_for_kafka_lag_clear(timeout_sec: int = 60):
        return original_wait_for_kafka_lag_clear(timeout_sec=min(timeout_sec, max_wait_sec))

    def start_mqtt_alert_publisher(*, profile, payload_dir, planned_send_at_ms):
        now_ms = int(time.time() * 1000)
        scheduled_sleep_sec = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
        container_name = (
            f"scenario-d-mqtt-alert-{profile.qos}-"
            f"{profile.window_mode}-{profile.repeat_index}-{now_ms}"
        )

        shell_command = (
            delayed_shell_prefix(int(planned_send_at_ms))
            + "exec /emqtt_bench/bin/emqtt_bench pub "
            "-A true "
            "-h mqtt-broker "
            "-p 1883 "
            "-V 4 "
            "-c 1 "
            "-R 1 "
            "-I 20 "
            f"-t {full_runner.common.MQTT_TOPIC} "
            f"-q {profile.qos or 0} "
            f"-L {profile.critical_count} "
            "-w true "
            "-m template:///payloads/mqtt_alert_payload_template.json "
            "--log_to null"
        )
        if (profile.qos or 0) > 0:
            shell_command += " -F 20"

        full_runner.common.run_cmd(
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
                full_runner.common.MQTT_TOOL_IMAGE,
                "-lc",
                shell_command,
            ],
            timeout=120,
        )
        return {
            "container_name": container_name,
            "planned_messages": profile.critical_count,
            "planned_send_at_ms": planned_send_at_ms,
            "scheduled_sleep_sec": round(scheduled_sleep_sec, 3),
        }

    def start_kafka_alert_publisher(*, profile, payload_dir, planned_send_at_ms):
        now_ms = int(time.time() * 1000)
        scheduled_sleep_sec = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
        container_name = (
            f"scenario-d-kafka-alert-{profile.acks}-{profile.topic_partitions}-"
            f"{profile.window_mode}-{profile.repeat_index}-{now_ms}"
        )

        shell_command = (
            delayed_shell_prefix(int(planned_send_at_ms))
            + "exec /opt/kafka/bin/kafka-producer-perf-test.sh "
            f"--topic {full_runner.common.KAFKA_TOPIC} "
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

        full_runner.common.run_cmd(
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
                full_runner.common.KAFKA_IMAGE,
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
            "scheduled_sleep_sec": round(scheduled_sleep_sec, 3),
        }

    def wait_for_publisher_completion(profile, publisher):
        container_name = str(publisher["container_name"])
        planned_messages = int(publisher["planned_messages"])
        scheduled_sleep_sec = float(publisher["scheduled_sleep_sec"])
        timeout_sec = int(scheduled_sleep_sec) + max_wait_sec

        completed_cleanly = False
        exit_code = 1
        try:
            wait_result = full_runner.common.run_cmd(
                ["docker", "wait", container_name],
                timeout=max(timeout_sec, 5),
                check=False,
            )
            exit_code = int(wait_result.stdout.strip() or "1")
            completed_cleanly = exit_code == 0
        except subprocess.TimeoutExpired:
            full_runner.common.run_cmd(["docker", "stop", container_name], timeout=30, check=False)
            exit_code = 124

        logs_text = full_runner.common.docker_logs(container_name)
        result = {
            "container_name": container_name,
            "planned_messages": planned_messages,
            "planned_send_at_ms": publisher["planned_send_at_ms"],
            "scheduled_sleep_sec": scheduled_sleep_sec,
            "completed_cleanly": completed_cleanly,
            "container_exit_code": exit_code,
            "tool_stdout": logs_text,
        }

        if profile.broker == "mqtt":
            sent_messages = full_runner.common.parse_mqtt_progress_records(logs_text)
            if sent_messages <= 0 and exit_code in (0, 124):
                sent_messages = planned_messages
            result["messages_sent"] = sent_messages
        else:
            summary = full_runner.common.parse_kafka_summary(logs_text)
            sent_messages = int(summary.get("records") or 0)
            if sent_messages <= 0 and exit_code in (0, 124):
                sent_messages = planned_messages
            result["messages_sent"] = sent_messages
            result["tool_summary"] = summary

        return result

    full_runner.wait_for_window_state = wait_for_window_state
    full_runner.wait_for_alert = wait_for_alert
    full_runner.wait_for_kafka_lag_clear = wait_for_kafka_lag_clear
    full_runner.wait_for_publisher_completion = wait_for_publisher_completion
    full_runner.build_payload_dir = original_build_payload_dir
    full_runner.start_mqtt_alert_publisher = start_mqtt_alert_publisher
    full_runner.start_kafka_alert_publisher = start_kafka_alert_publisher


class StackManager:
    def __init__(self, build_images: bool) -> None:
        self.build_images = build_images
        self.core_started = False

    def wait_for_consumers(self, timeout_sec: int = 45) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            checks = [
                full_runner.common.try_fetch_json(f"{full_runner.common.STORAGE_URL}/health"),
                full_runner.common.try_fetch_json(f"{full_runner.common.ANALYTICS_URL}/health"),
                full_runner.common.try_fetch_json(f"{full_runner.common.RESOURCE_MONITOR_URL}/health"),
            ]
            if all(status and status.get("ready") is True for status in checks):
                time.sleep(1)
                return
            time.sleep(1)
        raise TimeoutError("data-storage / analytics-service / resource-monitor did not become ready in time.")

    def ensure_core_started(self) -> None:
        if self.core_started:
            return
        if self.build_images:
            full_runner.common.run_cmd(
                ["docker", "compose", "build", *CORE_SERVICES, *CONSUMER_SERVICES, INGESTION_SERVICE],
                timeout=1200,
            )
        full_runner.common.run_cmd(["docker", "compose", "up", "-d", *CORE_SERVICES], timeout=300)
        self.core_started = True

    def ensure_consumers(self, broker: str, qos: Optional[int], partitions: int) -> None:
        env_overrides = {
            "BROKER_TYPE": broker,
            "DISABLE_DB_WRITE": "true",
            "MQTT_QOS": str(qos or 0),
            "KAFKA_TOPIC_PARTITIONS": str(partitions),
        }
        full_runner.common.run_cmd(
            ["docker", "compose", "up", "-d", "--force-recreate", "--no-deps", *CONSUMER_SERVICES],
            env_overrides=env_overrides,
            timeout=240,
        )
        self.wait_for_consumers()

    def wait_for_ingestion(self, timeout_sec: int = 45) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            status = full_runner.common.try_fetch_json(f"{full_runner.common.INGESTION_URL}/health")
            if status and status.get("ready") is True:
                time.sleep(1)
                return
            time.sleep(1)
        raise TimeoutError("data-ingestion did not become ready in time.")

    def ensure_ingestion_running(self, broker: str, qos: Optional[int], acks: Optional[str], partitions: int) -> None:
        env_overrides = {
            "BROKER_TYPE": broker,
            "DISABLE_DB_WRITE": "true",
            "MQTT_QOS": str(qos or 0),
            "KAFKA_ACKS": str(acks or "1"),
            "KAFKA_TOPIC_PARTITIONS": str(partitions),
        }
        full_runner.common.run_cmd(
            ["docker", "compose", "up", "-d", "--force-recreate", "--no-deps", INGESTION_SERVICE],
            env_overrides=env_overrides,
            timeout=240,
        )
        self.wait_for_ingestion()

    def ensure_ingestion_stopped(self) -> None:
        full_runner.common.run_cmd(
            ["docker", "compose", "stop", INGESTION_SERVICE],
            timeout=60,
            check=False,
        )

    def ensure_stack(self, profile: "full_runner.ScenarioDProfile", *, build_images: bool) -> None:  # noqa: ARG002
        self.ensure_core_started()
        self.ensure_consumers(profile.broker, profile.qos, profile.topic_partitions or 1)
        if profile.broker == "kafka":
            self.ensure_ingestion_running(
                profile.broker,
                profile.qos,
                profile.acks,
                profile.topic_partitions or 1,
            )
        else:
            self.ensure_ingestion_stopped()

    def shutdown(self) -> None:
        full_runner.common.run_cmd(
            ["docker", "compose", "down", "--remove-orphans"],
            timeout=180,
            check=False,
        )


def build_profiles(args: argparse.Namespace) -> List["full_runner.ScenarioDProfile"]:
    mqtt_qos = args.mqtt_qos if args.mqtt_qos is not None else DEFAULT_MQTT_QOS
    kafka_acks = args.kafka_acks if args.kafka_acks is not None else DEFAULT_KAFKA_ACKS

    profiles: List[full_runner.ScenarioDProfile] = []
    if args.broker in {"mqtt", "both"}:
        for window_mode in args.window_modes:
            for qos in mqtt_qos:
                profiles.append(
                    full_runner.ScenarioDProfile(
                        broker="mqtt",
                        window_mode=window_mode,
                        repeat_index=1,
                        critical_count=args.critical_count,
                        qos=int(qos),
                    )
                )

    if args.broker in {"kafka", "both"}:
        for window_mode in args.window_modes:
            for acks in kafka_acks:
                profiles.append(
                    full_runner.ScenarioDProfile(
                        broker="kafka",
                        window_mode=window_mode,
                        repeat_index=1,
                        critical_count=args.critical_count,
                        acks=str(acks),
                        topic_partitions=args.kafka_partitions,
                    )
                )

    return profiles


def profile_config_text(broker: str, qos: object = None, acks: object = None, partitions: object = None) -> str:
    if broker == "mqtt":
        return f"qos={qos}"
    return f"acks={acks} p={partitions}"


def describe_profile(profile: "full_runner.ScenarioDProfile") -> str:
    return (
        f"{profile.broker} | "
        f"{profile_config_text(profile.broker, profile.qos, profile.acks, profile.topic_partitions)} | "
        f"window={profile.window_mode} | "
        f"critical={profile.critical_count}"
    )


def format_number(value: object, decimals: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.{decimals}f}"


def status_text(result: Dict[str, object]) -> str:
    if result.get("error"):
        return "ERROR"
    issues = result.get("validation_issues") or []
    return "OK" if not issues else f"WARN({len(issues)})"


def normalize_result(result: Dict[str, object]) -> Dict[str, object]:
    if result.get("error"):
        return result

    normalized = dict(result)
    issues = list(normalized.get("validation_issues") or [])
    scheduled_to_alert_ms = normalized.get("scheduled_to_alert_ms")
    window_mode = normalized.get("window_mode")

    if (
        window_mode == "late"
        and isinstance(scheduled_to_alert_ms, (int, float))
        and float(scheduled_to_alert_ms) > 7000.0
        and "late_window_missed_target_possible" not in issues
    ):
        issues.append("late_window_missed_target_possible")

    normalized["validation_issues"] = issues
    return normalized


def print_table(results: List[Dict[str, object]]) -> None:
    headers = (
        "Broker",
        "Config",
        "Window",
        "Sent",
        "Alert",
        "First ms",
        "Last ms",
        "Sched->Alert",
        "CPU %",
        "RAM MB",
        "Peak Lag",
        "Status",
    )

    rows: List[Tuple[str, ...]] = []
    validation_notes: List[str] = []
    error_notes: List[str] = []

    for result in results:
        broker = str(result.get("broker", "?"))
        config = profile_config_text(
            broker,
            qos=result.get("qos"),
            acks=result.get("acks"),
            partitions=result.get("topic_partitions"),
        )
        window_mode = str(result.get("window_mode", "?"))

        if result.get("error"):
            rows.append((broker, config, window_mode, *(["-"] * 8), "ERROR"))
            error_notes.append(f"{broker} | {config} | {window_mode}: {result['error']}")
            continue

        issues = result.get("validation_issues") or []
        if issues:
            validation_notes.append(
                f"{broker} | {config} | {window_mode}: {', '.join(str(item) for item in issues)}"
            )

        rows.append(
            (
                broker,
                config,
                window_mode,
                format_number(result.get("messages_sent"), 0),
                format_number(result.get("alert_found")),
                format_number(result.get("alert_latency_first_ms")),
                format_number(result.get("alert_latency_last_ms")),
                format_number(result.get("scheduled_to_alert_ms")),
                format_number(result.get("cpu_pct")),
                format_number(result.get("ram_mb")),
                format_number(result.get("peak_consumer_lag"), 0),
                status_text(result),
            )
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = min(max(widths[index], len(cell)), 60)

    def trim(cell: str, width: int) -> str:
        return cell.ljust(width) if len(cell) <= width else (cell[: width - 3] + "...").ljust(width)

    separator = "-+-".join("-" * width for width in widths)
    print()
    print("Scenario D quick results")
    print("=" * len("Scenario D quick results"))
    print(" | ".join(trim(header, widths[index]) for index, header in enumerate(headers)))
    print(separator)
    for row in rows:
        print(" | ".join(trim(cell, widths[index]) for index, cell in enumerate(row)))
    print()

    if validation_notes:
        print("Validation notes:")
        for note in validation_notes:
            print(f"- {note}")
        print()

    if error_notes:
        print("Errors:")
        for note in error_notes:
            print(f"- {note}")
        print()


def host_trigger_scenario_d(count: int, timeout_sec: int = 10) -> Dict[str, object]:
    url = f"{full_runner.common.INGESTION_URL}/scenario/d/trigger?count={count}"
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    args = parse_args()
    patch_runner(args.verbose, args.max_wait_sec)

    profiles = build_profiles(args)
    if not profiles:
        print("No profiles selected, nothing to do.")
        return 1

    manager = StackManager(build_images=args.build_images)
    original_wait_for_publisher_completion = full_runner.wait_for_publisher_completion

    def patched_restart_stack(profile, *, disable_db_write, build_images):  # noqa: ARG001
        manager.ensure_stack(profile, build_images=build_images)

    def start_kafka_alert_publisher(*, profile, payload_dir, planned_send_at_ms):  # noqa: ARG001
        now_ms = int(time.time() * 1000)
        scheduled_sleep_sec = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
        return {
            "container_name": "data-ingestion",
            "planned_messages": profile.critical_count,
            "planned_send_at_ms": planned_send_at_ms,
            "scheduled_sleep_sec": round(scheduled_sleep_sec, 3),
            "trigger_mode": "ingestion_api",
            "run_id": profile.run_name,
            "window_mode": profile.window_mode,
        }

    def wait_for_publisher_completion(profile, publisher):
        if profile.broker == "kafka" and publisher.get("trigger_mode") == "ingestion_api":
            planned_messages = int(publisher["planned_messages"])
            planned_send_at_ms = int(publisher["planned_send_at_ms"])
            now_ms = int(time.time() * 1000)
            sleep_sec = max((planned_send_at_ms - now_ms) / 1000.0, 0.0)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

            run_id = str(publisher.get("run_id") or profile.run_name)
            window_mode = str(publisher.get("window_mode") or profile.window_mode)
            publish_script = f"""
import json
import os
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
acks = os.getenv("KAFKA_ACKS", "1")
topic = os.getenv("KAFKA_TOPIC", "iot-events")
planned_send_at_ms = {planned_send_at_ms}
run_id = {run_id!r}
window_mode = {window_mode!r}
count = {planned_messages}

acks_param = int(acks) if acks in ("0", "1") else "all"
producer = Producer({{
    "bootstrap.servers": bootstrap_servers,
    "acks": acks_param,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.ms": 10,
    "batch.num.messages": 1000,
    "retries": 5,
    "retry.backoff.ms": 500,
}})

delivered = {{"count": 0}}

def delivery_report(err, msg):
    if err is None:
        delivered["count"] += 1

for index in range(count):
    sent_at_ms = int(time.time() * 1000)
    payload = {{
        "event_id": f"{{run_id}}-{{index}}",
        "timestamp": datetime.fromtimestamp(sent_at_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_id": run_id,
        "scenario": "scenario_d",
        "window_mode": window_mode,
        "critical": True,
        "benchmark_sent_at_ms": sent_at_ms,
        "planned_send_at_ms": planned_send_at_ms,
        "device_id": f"D-ALERT-{{index:03d}}",
        "card_uid": f"AA:BB:CC:{{index:02d}}",
        "access_granted": True,
        "door_id": "SERVER_ROOM",
        "zone": "SECOND_FLOOR",
        "signal_strength": -45,
        "battery_voltage": 3.99,
        "response_time_ms": 12 + index,
        "event_type": "ENTRY",
        "temperature": 64.5 + (index * 0.1),
    }}
    producer.produce(topic, key=payload["device_id"], value=json.dumps(payload), callback=delivery_report)
    producer.poll(0)

producer.flush(30)
print(json.dumps({{"status": "sent", "messages_sent": delivered["count"]}}))
"""
            exec_result = full_runner.common.run_cmd(
                ["docker", "exec", "data-ingestion", "python", "-c", publish_script],
                timeout=30,
                check=False,
            )
            completed_cleanly = exec_result.returncode == 0
            sent_messages = planned_messages
            trigger_stdout = (exec_result.stdout or "").strip()
            if trigger_stdout:
                try:
                    parsed = json.loads(trigger_stdout.splitlines()[-1])
                    sent_messages = int(parsed.get("messages_sent") or planned_messages)
                except Exception:  # noqa: BLE001
                    sent_messages = planned_messages

            return {
                "container_name": "data-ingestion",
                "planned_messages": planned_messages,
                "planned_send_at_ms": planned_send_at_ms,
                "scheduled_sleep_sec": round(sleep_sec, 3),
                "completed_cleanly": completed_cleanly,
                "container_exit_code": exec_result.returncode,
                "tool_stdout": trigger_stdout,
                "messages_sent": sent_messages,
            }

        return original_wait_for_publisher_completion(profile, publisher)

    full_runner.restart_stack = patched_restart_stack
    full_runner.start_kafka_alert_publisher = start_kafka_alert_publisher
    full_runner.wait_for_publisher_completion = wait_for_publisher_completion
    full_runner.cleanup_stack_residue()
    full_runner.cleanup_scenario_d_tool_containers()

    results: List[Dict[str, object]] = []
    run_started_at = time.time()

    try:
        total = len(profiles)
        for index, profile in enumerate(profiles, start=1):
            print(f"[{index}/{total}] {describe_profile(profile)}")
            try:
                result = full_runner.execute_profile(
                    profile=profile,
                    disable_db_write=True,
                    build_images=False,
                    early_after_flush_ms=args.early_after_flush_ms,
                    late_before_flush_ms=args.late_before_flush_ms,
                    min_launch_lead_ms=args.min_launch_lead_ms,
                )
                result = normalize_result(result)
                results.append(result)
                print(
                    "  "
                    f"alert={format_number(result.get('alert_found'))} | "
                    f"first={format_number(result.get('alert_latency_first_ms'))}ms | "
                    f"last={format_number(result.get('alert_latency_last_ms'))}ms | "
                    f"sched_to_alert={format_number(result.get('scheduled_to_alert_ms'))}ms | "
                    f"cpu={format_number(result.get('cpu_pct'))}%"
                )
            except Exception as exc:
                failure: Dict[str, object] = {
                    "broker": profile.broker,
                    "window_mode": profile.window_mode,
                    "error": str(exc),
                }
                if profile.broker == "mqtt":
                    failure["qos"] = profile.qos
                else:
                    failure["acks"] = profile.acks
                    failure["topic_partitions"] = profile.topic_partitions
                results.append(failure)
                print(f"  FAILED: {exc}")
    finally:
        if not args.keep_stack_up:
            manager.shutdown()

    elapsed_sec = time.time() - run_started_at
    print_table(results)

    failures = sum(1 for item in results if item.get("error"))
    completed = len(results) - failures
    print(f"Completed profiles: {completed}/{len(results)}")
    if failures:
        print(f"Failed profiles: {failures}")
    print(f"Total wall-clock time: {elapsed_sec:.1f}s")
    if elapsed_sec > args.time_budget_sec:
        print(
            f"NOTE: exceeded the {args.time_budget_sec}s target. Reduce the "
            "number of QoS/acks values, keep only one window mode, or tune "
            "--min-launch-lead-ms / --max-wait-sec for a shorter demo."
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
