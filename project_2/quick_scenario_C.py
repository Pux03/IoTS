"""
quick_scenario_C.py

Fast, configurable, SINGLE-RUN Scenario C sanity check.

Scenario C = Burst Event Load:
baseline rate -> burst rate -> recovery window.

Goal: give a short CLI-only burst benchmark for MQTT and/or Kafka without
generating JSON/Markdown artifacts and without restarting the full stack for
every profile.

By default, the script follows the project requirement for Scenario C:
it models a jump from 50 msg/s to 5000 msg/s for a few seconds. That makes
the default run more representative, but also heavier than a tiny smoke test.

Examples:
    python quick_scenario_C.py
    python quick_scenario_C.py --broker mqtt --mqtt-qos 0 1 2
    python quick_scenario_C.py --broker kafka --kafka-acks 1 all --kafka-partitions 4
    python quick_scenario_C.py --warmup-rate 20 --burst-rate 500 --warmup-sec 2 --burst-sec 2 --recovery-sec 2
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent
BENCHMARKS_DIR = REPO_ROOT / "benchmarks"
if str(BENCHMARKS_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARKS_DIR))

import run_scenario_c as full_runner  # noqa: E402


CORE_SERVICES = ["db", "mqtt-broker", "kafka-broker", "resource-monitor"]
CONSUMER_SERVICES = ["data-storage", "analytics-service"]

DEFAULT_MQTT_QOS = [0, 1]
DEFAULT_KAFKA_ACKS = ["1"]
DEFAULT_KAFKA_PARTITIONS = 1
DEFAULT_WARMUP_RATE = 50
DEFAULT_BURST_RATE = 5000
DEFAULT_WARMUP_SEC = 2
DEFAULT_BURST_SEC = 3
DEFAULT_RECOVERY_SEC = 2
DEFAULT_SAMPLE_INTERVAL_SEC = 1.0
DEFAULT_KAFKA_LAG_SAMPLE_INTERVAL_SEC = 2.0
DEFAULT_DRAIN_TIMEOUT_SEC = 12
DEFAULT_MAX_WAIT_SEC = 15
DEFAULT_TIME_BUDGET_SEC = 90


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quick Scenario C sanity check. Runs exactly one iteration per "
            "selected profile and prints burst/backlog results to the CLI only."
        )
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument("--mqtt-qos", nargs="+", type=int, default=None)
    parser.add_argument("--kafka-acks", nargs="+", default=None)
    parser.add_argument("--kafka-partitions", type=int, default=DEFAULT_KAFKA_PARTITIONS)
    parser.add_argument("--warmup-rate", type=int, default=DEFAULT_WARMUP_RATE)
    parser.add_argument("--burst-rate", type=int, default=DEFAULT_BURST_RATE)
    parser.add_argument("--warmup-sec", type=int, default=DEFAULT_WARMUP_SEC)
    parser.add_argument("--burst-sec", type=int, default=DEFAULT_BURST_SEC)
    parser.add_argument("--recovery-sec", type=int, default=DEFAULT_RECOVERY_SEC)
    parser.add_argument("--sample-interval-sec", type=float, default=DEFAULT_SAMPLE_INTERVAL_SEC)
    parser.add_argument(
        "--kafka-lag-sample-interval-sec",
        type=float,
        default=DEFAULT_KAFKA_LAG_SAMPLE_INTERVAL_SEC,
    )
    parser.add_argument("--drain-timeout-sec", type=int, default=DEFAULT_DRAIN_TIMEOUT_SEC)
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

    original_wait_for_probe_receipts = full_runner.common.wait_for_probe_receipts
    original_wait_for_drain = full_runner.wait_for_drain
    original_finalize_mqtt_probe = full_runner.finalize_mqtt_probe
    original_read_publisher_progress = full_runner.read_publisher_progress

    def wait_for_probe_receipts(*, port, expected_messages, timeout_sec, stable_polls=3):
        return original_wait_for_probe_receipts(
            port=port,
            expected_messages=expected_messages,
            timeout_sec=min(timeout_sec, max_wait_sec),
            stable_polls=min(stable_polls, 2),
        )

    def wait_for_drain(
        *,
        profile,
        benchmark_started_at,
        publishers,
        sample_interval_sec,
        kafka_lag_sample_interval_sec,
        cached_kafka_lag,
        timeout_sec,
        stable_polls=3,
    ):
        return original_wait_for_drain(
            profile=profile,
            benchmark_started_at=benchmark_started_at,
            publishers=publishers,
            sample_interval_sec=sample_interval_sec,
            kafka_lag_sample_interval_sec=kafka_lag_sample_interval_sec,
            cached_kafka_lag=cached_kafka_lag,
            timeout_sec=min(timeout_sec, max_wait_sec),
            stable_polls=min(stable_polls, 2),
        )

    def finalize_mqtt_probe(probe, *, expected_messages, timeout_sec):
        return original_finalize_mqtt_probe(
            probe,
            expected_messages=expected_messages,
            timeout_sec=min(timeout_sec, max_wait_sec),
        )

    def wait_for_publishers_completion(profile, publishers):
        results: List[Dict[str, object]] = []
        for publisher in publishers:
            container_name = str(publisher["container_name"])
            target_rate = max(int(publisher["target_rate"]), 1)
            planned_messages = int(publisher["planned_messages"])
            expected_duration = max(5, int((planned_messages / target_rate) * 3) + 5)
            wait_timeout = min(expected_duration, max_wait_sec)
            completed_cleanly = False
            exit_code = 1

            try:
                wait_result = full_runner.common.run_cmd(
                    ["docker", "wait", container_name],
                    timeout=wait_timeout,
                    check=False,
                )
                exit_code = int(wait_result.stdout.strip() or "1")
                completed_cleanly = exit_code == 0
            except subprocess.TimeoutExpired:
                full_runner.common.run_cmd(["docker", "stop", container_name], timeout=30, check=False)
                exit_code = 124

            progress = original_read_publisher_progress(profile, publisher)
            result = {
                "kind": publisher["kind"],
                "container_name": container_name,
                "target_rate": publisher["target_rate"],
                "configured_rate": publisher["actual_configured_rate"],
                "planned_messages": publisher["planned_messages"],
                "completed_cleanly": completed_cleanly,
                "container_exit_code": exit_code,
                "tool_stdout": progress["logs_text"],
            }

            if profile.broker == "mqtt":
                result["messages_sent"] = int(progress["messages_sent"])
                result["publish_overrun_messages"] = int(progress["publish_overrun_messages"])
            else:
                result["messages_sent"] = int(progress["messages_sent"])
                result["publish_overrun_messages"] = 0
                result["tool_summary"] = progress["tool_summary"]

            results.append(result)

        return results

    full_runner.common.wait_for_probe_receipts = wait_for_probe_receipts
    full_runner.wait_for_drain = wait_for_drain
    full_runner.finalize_mqtt_probe = finalize_mqtt_probe
    full_runner.wait_for_publishers_completion = wait_for_publishers_completion


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
                ["docker", "compose", "build", *CORE_SERVICES, *CONSUMER_SERVICES],
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

    def ensure_stack(self, profile: "full_runner.ScenarioCProfile", *, build_images: bool) -> None:  # noqa: ARG002
        self.ensure_core_started()
        self.ensure_consumers(profile.broker, profile.qos, profile.topic_partitions or 1)

    def shutdown(self) -> None:
        full_runner.common.run_cmd(
            ["docker", "compose", "down", "--remove-orphans"],
            timeout=180,
            check=False,
        )


def build_profiles(args: argparse.Namespace) -> List["full_runner.ScenarioCProfile"]:
    mqtt_qos = args.mqtt_qos if args.mqtt_qos is not None else DEFAULT_MQTT_QOS
    kafka_acks = args.kafka_acks if args.kafka_acks is not None else DEFAULT_KAFKA_ACKS

    profiles: List[full_runner.ScenarioCProfile] = []
    if args.broker in {"mqtt", "both"}:
        for qos in mqtt_qos:
            profiles.append(
                full_runner.ScenarioCProfile(
                    broker="mqtt",
                    warmup_rate=args.warmup_rate,
                    burst_rate=args.burst_rate,
                    warmup_sec=args.warmup_sec,
                    burst_sec=args.burst_sec,
                    recovery_sec=args.recovery_sec,
                    repeat_index=1,
                    qos=int(qos),
                )
            )
    if args.broker in {"kafka", "both"}:
        for acks in kafka_acks:
            profiles.append(
                full_runner.ScenarioCProfile(
                    broker="kafka",
                    warmup_rate=args.warmup_rate,
                    burst_rate=args.burst_rate,
                    warmup_sec=args.warmup_sec,
                    burst_sec=args.burst_sec,
                    recovery_sec=args.recovery_sec,
                    repeat_index=1,
                    acks=str(acks),
                    topic_partitions=args.kafka_partitions,
                )
            )
    return profiles


def profile_config_text(broker: str, qos: object = None, acks: object = None, partitions: object = None) -> str:
    if broker == "mqtt":
        return f"qos={qos}"
    return f"acks={acks} p={partitions}"


def describe_profile(profile: "full_runner.ScenarioCProfile") -> str:
    return (
        f"{profile.broker} | "
        f"{profile_config_text(profile.broker, profile.qos, profile.acks, profile.topic_partitions)} | "
        f"{profile.warmup_rate}->{profile.burst_rate} msg/s | "
        f"warmup={profile.warmup_sec}s | burst={profile.burst_sec}s | recovery={profile.recovery_sec}s"
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


def display_recovery_sec(result: Dict[str, object]) -> Optional[float]:
    issues = {str(item) for item in (result.get("validation_issues") or [])}
    if "analytics_processed_exceeded_storage_received" in issues:
        return None

    peak_backlog = int(result.get("peak_pipeline_backlog_messages") or 0)
    peak_lag = int(result.get("peak_consumer_lag") or 0)
    recovery_to_baseline = result.get("recovery_sec_to_baseline_storage_throughput")
    recovery_to_backlog_zero = result.get("recovery_sec_to_backlog_zero")

    if recovery_to_baseline is not None:
        return float(recovery_to_baseline)

    if peak_backlog > 0 or peak_lag > 0:
        if recovery_to_backlog_zero is not None:
            return float(recovery_to_backlog_zero)

    return None


def recovery_note(result: Dict[str, object]) -> Optional[str]:
    issues = {str(item) for item in (result.get("validation_issues") or [])}
    broker = str(result.get("broker", "?"))
    config = profile_config_text(
        broker,
        qos=result.get("qos"),
        acks=result.get("acks"),
        partitions=result.get("topic_partitions"),
    )

    if "analytics_processed_exceeded_storage_received" in issues:
        return f"{broker} | {config}: recovery metric hidden because analytics/storage counters are inconsistent."

    peak_backlog = int(result.get("peak_pipeline_backlog_messages") or 0)
    peak_lag = int(result.get("peak_consumer_lag") or 0)
    recovery_to_baseline = result.get("recovery_sec_to_baseline_storage_throughput")
    recovery_to_backlog_zero = result.get("recovery_sec_to_backlog_zero")

    if recovery_to_baseline is None and recovery_to_backlog_zero == 0 and peak_backlog == 0 and peak_lag == 0:
        return (
            f"{broker} | {config}: no observable backlog/lag formed, so recovery is shown as '-' "
            "instead of a misleading 0.00s."
        )

    return None


def print_table(results: List[Dict[str, object]]) -> None:
    headers = (
        "Broker",
        "Config",
        "Sent",
        "Recv",
        "Loss %",
        "Warmup msg/s",
        "Burst msg/s",
        "Peak Backlog",
        "Peak Lag",
        "Peak Ovrrn",
        "Recover s",
        "p95 ms",
        "CPU %",
        "RAM MB",
        "Status",
    )

    rows: List[Tuple[str, ...]] = []
    error_notes: List[str] = []
    validation_notes: List[str] = []
    recovery_notes: List[str] = []

    for result in results:
        broker = str(result.get("broker", "?"))
        config = profile_config_text(
            broker,
            qos=result.get("qos"),
            acks=result.get("acks"),
            partitions=result.get("topic_partitions"),
        )
        if result.get("error"):
            rows.append((broker, config, *(["-"] * 12), "ERROR"))
            error_notes.append(f"{broker} | {config}: {result['error']}")
            continue

        issues = result.get("validation_issues") or []
        if issues:
            validation_notes.append(f"{broker} | {config}: {', '.join(str(issue) for issue in issues)}")
        note = recovery_note(result)
        if note:
            recovery_notes.append(note)

        rows.append(
            (
                broker,
                config,
                format_number(result.get("messages_sent"), 0),
                format_number(result.get("messages_received"), 0),
                format_number(result.get("loss_pct")),
                format_number((result.get("warmup_phase") or {}).get("storage_throughput_msg_s")),
                format_number((result.get("burst_phase") or {}).get("storage_throughput_msg_s")),
                format_number(result.get("peak_pipeline_backlog_messages"), 0),
                format_number(result.get("peak_consumer_lag"), 0),
                format_number(result.get("peak_mqtt_publish_overrun_messages"), 0),
                format_number(display_recovery_sec(result)),
                format_number(result.get("p95_latency_ms")),
                format_number(result.get("cpu_pct")),
                format_number(result.get("ram_mb")),
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
    print("Scenario C quick results")
    print("=" * len("Scenario C quick results"))
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

    if recovery_notes:
        print("Recovery notes:")
        for note in recovery_notes:
            print(f"- {note}")
        print()

    if error_notes:
        print("Errors:")
        for note in error_notes:
            print(f"- {note}")
        print()


def main() -> int:
    args = parse_args()
    patch_runner(args.verbose, args.max_wait_sec)

    profiles = build_profiles(args)
    if not profiles:
        print("No profiles selected, nothing to do.")
        return 1

    manager = StackManager(build_images=args.build_images)

    def patched_restart_stack(profile, *, disable_db_write, build_images):  # noqa: ARG001
        manager.ensure_stack(profile, build_images=build_images)

    full_runner.restart_stack = patched_restart_stack
    full_runner.cleanup_stack_residue()
    full_runner.cleanup_scenario_c_tool_containers()

    payload_tmp = full_runner.common.build_payload_dir()
    results: List[Dict[str, object]] = []
    run_started_at = time.time()

    try:
        total = len(profiles)
        for index, profile in enumerate(profiles, start=1):
            print(f"[{index}/{total}] {describe_profile(profile)}")
            try:
                result = full_runner.execute_profile(
                    profile=profile,
                    payload_dir=Path(payload_tmp.name),
                    disable_db_write=True,
                    build_images=False,
                    sample_interval_sec=args.sample_interval_sec,
                    kafka_lag_sample_interval_sec=args.kafka_lag_sample_interval_sec,
                    drain_timeout_sec=args.drain_timeout_sec,
                )
                results.append(result)
                print(
                    "  "
                    f"sent={result['messages_sent']} | "
                    f"recv={result['messages_received']} | "
                    f"peak_backlog={result['peak_pipeline_backlog_messages']} | "
                    f"recover={format_number(display_recovery_sec(result))}s | "
                    f"p95={format_number(result.get('p95_latency_ms'))}ms"
                )
            except Exception as exc:
                failure: Dict[str, object] = {"broker": profile.broker, "error": str(exc)}
                if profile.broker == "mqtt":
                    failure["qos"] = profile.qos
                else:
                    failure["acks"] = profile.acks
                    failure["topic_partitions"] = profile.topic_partitions
                results.append(failure)
                print(f"  FAILED: {exc}")
    finally:
        payload_tmp.cleanup()
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
            f"NOTE: exceeded the {args.time_budget_sec}s target. Reduce "
            "--warmup-rate/--burst-rate, the number of QoS/acks values, "
            "or shorten the warmup/burst/recovery windows."
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
