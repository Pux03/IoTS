"""
quick_scenario_B.py

Fast, configurable, SINGLE-RUN Scenario B sanity check (Edge Connectivity
Failures: warmup -> network outage -> reconnect -> recovery).

Goal: a CLI results table in well under ~90 seconds by default, on a
machine where the project's Docker images are already built (run once
with --build-images, or `docker compose build` beforehand).

How it stays fast (vs. benchmarks/run_scenario_b.py, which does a full
`docker compose down` + `up` before every single profile):

  - Exactly ONE execution per selected profile. No repeats, no retries.
  - Brokers stay up across the whole run, but per-profile counters are
    still isolated: data-storage / analytics-service are recreated for
    every profile, and data-ingestion is recreated whenever
    `app_buffered` mode is active. This keeps the run much faster than a
    full stack restart while avoiding cumulative counters between
    profiles.
  - `tool_benchmark` is the default (and recommended) mode for quick
    runs: it doesn't need data-ingestion at all. `app_buffered` mode is
    still available via --modes, but costs an extra restart per profile
    and is slower, so it's opt-in here.
  - The mandatory 30s outage from the assignment is still simulated
    (`docker network disconnect`), but warmup/outage/post-reconnect
    windows default to short values for a quick sanity check. Pass
    --outage-sec 30 (etc.) to reproduce the exact spec timing, at the
    cost of a much longer run.
  - Wait/settle timeouts are hard-capped (see --max-wait-sec) so one
    flaky profile can't blow the whole time budget.

Nothing is written to disk; results are only printed as a CLI table,
plus the total wall-clock time at the end.

Examples:
    python quick_scenario_B.py
    python quick_scenario_B.py --broker mqtt --mqtt-qos 0 1 2
    python quick_scenario_B.py --broker kafka --kafka-acks 1 all
    python quick_scenario_B.py --modes tool_benchmark app_buffered --mqtt-qos 0
    python quick_scenario_B.py --outage-sec 30 --warmup-sec 5 --post-reconnect-run-sec 15
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import benchmarks.run_scenario_b as full_runner


REPO_ROOT = Path(__file__).resolve().parent

CORE_SERVICES = ["db", "mqtt-broker", "kafka-broker", "resource-monitor"]
CONSUMER_SERVICES = ["data-storage", "analytics-service"]
INGESTION_SERVICE = "data-ingestion"

DEFAULT_MODES = ["tool_benchmark"]
DEFAULT_MQTT_QOS = [0, 1]
DEFAULT_KAFKA_ACKS = ["1"]
DEFAULT_KAFKA_PARTITIONS = 1
DEFAULT_DEVICES = 20
DEFAULT_INTERVAL_SEC = 1.0
DEFAULT_WARMUP_SEC = 2
DEFAULT_OUTAGE_SEC = 5
DEFAULT_POST_RECONNECT_SEC = 3
DEFAULT_MAX_WAIT_SEC = 20
DEFAULT_TIME_BUDGET_SEC = 90


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quick Scenario B sanity check (edge outage/recovery). Runs "
            "exactly one iteration per selected profile and prints a "
            "results table to the CLI. Tuned to finish well under 90 "
            "seconds by default (short warmup/outage/recovery windows)."
        )
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument(
        "--modes", nargs="+", choices=["tool_benchmark", "app_buffered"],
        default=DEFAULT_MODES,
        help="tool_benchmark bypasses data-ingestion (fast, default). "
             "app_buffered uses data-ingestion as the simulator (slower, "
             "needs an extra restart per config change).",
    )
    parser.add_argument("--mqtt-qos", nargs="+", type=int, default=None)
    parser.add_argument("--kafka-acks", nargs="+", default=None)
    parser.add_argument("--kafka-partitions", type=int, default=DEFAULT_KAFKA_PARTITIONS)
    parser.add_argument("--devices", type=int, default=DEFAULT_DEVICES)
    parser.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC)
    parser.add_argument(
        "--warmup-sec", type=int, default=DEFAULT_WARMUP_SEC,
        help="Seconds of normal traffic before the outage (spec: 5).",
    )
    parser.add_argument(
        "--outage-sec", type=int, default=DEFAULT_OUTAGE_SEC,
        help="Seconds the simulator/publisher is disconnected from the "
             "network (spec: 30). Shortened by default for a quick check.",
    )
    parser.add_argument(
        "--post-reconnect-run-sec", type=int, default=DEFAULT_POST_RECONNECT_SEC,
        help="Seconds of traffic captured after reconnect (spec: 15).",
    )
    parser.add_argument(
        "--max-wait-sec", type=int, default=DEFAULT_MAX_WAIT_SEC,
        help="Hard cap on any single settle/probe/resume wait (protects "
             "the overall time budget).",
    )
    parser.add_argument(
        "--time-budget-sec", type=int, default=DEFAULT_TIME_BUDGET_SEC,
        help="Only used to print a warning if exceeded; does not abort the run.",
    )
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--keep-stack-up", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


# --------------------------------------------------------------------------
# Command execution / monkey-patches
# --------------------------------------------------------------------------

def patch_runner(verbose: bool, max_wait_sec: int) -> None:
    """Quiet down full_runner (unless --verbose) and cap its wait
    timeouts so one slow/flaky profile can't eat the whole time budget."""

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
            cwd=full_runner.REPO_ROOT,
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

    full_runner.run_cmd = run_cmd

    original_get_tool_sent_messages = full_runner.get_tool_sent_messages
    original_wait_for_tool_progress_resume = full_runner.wait_for_tool_progress_resume
    original_wait_for_pipeline_and_buffers_settle = full_runner.wait_for_pipeline_and_buffers_settle
    original_wait_for_probe_receipts = full_runner.wait_for_probe_receipts
    original_wait_for_ingestion_ready = full_runner.wait_for_ingestion_ready

    def get_tool_sent_messages(profile, publisher):
        count = int(original_get_tool_sent_messages(profile, publisher) or 0)
        if profile.broker == "mqtt" and count <= 0 and publisher.get("probe"):
            try:
                probe_received = int(
                    full_runner.read_probe_recv_count(int(publisher["probe"]["port"]))
                )
                planned = int(publisher.get("planned_messages") or 0)
                count = min(planned, probe_received) if planned > 0 else probe_received
            except Exception:  # noqa: BLE001
                pass

        last_known = int(publisher.get("last_known_sent") or 0)
        if count > last_known:
            publisher["last_known_sent"] = count
            return count
        return max(count, last_known)

    def wait_for_tool_progress_resume(profile, publisher, baseline_sent, timeout_sec=180):
        return original_wait_for_tool_progress_resume(
            profile,
            publisher,
            baseline_sent,
            timeout_sec=min(timeout_sec, max_wait_sec),
        )

    def wait_for_pipeline_and_buffers_settle(
        profile,
        *,
        publisher=None,
        timeout_sec=180,
        stable_polls=3,
    ):
        return original_wait_for_pipeline_and_buffers_settle(
            profile,
            publisher=publisher,
            timeout_sec=min(timeout_sec, max_wait_sec),
            stable_polls=min(stable_polls, 2),
        )

    def wait_for_probe_receipts(*, port, expected_messages, timeout_sec, stable_polls=3):
        return original_wait_for_probe_receipts(
            port=port,
            expected_messages=expected_messages,
            timeout_sec=min(timeout_sec, max_wait_sec),
            stable_polls=min(stable_polls, 2),
        )

    def wait_for_ingestion_ready(timeout_sec=120):
        return original_wait_for_ingestion_ready(timeout_sec=min(timeout_sec, max_wait_sec))

    def wait_for_tool_completion(profile, publisher):
        container_name = str(publisher["container_name"])
        wait_timeout = max(5, min(max_wait_sec, max(5, profile.planned_publish_window_sec * 2)))
        timed_out = False
        exit_code = 1

        try:
            wait_result = full_runner.run_cmd(
                ["docker", "wait", container_name],
                timeout=wait_timeout,
                check=False,
            )
            try:
                exit_code = int(wait_result.stdout.strip() or "1")
            except ValueError:
                exit_code = 1
        except subprocess.TimeoutExpired:
            timed_out = True
            logs_before_stop = full_runner.docker_logs(container_name)
            publisher["timed_out_logs"] = logs_before_stop
            full_runner.run_cmd(["docker", "stop", container_name], timeout=30, check=False)
            exit_code = 124

        logs_text = str(publisher.get("timed_out_logs") or full_runner.docker_logs(container_name))
        result: Dict[str, object] = {
            "messages_sent": 0,
            "tool_stdout": logs_text,
            "tool_stderr": "",
            "latency_source": publisher.get("latency_source"),
            "completed_cleanly": exit_code == 0 and not timed_out,
            "container_exit_code": exit_code,
        }

        try:
            if profile.broker == "mqtt":
                messages_sent = get_tool_sent_messages(profile, publisher)
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
                        timeout_sec=max_wait_sec,
                    )
                    probe_metrics_text = full_runner.fetch_text(
                        f"http://localhost:{int(probe['port'])}/metrics",
                        timeout=10,
                    )
                    latency_summary = full_runner.compute_histogram_latency_summary(
                        full_runner.parse_histogram(probe_metrics_text, "e2e_latency")
                    )

                if messages_sent <= 0 and int(probe_wait["received_messages"]) > 0:
                    planned = int(publisher.get("planned_messages") or 0)
                    messages_sent = (
                        min(planned, int(probe_wait["received_messages"]))
                        if planned > 0
                        else int(probe_wait["received_messages"])
                    )

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
                summary = full_runner.parse_kafka_summary(logs_text)
                messages_sent = int(summary.get("records") or get_tool_sent_messages(profile, publisher))
                result.update(
                    {
                        "messages_sent": messages_sent,
                        "tool_summary": summary,
                    }
                )
        finally:
            if profile.broker == "mqtt" and publisher.get("probe"):
                full_runner.stop_container(str(publisher["probe"]["container_name"]))

        return result

    full_runner.get_tool_sent_messages = get_tool_sent_messages
    full_runner.wait_for_tool_progress_resume = wait_for_tool_progress_resume
    full_runner.wait_for_pipeline_and_buffers_settle = wait_for_pipeline_and_buffers_settle
    full_runner.wait_for_probe_receipts = wait_for_probe_receipts
    full_runner.wait_for_ingestion_ready = wait_for_ingestion_ready
    full_runner.wait_for_tool_completion = wait_for_tool_completion


# --------------------------------------------------------------------------
# Stack lifecycle (restart only when actually necessary)
# --------------------------------------------------------------------------

class StackManager:
    def __init__(self, build_images: bool) -> None:
        self.build_images = build_images
        self.core_started = False
        self.ingestion_state: Optional[Tuple[str, Optional[int], Optional[str], int]] = None

    def wait_for_consumers(self, timeout_sec: int = 60) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            checks = [
                full_runner.try_fetch_json(f"{full_runner.STORAGE_URL}/health"),
                full_runner.try_fetch_json(f"{full_runner.ANALYTICS_URL}/health"),
                full_runner.try_fetch_json(f"{full_runner.RESOURCE_MONITOR_URL}/health"),
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
            full_runner.run_cmd(
                ["docker", "compose", "build", *CORE_SERVICES, *CONSUMER_SERVICES, INGESTION_SERVICE],
                timeout=1200,
            )
        full_runner.run_cmd(["docker", "compose", "up", "-d", *CORE_SERVICES], timeout=300)
        self.core_started = True

    def ensure_consumers(self, broker: str, qos: Optional[int], partitions: int) -> None:
        env_overrides = {
            "BROKER_TYPE": broker,
            "DISABLE_DB_WRITE": "true",
            "MQTT_QOS": str(qos or 0),
            "KAFKA_TOPIC_PARTITIONS": str(partitions),
        }
        full_runner.run_cmd(
            ["docker", "compose", "up", "-d", "--force-recreate", "--no-deps", *CONSUMER_SERVICES],
            env_overrides=env_overrides,
            timeout=180,
        )
        self.wait_for_consumers()

    def ensure_ingestion_stopped(self) -> None:
        full_runner.run_cmd(["docker", "compose", "stop", INGESTION_SERVICE], timeout=60, check=False)
        self.ingestion_state = None

    def ensure_ingestion_running(
        self, broker: str, qos: Optional[int], acks: Optional[str], partitions: int
    ) -> None:
        env_overrides = {
            "BROKER_TYPE": broker,
            "DISABLE_DB_WRITE": "true",
            "MQTT_QOS": str(qos or 0),
            "KAFKA_ACKS": str(acks or "1"),
            "KAFKA_TOPIC_PARTITIONS": str(partitions),
            "PUBLISH_QUEUE_MAX_SIZE": "200000",
            "PUBLISH_WORKER_COUNT": "8",
            "OFFLINE_BUFFER_MAX_SIZE": "200000",
            "DISCONNECTED_RETRY_DELAY_MS": "250",
        }
        full_runner.run_cmd(
            ["docker", "compose", "up", "-d", "--force-recreate", "--no-deps", INGESTION_SERVICE],
            env_overrides=env_overrides,
            timeout=180,
        )
        full_runner.wait_for_ingestion_ready()
        self.ingestion_state = (broker, qos, acks, partitions)

    def ensure_stack(self, profile: "full_runner.ScenarioBProfile", *, build_images: bool) -> None:
        self.ensure_core_started()
        self.ensure_consumers(profile.broker, profile.qos, profile.topic_partitions or 1)
        if profile.mode == "tool_benchmark":
            self.ensure_ingestion_stopped()
        else:
            self.ensure_ingestion_running(
                profile.broker, profile.qos, profile.acks, profile.topic_partitions or 1
            )

    def shutdown(self) -> None:
        full_runner.run_cmd(["docker", "compose", "down", "--remove-orphans"], timeout=180, check=False)


# --------------------------------------------------------------------------
# Profile construction
# --------------------------------------------------------------------------

def build_profiles(args: argparse.Namespace) -> List["full_runner.ScenarioBProfile"]:
    mqtt_qos = args.mqtt_qos if args.mqtt_qos is not None else DEFAULT_MQTT_QOS
    kafka_acks = args.kafka_acks if args.kafka_acks is not None else DEFAULT_KAFKA_ACKS

    profiles: List[full_runner.ScenarioBProfile] = []
    for mode in args.modes:
        if args.broker in {"mqtt", "both"}:
            for qos in mqtt_qos:
                profiles.append(
                    full_runner.ScenarioBProfile(
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
            for acks in kafka_acks:
                profiles.append(
                    full_runner.ScenarioBProfile(
                        broker="kafka",
                        mode=mode,
                        acks=str(acks),
                        topic_partitions=args.kafka_partitions,
                        devices=args.devices,
                        interval_sec=args.interval_sec,
                        warmup_sec=args.warmup_sec,
                        outage_sec=args.outage_sec,
                        post_reconnect_run_sec=args.post_reconnect_run_sec,
                    )
                )
    return profiles


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def profile_config_text(broker: str, qos: object = None, acks: object = None, partitions: object = None) -> str:
    if broker == "mqtt":
        return f"qos={qos}"
    return f"acks={acks} p={partitions}"


def describe_profile(profile: "full_runner.ScenarioBProfile") -> str:
    config = profile_config_text(
        profile.broker,
        qos=profile.qos,
        acks=profile.acks,
        partitions=profile.topic_partitions,
    )
    return (
        f"{profile.mode} | {profile.broker} | {config} | "
        f"devices={profile.devices} | warmup={profile.warmup_sec}s | "
        f"outage={profile.outage_sec}s | recovery={profile.post_reconnect_run_sec}s"
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


def max_kafka_lag(result: Dict[str, object]) -> object:
    if result.get("broker") != "kafka":
        return None
    summary = result.get("kafka_consumer_lag_summary") or {}
    lags = [group.get("max_lag", 0) for group in summary.values() if isinstance(group, dict)]
    return max(lags) if lags else None


def result_row_prefix(result: Dict[str, object]) -> Tuple[str, str, str]:
    broker = str(result.get("broker", "?"))
    mode = str(result.get("mode", "?"))
    config = profile_config_text(
        broker,
        qos=result.get("qos"),
        acks=result.get("acks"),
        partitions=result.get("topic_partitions"),
    )
    return mode, broker, config


def status_text(result: Dict[str, object]) -> str:
    if result.get("error"):
        return "ERROR"
    issues = result.get("validation_issues") or []
    return "OK" if not issues else f"WARN({len(issues)})"


def print_table(results: List[Dict[str, object]]) -> None:
    headers = (
        "Mode", "Broker", "Config", "Sent", "Recv", "Loss %",
        "Pub msg/s", "Store msg/s", "p95 ms", "CPU %", "RAM MB",
        "Ready s", "1st Analytics s", "Max Lag", "Status",
    )
    rows: List[Tuple[str, ...]] = []
    error_notes: List[str] = []
    validation_notes: List[str] = []
    for result in results:
        mode, broker, config = result_row_prefix(result)
        if result.get("error"):
            rows.append((mode, broker, config, *(["-"] * 11), "ERROR"))
            error_notes.append(f"{mode} | {broker} | {config}: {result['error']}")
            continue
        issues = result.get("validation_issues") or []
        if issues:
            validation_notes.append(
                f"{mode} | {broker} | {config}: {', '.join(str(issue) for issue in issues)}"
            )
        rows.append(
            (
                mode,
                broker,
                config,
                format_number(result.get("total_successful_publish_messages"), 0),
                format_number(result.get("total_storage_received_messages"), 0),
                format_number(result.get("loss_pct")),
                format_number(result.get("publish_throughput_msg_s")),
                format_number(result.get("storage_throughput_msg_s")),
                format_number(result.get("p95_latency_ms")),
                format_number(result.get("cpu_pct")),
                format_number(result.get("ram_mb")),
                format_number(result.get("recovery_sec_to_source_ready")),
                format_number(result.get("recovery_sec_to_first_analytics_message")),
                format_number(max_kafka_lag(result), 0),
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
    print("Scenario B quick results")
    print("=" * len("Scenario B quick results"))
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


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

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
    full_runner.cleanup_scenario_b_tool_containers()

    payload_tmp = full_runner.build_payload_dir()
    results: List[Dict[str, object]] = []
    run_started_at = time.time()

    try:
        total = len(profiles)
        for index, profile in enumerate(profiles, start=1):
            print(f"[{index}/{total}] {describe_profile(profile)}")
            try:
                result = full_runner.execute_profile(
                    profile,
                    payload_dir=Path(payload_tmp.name),
                    disable_db_write=True,
                    build_images=False,
                )
                results.append(result)
                print(
                    "  "
                    f"sent={result['total_successful_publish_messages']} | "
                    f"recv={result['total_storage_received_messages']} | "
                    f"loss={result['loss_pct']:.2f}% | "
                    f"ready={format_number(result.get('recovery_sec_to_source_ready'))}s | "
                    f"first_analytics={format_number(result.get('recovery_sec_to_first_analytics_message'))}s | "
                    f"p95={format_number(result.get('p95_latency_ms'))}ms"
                )
            except Exception as exc:
                failure: Dict[str, object] = {"broker": profile.broker, "mode": profile.mode, "error": str(exc)}
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
            "--outage-sec/--warmup-sec/--post-reconnect-run-sec, the "
            "number of QoS/acks values, or drop app_buffered from --modes "
            "to bring it back under budget."
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
