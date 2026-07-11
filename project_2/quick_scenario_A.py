"""
quick_scenario_A.py

Fast, configurable, SINGLE-RUN Scenario A sanity check.

Goal: give a CLI results table for MQTT (QoS 0/1/2) and/or Kafka
(acks 0/1/all) in well under ~90 seconds total, on a machine where the
project's Docker images are already built (`docker compose build` once,
beforehand, or pass --build-images the first time).

How it stays fast (vs. benchmarks/run_scenario_a.py, which restarts the
whole stack before every single profile):

  - Exactly ONE execution per selected profile. No repeats, no retries.
  - Container restarts only happen when they are actually required:
    switching MQTT <-> Kafka, or changing the Kafka partition count.
    Changing QoS or acks does NOT restart anything, because the load
    tools (emqtt-bench / kafka-producer-perf-test.sh) publish directly
    against the already-running broker.
  - `data-ingestion` is not started at all: it is the "device simulator"
    for full-scale runs and isn't needed for these direct-to-broker
    load tests, so its (slow) startup is skipped entirely.
  - Small default workload (few devices, ~2s of traffic per profile).
  - Wait/settle timeouts are hard-capped (see --max-wait-sec) so a
    single flaky profile can't blow the whole time budget.

Nothing is written to disk; results are only printed as a CLI table,
plus a total wall-clock time at the end.

Examples:
    python quick_scenario_A.py
    python quick_scenario_A.py --broker mqtt --mqtt-qos 0 1
    python quick_scenario_A.py --broker kafka --kafka-acks 1 all --devices 50
    python quick_scenario_A.py --devices 10 --duration-sec 1 --verbose
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import benchmarks.run_scenario_a as full_runner


REPO_ROOT = Path(__file__).resolve().parent

# Services needed to *measure* Scenario A. data-ingestion is intentionally
# excluded: emqtt-bench / kafka-producer-perf-test.sh publish straight to
# the broker, bypassing it.
CORE_SERVICES = ["db", "mqtt-broker", "kafka-broker", "resource-monitor"]
CONSUMER_SERVICES = ["data-storage", "analytics-service"]

DEFAULT_MQTT_QOS = [0, 1, 2]
DEFAULT_KAFKA_ACKS = ["0", "1", "all"]
DEFAULT_KAFKA_PARTITIONS = 1
DEFAULT_DEVICES = 20
DEFAULT_DURATION_SEC = 2.0
DEFAULT_INTERVAL_SEC = 1.0
DEFAULT_MAX_WAIT_SEC = 15
DEFAULT_TIME_BUDGET_SEC = 90


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quick Scenario A sanity check. Runs exactly one iteration per "
            "selected profile and prints a results table to the CLI. "
            "Tuned to finish well under 90 seconds by default."
        )
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka", "both"], default="both")
    parser.add_argument(
        "--mqtt-qos", nargs="+", type=int, default=None,
        help=f"MQTT QoS levels to test (default: {DEFAULT_MQTT_QOS}).",
    )
    parser.add_argument(
        "--kafka-acks", nargs="+", default=None,
        help=f"Kafka acks values to test (default: {DEFAULT_KAFKA_ACKS}).",
    )
    parser.add_argument(
        "--kafka-partitions", type=int, default=DEFAULT_KAFKA_PARTITIONS,
        help="Single Kafka partition count used for all Kafka profiles "
             "in this run (changing it mid-run would require a restart, "
             "so this script only takes one value).",
    )
    parser.add_argument("--devices", type=int, default=DEFAULT_DEVICES)
    parser.add_argument("--duration-sec", type=float, default=DEFAULT_DURATION_SEC)
    parser.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC)
    parser.add_argument(
        "--max-wait-sec", type=int, default=DEFAULT_MAX_WAIT_SEC,
        help="Hard cap on how long any single settle/probe wait may take "
             "(protects the overall time budget).",
    )
    parser.add_argument(
        "--time-budget-sec", type=int, default=DEFAULT_TIME_BUDGET_SEC,
        help="Only used to print a warning if the run exceeds it; does not "
             "abort the run.",
    )
    parser.add_argument(
        "--build-images", action="store_true",
        help="Build Docker images before starting (slow, do this once).",
    )
    parser.add_argument(
        "--keep-stack-up", action="store_true",
        help="Leave containers running after the script finishes.",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print underlying docker/tool commands while running.",
    )
    return parser.parse_args()


# --------------------------------------------------------------------------
# Command execution / monkey-patches
# --------------------------------------------------------------------------

def patch_runner(verbose: bool, max_wait_sec: int) -> None:
    """Make full_runner quieter (unless --verbose) and cap its wait timeouts
    so a single slow profile cannot eat the whole time budget."""

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

    original_settle = full_runner.wait_for_pipeline_settle

    def capped_settle(*, base_snapshot, expected_messages, timeout_sec, **kwargs):
        return original_settle(
            base_snapshot=base_snapshot,
            expected_messages=expected_messages,
            timeout_sec=min(timeout_sec, max_wait_sec),
            **kwargs,
        )

    full_runner.wait_for_pipeline_settle = capped_settle

    original_probe_wait = full_runner.wait_for_probe_receipts

    def capped_probe_wait(*, port, expected_messages, timeout_sec, **kwargs):
        return original_probe_wait(
            port=port,
            expected_messages=expected_messages,
            timeout_sec=min(timeout_sec, max_wait_sec),
            **kwargs,
        )

    full_runner.wait_for_probe_receipts = capped_probe_wait


# --------------------------------------------------------------------------
# Stack lifecycle (restart only when actually necessary)
# --------------------------------------------------------------------------

def consumer_env(broker: str, partitions: int) -> Dict[str, str]:
    return {
        "BROKER_TYPE": broker,
        "DISABLE_DB_WRITE": "true",
        "KAFKA_TOPIC_PARTITIONS": str(partitions),
    }


def wait_for_consumers(timeout_sec: int = 30) -> None:
    """Lightweight readiness wait: only checks the services this script
    actually starts (data-ingestion is deliberately not started)."""
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


def start_core_stack(build_images: bool) -> None:
    if build_images:
        full_runner.run_cmd(
            ["docker", "compose", "build", *CORE_SERVICES, *CONSUMER_SERVICES],
            timeout=1200,
        )
    full_runner.run_cmd(["docker", "compose", "up", "-d", *CORE_SERVICES], timeout=300)


def switch_consumers(broker: str, partitions: int) -> None:
    env_overrides = consumer_env(broker, partitions)
    full_runner.run_cmd(
        ["docker", "compose", "up", "-d", "--force-recreate", "--no-deps", *CONSUMER_SERVICES],
        env_overrides=env_overrides,
        timeout=180,
    )
    wait_for_consumers()


def shutdown_stack() -> None:
    full_runner.run_cmd(
        ["docker", "compose", "down", "--remove-orphans"],
        timeout=180,
        check=False,
    )


# --------------------------------------------------------------------------
# Profile construction
# --------------------------------------------------------------------------

def build_profiles(args: argparse.Namespace) -> List[full_runner.ScenarioAProfile]:
    mqtt_qos = args.mqtt_qos if args.mqtt_qos is not None else DEFAULT_MQTT_QOS
    kafka_acks = args.kafka_acks if args.kafka_acks is not None else DEFAULT_KAFKA_ACKS

    profiles: List[full_runner.ScenarioAProfile] = []
    if args.broker in {"mqtt", "both"}:
        for qos in mqtt_qos:
            profiles.append(
                full_runner.ScenarioAProfile(
                    broker="mqtt",
                    qos=int(qos),
                    devices=args.devices,
                    interval_sec=args.interval_sec,
                    duration_sec=int(round(args.duration_sec)),
                )
            )
    if args.broker in {"kafka", "both"}:
        for acks in kafka_acks:
            profiles.append(
                full_runner.ScenarioAProfile(
                    broker="kafka",
                    acks=str(acks),
                    topic_partitions=args.kafka_partitions,
                    devices=args.devices,
                    interval_sec=args.interval_sec,
                    duration_sec=int(round(args.duration_sec)),
                )
            )
    return profiles


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def short_profile_name(result: Dict[str, object]) -> str:
    if result["broker"] == "mqtt":
        return f"mqtt qos={result.get('qos')}"
    return f"kafka acks={result.get('acks')} p={result.get('topic_partitions')}"


def format_number(value: object, decimals: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.{decimals}f}"


def print_table(results: List[Dict[str, object]]) -> None:
    headers = ("Profile", "Sent", "Recv", "Loss %", "Cons msg/s", "p95 ms", "CPU %", "RAM MB", "Status")
    rows: List[Tuple[str, ...]] = []
    for result in results:
        if result.get("error"):
            rows.append((short_profile_name(result), "-", "-", "-", "-", "-", "-", "-", f"ERROR: {result['error']}"))
            continue
        issues = result.get("validation_issues") or []
        rows.append(
            (
                short_profile_name(result),
                format_number(result.get("messages_sent"), 0),
                format_number(result.get("messages_received"), 0),
                format_number(result.get("loss_pct")),
                format_number(result.get("consumer_throughput_msg_per_sec")),
                format_number(result.get("p95_latency_ms")),
                format_number(result.get("cpu_pct")),
                format_number(result.get("ram_mb")),
                "OK" if not issues else ",".join(str(issue) for issue in issues),
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
    print(" | ".join(trim(header, widths[index]) for index, header in enumerate(headers)))
    print(separator)
    for row in rows:
        print(" | ".join(trim(cell, widths[index]) for index, cell in enumerate(row)))
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

    payload_tmp = full_runner.build_payload_dir()
    results: List[Dict[str, object]] = []
    active_broker: Optional[str] = None
    active_partitions: Optional[int] = None
    run_started_at = time.time()

    try:
        start_core_stack(build_images=args.build_images)

        total = len(profiles)
        for index, profile in enumerate(profiles, start=1):
            print(f"[{index}/{total}] {profile.config_name}")
            needs_switch = (
                profile.broker != active_broker
                or (profile.broker == "kafka" and profile.topic_partitions != active_partitions)
            )
            try:
                if needs_switch:
                    switch_consumers(profile.broker, profile.topic_partitions or 1)
                    active_broker = profile.broker
                    active_partitions = profile.topic_partitions if profile.broker == "kafka" else active_partitions

                result = full_runner.execute_profile(profile=profile, payload_dir=Path(payload_tmp.name))
                results.append(result)
                print(
                    f"  sent={result['messages_sent']} recv={result['messages_received']} "
                    f"loss={result['loss_pct']:.2f}% "
                    f"consumer={result['consumer_throughput_msg_per_sec']:.2f} msg/s "
                    f"p95={format_number(result.get('p95_latency_ms'))} ms"
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
            shutdown_stack()

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
            f"NOTE: exceeded the {args.time_budget_sec}s target. "
            "Reduce --devices/--duration-sec or the number of QoS/acks "
            "values to bring it back under budget."
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())