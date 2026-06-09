# Benchmark Guide

This folder now contains two different benchmark entry points:

- `run_scenario_a.py`
  Primary dedicated script for Scenario A execution.
  By default it runs the full matrix:
  - MQTT: `QoS 0`, `QoS 1`, `QoS 2`
  - Kafka: `acks 0`, `acks 1`, `acks all`
  - Kafka partitions: `1`, `4`, `8`
  - Device groups: `100`, `1000`, `10000`
- `run_scenario_b.py`
  Dedicated outage/recovery runner for Scenario B.
  It supports:
  - MQTT: `QoS 0`, `QoS 1`, `QoS 2`
  - Kafka: `acks 0`, `acks 1`, `acks all`
  - execution modes: `tool_benchmark` and `app_buffered`
  - Kafka partitions: `1`, `4`, `8`
  - automatic stack restart per test
  - simulator network disconnect / reconnect
  - backlog, drop and recovery metrics
  - publish/storage/analytics throughput summaries
  - p95 / average / maximum E2E latency
  - broker CPU / RAM / network footprint
  - Kafka consumer lag snapshots during recovery
  - Markdown export for a comparison table and report-ready analysis
- `run_scenario_c.py`
  Dedicated burst-load runner for Scenario C.
  It supports:
  - MQTT: `QoS 0`, `QoS 1`, `QoS 2`
  - Kafka: `acks 0`, `acks 1`, `acks all`
  - Kafka partitions: `1`, `4`, `8`
  - dedicated-tool publishing only
  - `50 -> 5000 -> 50 msg/s` burst phases
  - storage / analytics backlog tracking
  - Kafka consumer lag sampling during burst and drain
  - Markdown export for a comparison table and report-ready analysis
- `run_scenario_d.py`
  Dedicated alert-latency runner for Scenario D.
  It supports:
  - MQTT: `QoS 0`, `QoS 1`, `QoS 2`
  - Kafka: `acks 0`, `acks 1`, `acks all`
  - Kafka partitions: default `1`, optional `4` / `8`
  - window-aligned `early` and `late` critical-event placement
  - max `3` repeats per profile
  - broker CPU / RAM / network sampling
  - Kafka consumer lag snapshots before / after alert
  - Markdown export for a comparison table and report-ready analysis
- `run_protocol_benchmarks.py`
  Legacy entry point name kept for compatibility.
- `run_all_scenarios.py`
  Legacy application-scenario orchestrator that drives the existing FastAPI endpoints.

## Recommended Flow

Use `run_scenario_a.py` when you need the full, PDF-aligned Scenario A path.

Example commands:

```bash
python benchmarks/run_scenario_a.py
python benchmarks/run_scenario_a.py --broker mqtt --mqtt-qos 0 1 2
python benchmarks/run_scenario_a.py --broker kafka --kafka-acks 0 1 all --kafka-partitions 1 4 8
python benchmarks/run_scenario_b.py
python benchmarks/run_scenario_b.py --broker mqtt --modes tool_benchmark app_buffered --mqtt-qos 0 1 2 --outage-sec 30
python benchmarks/run_scenario_b.py --broker kafka --modes tool_benchmark app_buffered --kafka-acks 0 1 all --kafka-partitions 1 4 8 --outage-sec 30
python benchmarks/run_scenario_c.py
python benchmarks/run_scenario_c.py --broker mqtt --mqtt-qos 0 1 2 --repeats 3
python benchmarks/run_scenario_c.py --broker kafka --kafka-acks 0 1 all --kafka-partitions 1 4 8 --repeats 3
python benchmarks/run_scenario_d.py
python benchmarks/run_scenario_d.py --broker mqtt --mqtt-qos 0 1 2 --window-modes late early --repeats 3
python benchmarks/run_scenario_d.py --broker kafka --kafka-acks 0 1 all --kafka-partitions 1 4 8 --window-modes late early --repeats 3
```

## What The Script Does

- Restarts the Docker stack for each individual test profile.
- Forces `DISABLE_DB_WRITE=true` so the benchmark isolates broker and consumer behavior instead of PostgreSQL write speed.
- Waits for `data-ingestion`, `data-storage`, and `analytics-service` to become ready.
- In `tool_benchmark` mode, publishes benchmark traffic with:
  - `emqx/emqtt-bench:latest` for MQTT
  - `apache/kafka:3.7.0` + `/opt/kafka/bin/kafka-producer-perf-test.sh` for Kafka
- In `app_buffered` mode, keeps `data-ingestion` as the simulator so we can compare broker recovery with and without the application-side offline buffer.
- Disconnects the active simulator container with `docker network disconnect`, waits through the outage window, and reconnects it.
- Reads `storage_messages_received_total` and `analytics_messages_processed_total` from the service metrics endpoints.
- Samples broker CPU, RAM and network traffic from `resource-monitor` during each run.
- Collects Kafka consumer lag for `data-storage-group` and `analytics-group`.
- Waits for the message counters to settle instead of relying on a fixed sleep.
- Reports validation issues if received counters exceed requested traffic or if the pipeline does not settle in time.
- Writes aggregated results to `benchmarks/scenario_a_results.json` unless another path is provided.
- For Scenario B, writes aggregated outage/recovery results to `benchmarks/scenario_b_results.json` unless another path is provided.
- Scenario B result JSON also includes total and phase throughput, `avg_latency_ms`, `p95_latency_ms`, `max_latency_ms`, and broker resource metrics (`cpu_pct`, `ram_mb`, `network_mb`).
- Scenario B also generates:
  - `*_performance_table.md` with the report table rows
  - `*_analysis.md` with a written MQTT vs Kafka recovery interpretation
- For Scenario C, writes aggregated burst-load results to `benchmarks/scenario_c_results.json` unless another path is provided.
- Scenario C result JSON includes warmup / burst / recovery / drain phase summaries, backlog peaks, recovery timing, latency, broker resource metrics, and Kafka lag.
- Scenario C also generates:
  - `*_performance_table.md` with the report table rows
  - `*_analysis.md` with a written MQTT vs Kafka burst-load interpretation
- For Scenario D, writes aggregated alert-latency results to `benchmarks/scenario_d_results.json` unless another path is provided.
- Scenario D result JSON includes window placement metadata, alert first/last latency, broker resource metrics, and Kafka lag snapshots.
- Scenario D also generates:
  - `*_performance_table.md` with alert-latency comparison rows
  - `*_analysis.md` with a written MQTT vs Kafka real-time alerting interpretation

## Notes

- The tool-based benchmark script is meant to close the "use dedicated benchmark tools" gap from the PDF.
- The legacy scenario script is still useful for business-flow checks such as outage simulation and alert latency, but it is no longer the primary benchmark path for protocol comparison.
- For MQTT runs, delivery/loss still comes from the consumer-side Prometheus counters, while latency comes from the official `emqtt-bench` `e2e_latency` histogram.
- For MQTT runs, `max_latency_ms` is the histogram upper-bound bucket that contains the slowest observed message, so it is intentionally conservative.
- Scenario B intentionally runs in two modes:
  - `tool_benchmark`: isolates broker recovery with dedicated publisher tools
  - `app_buffered`: shows how the existing ingestion service behaves when its own offline buffer is part of the recovery path
- Scenario C uses dedicated publisher tools only and models the burst as two concurrent publishers:
  - one baseline publisher fixed at `50 msg/s`
  - one temporary burst publisher that adds the extra load needed to reach `5000 msg/s`
- Scenario D uses dedicated publisher tools only and aligns critical-event publication to the analytics tumbling-window boundary:
  - `early`: immediately after a new window starts
  - `late`: shortly before the current window closes
