# Scenario C Analysis

Generated from `scenario_c_results.json` on 2026-06-15 15:32 UTC.

This document summarizes the executed burst-load runs and is intended to feed the written report.

## MQTT

- QoS `0`: median peak backlog `0.000` messages, median p95 `16.180` ms, median recovery `18.452` s, median CPU `25.934`%.
- QoS `1`: median peak backlog `292.000` messages, median p95 `475.340` ms, median recovery `20.151` s, median CPU `67.640`%.
- QoS `2`: median peak backlog `0.000` messages, median p95 `969.473` ms, median recovery `74.125` s, median CPU `102.839`%.
- Interpretation: higher MQTT QoS values should reduce delivery risk during the burst, but they tend to increase latency, overrun pressure and recovery cost.

## Report Implications

- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.
- MQTT edge suitability can be argued from its smaller footprint and simpler broker stack, while its burst behavior becomes less attractive when we need replayable, lag-aware historical analytics.
- Kafka cloud suitability can be argued from its lag visibility, partition scaling and backlog control, while the price is substantially higher CPU/RAM usage.
