# Scenario C Analysis

Generated from `scenario_c_smoke_mqtt.json` on 2026-06-08 22:18 UTC.

This document summarizes the executed burst-load runs and is intended to feed the written report.

## MQTT

- QoS `0`: median peak backlog `2.000` messages, median p95 `7.315` ms, median recovery `3.108` s, median CPU `3.954`%.
- Interpretation: higher MQTT QoS values should reduce delivery risk during the burst, but they tend to increase latency, overrun pressure and recovery cost.

## Report Implications

- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.
- MQTT edge suitability can be argued from its smaller footprint and simpler broker stack, while its burst behavior becomes less attractive when we need replayable, lag-aware historical analytics.
- Kafka cloud suitability can be argued from its lag visibility, partition scaling and backlog control, while the price is substantially higher CPU/RAM usage.
