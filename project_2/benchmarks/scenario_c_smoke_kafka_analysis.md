# Scenario C Analysis

Generated from `scenario_c_smoke_kafka.json` on 2026-06-08 22:20 UTC.

This document summarizes the executed burst-load runs and is intended to feed the written report.

## Kafka

- Partitions `1`: median burst storage throughput `93.652` msg/s, median peak lag `3.000` messages, median RAM `327.371` MB.
- Interpretation: Kafka exposes burst pressure directly through consumer lag and offset drift, while partitions trade memory/CPU for parallelism and smoother backlog drainage.

## Report Implications

- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.
- MQTT edge suitability can be argued from its smaller footprint and simpler broker stack, while its burst behavior becomes less attractive when we need replayable, lag-aware historical analytics.
- Kafka cloud suitability can be argued from its lag visibility, partition scaling and backlog control, while the price is substantially higher CPU/RAM usage.
