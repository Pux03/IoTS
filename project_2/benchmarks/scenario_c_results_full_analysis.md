# Scenario C Analysis

Generated from `scenario_c_results_full.json` on 2026-06-09 00:12 UTC.

This document summarizes the executed burst-load runs and is intended to feed the written report.

## MQTT

- QoS `0`: median peak backlog `1.000` messages, median p95 `20.912` ms, median recovery `19.643` s, median CPU `45.366`%.
- QoS `1`: median peak backlog `118.000` messages, median p95 `474.716` ms, median recovery `19.199` s, median CPU `61.682`%.
- QoS `2`: median peak backlog `220.000` messages, median p95 `967.645` ms, median recovery `57.208` s, median CPU `103.672`%.
- Interpretation: higher MQTT QoS values should reduce delivery risk during the burst, but they tend to increase latency, overrun pressure and recovery cost.

## Kafka

- Partitions `1`: median burst storage throughput `2955.065` msg/s, median peak lag `355.000` messages, median RAM `442.359` MB.
- Partitions `4`: median burst storage throughput `2962.759` msg/s, median peak lag `391.000` messages, median RAM `433.301` MB.
- Partitions `8`: median burst storage throughput `2792.030` msg/s, median peak lag `410.000` messages, median RAM `453.562` MB.
- Interpretation: Kafka exposes burst pressure directly through consumer lag and offset drift, while partitions trade memory/CPU for parallelism and smoother backlog drainage.

## MQTT vs Kafka

- MQTT median broker footprint: `61.682`% CPU / `16.320` MB RAM; Kafka median broker footprint: `100.560`% CPU / `442.359` MB RAM.
- MQTT median p95 latency across executed burst runs: `474.716` ms; Kafka median p95 latency: `111.000` ms.
- MQTT remains the lighter option for edge publication, while Kafka gives stronger backlog observability and partition scaling at a noticeably higher resource cost.

## Report Implications

- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.
- MQTT edge suitability can be argued from its smaller footprint and simpler broker stack, while its burst behavior becomes less attractive when we need replayable, lag-aware historical analytics.
- Kafka cloud suitability can be argued from its lag visibility, partition scaling and backlog control, while the price is substantially higher CPU/RAM usage.
