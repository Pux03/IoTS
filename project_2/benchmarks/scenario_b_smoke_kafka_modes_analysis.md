# Scenario B Analysis

Generated from `scenario_b_smoke_kafka_modes.json` on 2026-06-08 16:47 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### Kafka

- Average broker CPU footprint: `43.444`%; average RAM footprint: `335.918` MB.
- Average peak consumer lag across executed Kafka runs: `0.000` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.

## Mode: `app_buffered`

### Kafka

- Average broker CPU footprint: `37.319`%; average RAM footprint: `332.160` MB.
- Average peak consumer lag across executed Kafka runs: `16.000` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs.
- Kafka cloud suitability can be argued from higher throughput and explicit lag/offset visibility, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report.
