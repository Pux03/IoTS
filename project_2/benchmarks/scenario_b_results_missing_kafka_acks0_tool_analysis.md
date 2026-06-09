# Scenario B Analysis

Generated from `scenario_b_results_missing_kafka_acks0_tool.json` on 2026-06-08 21:26 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### Kafka

- Average broker CPU footprint: `106.763`%; average RAM footprint: `339.938` MB; average p95 latency: `47067.667` ms.
- Average peak consumer lag across executed Kafka runs: `0.000` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.
- Partitions `1`: average storage throughput `48.276` msg/s; average peak lag `0.000` messages.
- Partitions `4`: average storage throughput `48.289` msg/s; average peak lag `0.000` messages.
- Partitions `8`: average storage throughput `48.067` msg/s; average peak lag `0.000` messages.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs, especially in tool-benchmark mode where application buffering is removed from the picture.
- Kafka cloud suitability can be argued from explicit lag/offset visibility and the partition scaling data, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report, while this analysis file can seed the narrative answers to the engineering questions.
