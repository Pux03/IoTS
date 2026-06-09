# Scenario B Analysis

Generated from `scenario_b_results_missing_kafka_acks1_p1_tool.json` on 2026-06-08 21:39 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### Kafka

- Average broker CPU footprint: `78.287`%; average RAM footprint: `332.242` MB; average p95 latency: `47392.000` ms.
- Average peak consumer lag across executed Kafka runs: `0.000` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.
- Partitions `1`: average storage throughput `50.304` msg/s; average peak lag `0.000` messages.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs, especially in tool-benchmark mode where application buffering is removed from the picture.
- Kafka cloud suitability can be argued from explicit lag/offset visibility and the partition scaling data, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report, while this analysis file can seed the narrative answers to the engineering questions.
