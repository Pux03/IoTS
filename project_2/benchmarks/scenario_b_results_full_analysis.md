# Scenario B Analysis

Generated from `scenario_b_results_full.json` on 2026-06-08 21:40 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### MQTT

- QoS 0 average loss after outage: `50.000`%; average source recovery time: `0.219` s; average p95 latency: `30000.000` ms.
- QoS 1/2 average loss after outage: `18.593`%; average source recovery time: `10.876` s; average p95 latency: `15234.408` ms.
- Interpretation: QoS 0 shows the lowest protocol overhead, while QoS 1/2 trade extra handshake cost for stronger delivery guarantees after reconnect.

### Kafka

- Average broker CPU footprint: `102.665`%; average RAM footprint: `338.751` MB; average p95 latency: `47020.889` ms.
- Average peak consumer lag across executed Kafka runs: `0.000` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.
- Partitions `1`: average storage throughput `49.786` msg/s; average peak lag `0.000` messages.
- Partitions `4`: average storage throughput `49.907` msg/s; average peak lag `0.000` messages.
- Partitions `8`: average storage throughput `49.819` msg/s; average peak lag `0.000` messages.

### MQTT vs Kafka

- MQTT average broker footprint in this mode: `24.716`% CPU / `7.242` MB RAM; Kafka: `102.665`% CPU / `338.751` MB RAM.
- MQTT average p95 latency in this mode: `20156.272` ms; Kafka average p95 latency: `47020.889` ms.
- Tool-benchmark mode isolates broker-level recovery, so these numbers are the cleanest basis for the broker comparison chapter.

## Mode: `app_buffered`

### MQTT

- QoS 0 average loss after outage: `0.151`%; average source recovery time: `25.425` s; average p95 latency: `55606.319` ms.
- QoS 1/2 average loss after outage: `0.000`%; average source recovery time: `23.752` s; average p95 latency: `70575.049` ms.
- Interpretation: QoS 0 shows the lowest protocol overhead, while QoS 1/2 trade extra handshake cost for stronger delivery guarantees after reconnect.

### Kafka

- Average broker CPU footprint: `91.160`%; average RAM footprint: `791.469` MB; average p95 latency: `111485.106` ms.
- Average peak consumer lag across executed Kafka runs: `17.444` messages.
- Interpretation: Kafka exposes recovery state explicitly through offsets and lag, which is useful for cloud-side observability.
- Partitions `1`: average storage throughput `124.770` msg/s; average peak lag `16.000` messages.
- Partitions `4`: average storage throughput `124.691` msg/s; average peak lag `17.667` messages.
- Partitions `8`: average storage throughput `125.934` msg/s; average peak lag `18.667` messages.

### MQTT vs Kafka

- MQTT average broker footprint in this mode: `34.060`% CPU / `11.753` MB RAM; Kafka: `91.160`% CPU / `791.469` MB RAM.
- MQTT average p95 latency in this mode: `65585.472` ms; Kafka average p95 latency: `111485.106` ms.
- App-buffered mode includes the ingestion service offline queue, so these numbers show end-to-end operational behavior rather than broker-only behavior.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs, especially in tool-benchmark mode where application buffering is removed from the picture.
- Kafka cloud suitability can be argued from explicit lag/offset visibility and the partition scaling data, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report, while this analysis file can seed the narrative answers to the engineering questions.
