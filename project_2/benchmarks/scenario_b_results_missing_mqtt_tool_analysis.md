# Scenario B Analysis

Generated from `scenario_b_results_missing_mqtt_tool.json` on 2026-06-08 21:06 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### MQTT

- QoS 0 average loss after outage: `50.000`%; average source recovery time: `0.219` s; average p95 latency: `30000.000` ms.
- QoS 1/2 average loss after outage: `18.593`%; average source recovery time: `10.876` s; average p95 latency: `15234.408` ms.
- Interpretation: QoS 0 shows the lowest protocol overhead, while QoS 1/2 trade extra handshake cost for stronger delivery guarantees after reconnect.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs, especially in tool-benchmark mode where application buffering is removed from the picture.
- Kafka cloud suitability can be argued from explicit lag/offset visibility and the partition scaling data, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report, while this analysis file can seed the narrative answers to the engineering questions.
