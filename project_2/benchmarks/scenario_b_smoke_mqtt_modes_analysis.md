# Scenario B Analysis

Generated from `scenario_b_smoke_mqtt_modes.json` on 2026-06-08 16:44 UTC.

This document summarizes the executed Scenario B runs and is intended to feed the written report.

## Mode: `tool_benchmark`

### MQTT

- QoS 0 average loss after outage: `46.524`%; average source recovery time: `0.215` s.
- QoS 1/2 average loss after outage: `-`%; average source recovery time: `-` s.
- Interpretation: lower QoS should favor minimal overhead, while QoS 1/2 should favor stronger delivery guarantees after reconnect.

## Mode: `app_buffered`

### MQTT

- QoS 0 average loss after outage: `5.525`%; average source recovery time: `1.055` s.
- QoS 1/2 average loss after outage: `-`%; average source recovery time: `-` s.
- Interpretation: lower QoS should favor minimal overhead, while QoS 1/2 should favor stronger delivery guarantees after reconnect.

## Report Implications

- MQTT edge suitability can be argued from its smaller resource footprint and simpler reconnect behavior in the executed runs.
- Kafka cloud suitability can be argued from higher throughput and explicit lag/offset visibility, at the cost of larger CPU/RAM usage.
- The Markdown performance table should be used directly when filling the comparative Throughput / p95 / CPU / RAM table in the report.
