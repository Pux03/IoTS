# Scenario D Analysis

Generated from `scenario_d_results_full.json` on 2026-06-09 09:57 UTC.

This document summarizes the alert-latency benchmark runs and is intended to feed the written report.

## MQTT

- `mqtt_qos_0_window_early`: alert avg `6638.000` ms, p95 `7759.000` ms, CPU `0.084`%, RAM `0.695` MB.
- `mqtt_qos_0_window_late`: alert avg `7985.667` ms, p95 `9307.000` ms, CPU `0.099`%, RAM `0.707` MB.
- `mqtt_qos_1_window_early`: alert avg `6708.667` ms, p95 `7678.000` ms, CPU `0.134`%, RAM `0.703` MB.
- `mqtt_qos_1_window_late`: alert avg `8126.000` ms, p95 `9027.000` ms, CPU `0.098`%, RAM `0.699` MB.
- `mqtt_qos_2_window_early`: alert avg `6796.667` ms, p95 `7595.000` ms, CPU `0.119`%, RAM `0.699` MB.
- `mqtt_qos_2_window_late`: alert avg `8076.667` ms, p95 `9180.000` ms, CPU `0.170`%, RAM `0.707` MB.

## Kafka

- `kafka_acks_0_partitions_1_window_early`: alert avg `9847.667` ms, p95 `9848.000` ms, peak lag `0.000` messages, RAM `310.027` MB.
- `kafka_acks_0_partitions_1_window_late`: alert avg `11198.000` ms, p95 `11200.000` ms, peak lag `0.000` messages, RAM `307.879` MB.
- `kafka_acks_1_partitions_1_window_early`: alert avg `9849.667` ms, p95 `9851.000` ms, peak lag `0.000` messages, RAM `305.160` MB.
- `kafka_acks_1_partitions_1_window_late`: alert avg `11197.000` ms, p95 `11198.000` ms, peak lag `0.000` messages, RAM `308.879` MB.
- `kafka_acks_all_partitions_1_window_early`: alert avg `9847.000` ms, p95 `9848.000` ms, peak lag `0.000` messages, RAM `310.199` MB.
- `kafka_acks_all_partitions_1_window_late`: alert avg `11197.333` ms, p95 `11198.000` ms, peak lag `0.000` messages, RAM `311.219` MB.

## MQTT vs Kafka

- MQTT average alert-latency p95 across profile summaries: `8424.333` ms.
- Kafka average alert-latency p95 across profile summaries: `10523.833` ms.
- MQTT median broker footprint trend: `0.117`% CPU / `0.702` MB RAM.
- Kafka median broker footprint trend: `36.812`% CPU / `308.894` MB RAM.

## Report Implications

- Use the performance table directly in the real-time alerting comparison chapter.
- Use `early` vs `late` window placement to explain how the 10-second tumbling window dominates end-user alert delay.
- Use Kafka lag snapshots to discuss observability and replay-oriented cloud-side processing.
