# Scenario A Analysis

Generated from `scenario_a_results_full.json` on 2026-06-09 10:30 UTC.

This document summarizes the executed massive-ingestion runs and is intended to feed the written report.

## MQTT

- Devices `100`: median loss `0.000`%, median p95 `24.130` ms, median CPU `2.300`%.
- Devices `1000`: median loss `0.000`%, median p95 `479.335` ms, median CPU `25.501`%.
- Devices `10000`: median loss `98.944`%, median p95 `14739.130` ms, median CPU `100.763`%.
- Interpretation: MQTT remains viable at smaller scales, but at `10000` devices the executed matrix reaches up to `98.960`% loss for higher QoS levels.

## Kafka

- Devices `100`: median producer throughput `99.930` msg/s, median p95 `8.000` ms, median RAM `347.027` MB.
- Devices `1000`: median producer throughput `998.104` msg/s, median p95 `23.000` ms, median RAM `452.492` MB.
- Devices `10000`: median producer throughput `9980.040` msg/s, median p95 `69.000` ms, median RAM `404.379` MB.
- Interpretation: Kafka keeps `0%` loss across the executed scale matrix, while partitions and acks trade higher resource cost for stronger cloud-oriented observability and delivery guarantees.

## MQTT vs Kafka

- MQTT median broker footprint across executed runs: `25.501`% CPU / `8.359` MB RAM.
- Kafka median broker footprint across executed runs: `226.340`% CPU / `377.578` MB RAM.
- MQTT median p95 latency across executed runs: `479.335` ms; Kafka median p95 latency: `19.000` ms.
- MQTT is the lighter option for edge ingestion, while Kafka is the more scalable and loss-resistant option for data-intensive cloud pipelines.

## Report Implications

- The performance table can be copied directly into the comparative Throughput / p95 / CPU / RAM chapter.
- Scenario A is the strongest experimental basis for discussing pure ingest scalability and loss under rising device counts.
- The `10000` device runs should be emphasized in the written report because they make the MQTT vs Kafka scaling trade-off the clearest.
