| Mode | Broker | Config | Partitions | Publish msg/s | Storage msg/s | p95 ms | CPU % | RAM MB | Loss % | Ready s | First Analytics s | Max Lag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_1_outage | 1 | 50.272 | 48.276 | 47427.000 | 101.105 | 338.512 | 3.970 | 6.074 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_4_outage | 4 | 50.280 | 48.289 | 47342.000 | 125.213 | 336.766 | 3.960 | 6.334 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_8_outage | 8 | 50.049 | 48.067 | 46434.000 | 93.971 | 344.535 | 3.960 | 4.865 | 0.567 | 0 |
