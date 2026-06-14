# Tehnicki Izvestaj
## Uporedna Evaluacija MQTT i Kafka Brokera u IoT Mikroservisnoj Arhitekturi

## 1. Cilj Projekta

Cilj projekta je bio da se istraze performanse, skalabilnost i ogranicenja dva publish-subscribe message broker sistema, `MQTT (Mosquitto)` i `Apache Kafka`, u okviru jedne iste IoT mikroservisne arhitekture. Poseban fokus bio je na razumevanju trade-off odnosa izmedju:
- kasnjenja i pouzdanosti
- potrosnje resursa i skalabilnosti
- pogodnosti za edge i cloud okruzenja

Eksperimentalni deo projekta realizovan je kroz cetiri scenarija:
- `Scenario A`: Massive Sensor Ingestion
- `Scenario B`: Edge Connectivity Failures
- `Scenario C`: Burst Event Load
- `Scenario D`: Real-Time Alerting

Sve numericke vrednosti u ovom izvestaju izvedene su iz sledecih finalnih performance tabela:
- [scenario_a_results_full_performance_table.md](../benchmarks/scenario_a_results_full_performance_table.md)
- [scenario_b_results_full_performance_table.md](../benchmarks/scenario_b_results_full_performance_table.md)
- [scenario_c_results_full_performance_table.md](../benchmarks/scenario_c_results_full_performance_table.md)
- [scenario_d_results_full_performance_table.md](../benchmarks/scenario_d_results_full_performance_table.md)

## 2. Arhitektura Sistema

Projektovana je asinhrona, event-driven mikroservisna arhitektura sa tri glavne komponente:
- `Data Ingestion Service`: simulira IoT uredjaje i salje poruke ka MQTT ili Kafka brokeru
- `Data Storage Service`: pretplacen je na broker i upisuje poruke u PostgreSQL
- `Analytics Service`: radi stream obradu nad 10-sekundnim tumbling window-om i podize alert kada prosecna temperatura predje prag

Pratece komponente sistema su:
- `PostgreSQL` kao baza podataka
- `Docker Compose` za orkestraciju
- `Mosquitto` kao MQTT broker
- `Apache Kafka` u `KRaft` rezimu, bez Zookeeper-a
- monitoring sloj za CPU, RAM i mrezu

Tokom intenzivnih stress-testova, narocito u scenarijima `A` i `C`, DB write je bio iskljucen ili je koriscen batching kako bi fokus ostao na brokeru i potrosackom lancu, a ne na I/O uskom grlu baze.

## 3. Metodologija Merenja

Za generisanje opterecenja i merenje korisceni su namenski alati:
- `emqtt-bench` za MQTT
- `kafka-producer-perf-test.sh` za Kafka
- Docker stats API i monitoring sloj za `CPU`, `RAM` i `network` metrike

Konfiguracije koje su testirane:
- `MQTT`: `QoS 0`, `QoS 1`, `QoS 2`
- `Kafka`: `acks=0`, `acks=1`, `acks=all`
- gde je relevantno, za Kafka su testirane i `1`, `4` i `8` particija

Metrike koje su posmatrane kroz scenarije:
- throughput
- loss procenat
- `p95` latencija
- CPU i RAM footprint
- network saobracaj
- backlog i consumer lag
- recovery vreme
- end-to-end alert latencija

## 4. Rezultati Po Scenarijima

### 4.1 Scenario A: Massive Sensor Ingestion

Scenario A meri kako se sistem ponasa pri paralelnom radu `100`, `1000` i `10000` uredjaja. Kompletna tabela svih profila je:

| Broker | Config | Devices | Partitions | Loss % | Producer msg/s | Consumer msg/s | p95 ms | CPU % | RAM MB | Network MB | Lag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| kafka | kafka_acks_0_partitions_1_devices_100 | 100 | 1 | 0.000 | 99.771 | 76.161 | 4.000 | 53.324 | 340.938 | 0.562 | 0.000 |
| kafka | kafka_acks_1_partitions_1_devices_100 | 100 | 1 | 0.000 | 99.940 | 76.617 | 7.000 | 93.880 | 337.652 | 2.452 | 0.000 |
| kafka | kafka_acks_all_partitions_1_devices_100 | 100 | 1 | 0.000 | 99.930 | 75.580 | 16.000 | 92.136 | 340.184 | 2.590 | 0.000 |
| kafka | kafka_acks_0_partitions_4_devices_100 | 100 | 4 | 0.000 | 99.870 | 77.214 | 3.000 | 105.456 | 347.461 | 2.978 | 0.000 |
| kafka | kafka_acks_1_partitions_4_devices_100 | 100 | 4 | 0.000 | 99.880 | 75.529 | 8.000 | 115.818 | 347.516 | 2.904 | 0.000 |
| kafka | kafka_acks_all_partitions_4_devices_100 | 100 | 4 | 0.000 | 99.930 | 75.643 | 18.000 | 113.346 | 349.836 | 2.818 | 0.000 |
| kafka | kafka_acks_0_partitions_8_devices_100 | 100 | 8 | 0.000 | 99.950 | 76.272 | 3.000 | 57.982 | 322.742 | 0.595 | 0.000 |
| kafka | kafka_acks_1_partitions_8_devices_100 | 100 | 8 | 0.000 | 99.860 | 75.523 | 14.000 | 129.261 | 347.027 | 2.974 | 0.000 |
| kafka | kafka_acks_all_partitions_8_devices_100 | 100 | 8 | 0.000 | 99.940 | 77.095 | 19.000 | 108.623 | 353.375 | 3.362 | 0.000 |
| kafka | kafka_acks_0_partitions_1_devices_1000 | 1000 | 1 | 0.000 | 998.104 | 749.738 | 4.000 | 294.320 | 415.285 | 10.504 | 0.000 |
| kafka | kafka_acks_1_partitions_1_devices_1000 | 1000 | 1 | 0.000 | 998.602 | 749.569 | 23.000 | 235.132 | 437.859 | 13.023 | 0.000 |
| kafka | kafka_acks_all_partitions_1_devices_1000 | 1000 | 1 | 0.000 | 997.606 | 756.372 | 29.000 | 249.872 | 439.020 | 12.841 | 0.000 |
| kafka | kafka_acks_0_partitions_4_devices_1000 | 1000 | 4 | 0.000 | 997.606 | 752.219 | 4.000 | 287.733 | 452.492 | 13.797 | 0.000 |
| kafka | kafka_acks_1_partitions_4_devices_1000 | 1000 | 4 | 0.000 | 998.104 | 752.615 | 23.000 | 297.012 | 476.402 | 13.457 | 0.000 |
| kafka | kafka_acks_all_partitions_4_devices_1000 | 1000 | 4 | 0.000 | 998.303 | 760.456 | 35.000 | 103.967 | 342.668 | 3.086 | 0.000 |
| kafka | kafka_acks_0_partitions_8_devices_1000 | 1000 | 8 | 0.000 | 998.104 | 761.093 | 4.000 | 268.694 | 461.988 | 12.494 | 0.000 |
| kafka | kafka_acks_1_partitions_8_devices_1000 | 1000 | 8 | 0.000 | 998.303 | 772.618 | 19.000 | 271.705 | 457.176 | 15.191 | 0.000 |
| kafka | kafka_acks_all_partitions_8_devices_1000 | 1000 | 8 | 0.000 | 998.901 | 751.089 | 37.000 | 288.862 | 457.355 | 14.626 | 0.000 |
| kafka | kafka_acks_0_partitions_1_devices_10000 | 10000 | 1 | 0.000 | 9983.029 | 7637.086 | 79.000 | 97.785 | 349.039 | 24.322 | 0.000 |
| kafka | kafka_acks_1_partitions_1_devices_10000 | 10000 | 1 | 0.000 | 9963.136 | 7370.826 | 376.000 | 264.943 | 404.379 | 66.662 | 0.000 |
| kafka | kafka_acks_all_partitions_1_devices_10000 | 10000 | 1 | 0.000 | 9954.211 | 7607.455 | 457.000 | 58.850 | 326.199 | 14.398 | 0.000 |
| kafka | kafka_acks_0_partitions_4_devices_10000 | 10000 | 4 | 0.000 | 9982.032 | 6790.249 | 43.000 | 226.340 | 377.578 | 51.949 | 0.000 |
| kafka | kafka_acks_1_partitions_4_devices_10000 | 10000 | 4 | 0.000 | 9978.048 | 5529.750 | 47.000 | 305.054 | 465.328 | 71.375 | 0.000 |
| kafka | kafka_acks_all_partitions_4_devices_10000 | 10000 | 4 | 0.000 | 9980.040 | 7544.323 | 136.000 | 308.365 | 469.418 | 73.693 | 0.000 |
| kafka | kafka_acks_0_partitions_8_devices_10000 | 10000 | 8 | 0.000 | 9980.040 | 7209.805 | 15.000 | 283.867 | 481.125 | 70.545 | 0.000 |
| kafka | kafka_acks_1_partitions_8_devices_10000 | 10000 | 8 | 0.000 | 9987.017 | 7505.817 | 40.000 | 274.529 | 474.023 | 72.199 | 0.000 |
| kafka | kafka_acks_all_partitions_8_devices_10000 | 10000 | 8 | 0.000 | 9964.129 | 7784.524 | 69.000 | 206.192 | 364.797 | 37.502 | 0.000 |
| mqtt | mqtt_qos_0_devices_100 | 100 | - | 0.000 | 71.403 | 48.537 | 23.592 | 1.663 | 1.180 | 2.861 | - |
| mqtt | mqtt_qos_1_devices_100 | 100 | - | 0.000 | 66.689 | 44.411 | 24.130 | 2.300 | 1.469 | 2.935 | - |
| mqtt | mqtt_qos_2_devices_100 | 100 | - | 0.000 | 71.674 | 48.379 | 48.209 | 4.021 | 1.996 | 3.429 | - |
| mqtt | mqtt_qos_0_devices_1000 | 1000 | - | 0.000 | 606.502 | 404.089 | 96.183 | 10.706 | 5.297 | 26.948 | - |
| mqtt | mqtt_qos_1_devices_1000 | 1000 | - | 0.000 | 707.714 | 477.418 | 479.335 | 25.501 | 8.359 | 29.820 | - |
| mqtt | mqtt_qos_2_devices_1000 | 1000 | - | 0.000 | 689.227 | 446.688 | 498.613 | 45.784 | 13.738 | 35.576 | - |
| mqtt | mqtt_qos_0_devices_10000 | 10000 | - | 0.000 | 3700.414 | 2954.908 | 2727.876 | 99.689 | 53.715 | 202.762 | - |
| mqtt | mqtt_qos_1_devices_10000 | 10000 | - | 98.944 | 3893.475 | 27.487 | 14739.130 | 102.220 | 45.262 | 84.071 | - |
| mqtt | mqtt_qos_2_devices_10000 | 10000 | - | 98.960 | 3109.840 | 22.561 | 19745.846 | 100.763 | 63.020 | 111.911 | - |

#### Zakljucci Scenario A

- `Kafka` drzi `0%` loss kroz sve profile, bez obzira na `acks`, broj uredjaja i broj particija.
- Kod `Kafka` se vidi da rast broja uredjaja uglavnom povecava throughput skoro linearno do `10000` uredjaja, dok `p95` i RAM rastu, ali ne dolazi do raspada sistema.
- `MQTT` je vrlo lagana pri `100` i `1000` uredjaja i u svim tim profilima ima `0%` loss, sto potvrduje da je pogodna za manje edge topologije.
- Problem se javlja na `10000` uredjaja. `QoS 0` i dalje prolazi bez loss-a, ali uz znatno manji throughput od Kafke i mnogo veci `p95` od gotovo svih Kafka profila.
- `MQTT QoS 1` i `QoS 2` na `10000` uredjaja prakticno kolabiraju: consumer throughput pada na `27.487` i `22.561 msg/s`, dok `p95` raste na `14739.130 ms` i `19745.846 ms`.
- Iz kompletne tabele se jasno vidi osnovni trade-off: `MQTT` je laksa, ali `Kafka` mnogo bolje skaluje kada broj uredjaja i obim poruka ozbiljno porastu.

### 4.2 Scenario B: Edge Connectivity Failures

Scenario B meri ponasanje sistema tokom `30s` mreznog prekida i oporavak posle reconnect-a. Kompletna tabela svih profila je:

| Mode | Broker | Config | Partitions | Publish msg/s | Storage msg/s | p95 ms | CPU % | RAM MB | Loss % | Ready s | First Analytics s | Max Lag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| app_buffered | kafka | app_buffered_kafka_acks_0_partitions_1_outage | 1 | 124.877 | 124.877 | 108053.014 | 42.437 | 506.688 | 0.000 | 0.014 | 9.470 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_1_partitions_1_outage | 1 | 123.798 | 123.798 | 109029.567 | 41.637 | 537.750 | 0.000 | 0.006 | 9.896 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_all_partitions_1_outage | 1 | 125.636 | 125.636 | 113139.231 | 51.000 | 565.258 | 0.000 | 0.005 | 11.370 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_0_partitions_4_outage | 4 | 125.418 | 125.418 | 111590.121 | 82.153 | 931.586 | 0.000 | 0.013 | 9.204 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_1_partitions_4_outage | 4 | 124.037 | 124.037 | 110394.869 | 96.854 | 884.828 | 0.000 | 0.006 | 9.585 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_all_partitions_4_outage | 4 | 124.617 | 124.617 | 112584.966 | 106.908 | 867.246 | 0.000 | 0.004 | 10.130 | 21 |
| app_buffered | kafka | app_buffered_kafka_acks_0_partitions_8_outage | 8 | 130.272 | 130.272 | 120000.000 | 116.694 | 950.496 | 0.000 | 0.005 | 16.238 | 24 |
| app_buffered | kafka | app_buffered_kafka_acks_1_partitions_8_outage | 8 | 124.441 | 124.441 | 110844.025 | 139.023 | 941.844 | 0.000 | 0.005 | 9.557 | 16 |
| app_buffered | kafka | app_buffered_kafka_acks_all_partitions_8_outage | 8 | 123.090 | 123.090 | 107730.157 | 143.737 | 937.523 | 0.000 | 0.015 | 9.103 | 16 |
| app_buffered | mqtt | app_buffered_mqtt_qos_0_outage | - | 165.342 | 165.093 | 55606.319 | 9.585 | 5.984 | 0.151 | 25.425 | 0.575 | - |
| app_buffered | mqtt | app_buffered_mqtt_qos_1_outage | - | 188.062 | 188.062 | 71054.498 | 38.552 | 11.059 | 0.000 | 24.237 | 0.753 | - |
| app_buffered | mqtt | app_buffered_mqtt_qos_2_outage | - | 189.055 | 189.055 | 70095.600 | 54.042 | 18.215 | 0.000 | 23.268 | 0.653 | - |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_1_outage | 1 | 50.272 | 48.276 | 47427.000 | 101.105 | 338.512 | 3.970 | 6.074 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_1_partitions_1_outage | 1 | 50.304 | 50.304 | 47392.000 | 78.287 | 332.242 | 0.000 | 3.777 | 0.557 | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_all_partitions_1_outage | 1 | 50.778 | 50.778 | 45832.000 | 89.117 | 342.254 | 0.000 | 2.581 | 0.742 | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_4_outage | 4 | 50.280 | 48.289 | 47342.000 | 125.213 | 336.766 | 3.960 | 6.334 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_1_partitions_4_outage | 4 | 50.588 | 50.588 | 47160.000 | 107.249 | 336.949 | 0.000 | 3.745 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_all_partitions_4_outage | 4 | 50.844 | 50.844 | 48248.000 | 85.567 | 336.785 | 0.000 | 4.897 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_0_partitions_8_outage | 8 | 50.049 | 48.067 | 46434.000 | 93.971 | 344.535 | 3.960 | 4.865 | 0.567 | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_1_partitions_8_outage | 8 | 50.278 | 50.278 | 46503.000 | 120.917 | 341.641 | 0.000 | 3.945 | - | 0 |
| tool_benchmark | kafka | tool_benchmark_kafka_acks_all_partitions_8_outage | 8 | 51.111 | 51.111 | 46850.000 | 122.562 | 339.078 | 0.000 | 3.674 | - | 0 |
| tool_benchmark | mqtt | tool_benchmark_mqtt_qos_0_outage | - | 218.448 | 109.224 | 30000.000 | 23.925 | 4.906 | 50.000 | 0.219 | 0.632 | - |
| tool_benchmark | mqtt | tool_benchmark_mqtt_qos_1_outage | - | 30.181 | 18.958 | 30000.000 | 12.375 | 3.727 | 37.185 | 10.988 | - | - |
| tool_benchmark | mqtt | tool_benchmark_mqtt_qos_2_outage | - | 91.152 | 100.814 | 468.816 | 37.849 | 13.094 | 0.000 | 10.764 | 0.608 | - |

#### Zakljucci Scenario B

- U `tool_benchmark` modu se vidi cistiji broker-level recovery. Tu `MQTT QoS 0` ima najmanji overhead za reconnect, ali uz `50%` loss, sto je najgori rezultat u toj grupi.
- `MQTT QoS 2` pokazuje kako jace delivery garancije popravljaju pouzdanost: `0%` loss, ali uz veci `Ready s`, veci CPU i RAM.
- `Kafka acks=1` i `acks=all` u `tool_benchmark` modu drze `0%` loss kroz sve particije. `acks=0` je jedina Kafka konfiguracija u ovoj grupi sa malim, ali prisutnim loss-om od oko `3.96-3.97%`.
- `Kafka` je kroz ceo Scenario B znatno teza po resursima. Vec u tool modu ide na oko `332-345 MB` RAM, dok `MQTT` ostaje na oko `3.7-13.1 MB`.
- U `app_buffered` modu i `MQTT` i `Kafka` izgledaju bolje u pogledu loss-a jer se u rezultat ukljucuje i aplikaciono bufferovanje, ne samo broker mehanizam.
- `Kafka` u `app_buffered` modu pokazuje vrlo jasnu observability sliku kroz `Max Lag`, ali po cenu veoma visokog RAM-a, koji raste do `950.496 MB` pri `8` particija.
- Iz punog skupa profila vidi se da je za edge reconnect slucajeve `MQTT` prirodnija, dok je `Kafka` pogodnija kada su prioriteti kontrola, replay i detaljna vidljivost recovery-ja.

### 4.3 Scenario C: Burst Event Load

Scenario C simulira nagli skok opterecenja sa `50` na `5000 msg/s`, uz pracenje backlog-a, backpressure-a i recovery vremena. Kompletna tabela svih profila je:

| Broker | Config | Partitions | Warmup Storage msg/s | Burst Storage msg/s | p95 ms | CPU % | RAM MB | Peak Backlog | Recovery to Zero s | Peak Lag | Loss % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| kafka | kafka_acks_0_partitions_1 | 1 | 45.752 | 2933.735 | 150.000 | 111.915 | 442.359 | 0.000 | 1.693 | 355.000 | 0.000 |
| kafka | kafka_acks_1_partitions_1 | 1 | 49.993 | 2955.065 | 297.000 | 109.536 | 447.734 | 0.000 | 0.000 | 558.000 | 0.000 |
| kafka | kafka_acks_all_partitions_1 | 1 | 49.204 | 3026.901 | 789.000 | 98.760 | 441.871 | 0.000 | 0.000 | 311.000 | 0.000 |
| kafka | kafka_acks_0_partitions_4 | 4 | 48.803 | 2870.532 | 43.000 | 87.424 | 433.301 | 0.000 | 1.706 | 391.000 | 0.000 |
| kafka | kafka_acks_1_partitions_4 | 4 | 48.236 | 2962.759 | 213.000 | 91.100 | 429.367 | 0.000 | 1.693 | 365.000 | 0.000 |
| kafka | kafka_acks_all_partitions_4 | 4 | 48.859 | 2974.453 | 48.000 | 101.473 | 434.863 | 0.000 | 0.000 | 445.000 | 0.000 |
| kafka | kafka_acks_0_partitions_8 | 8 | 47.892 | 2792.030 | 42.000 | 85.273 | 453.562 | 0.000 | 1.690 | 415.000 | 0.000 |
| kafka | kafka_acks_1_partitions_8 | 8 | 46.531 | 3114.134 | 104.000 | 100.560 | 449.809 | 0.000 | 0.000 | 410.000 | 0.000 |
| kafka | kafka_acks_all_partitions_8 | 8 | 46.580 | 2684.398 | 111.000 | 105.772 | 457.988 | 0.000 | 0.000 | 370.000 | 0.000 |
| mqtt | mqtt_qos_0 | - | 45.846 | 1737.992 | 20.912 | 45.366 | 11.977 | 1.000 | 19.643 | 0.000 | 48.838 |
| mqtt | mqtt_qos_1 | - | 46.220 | 994.549 | 474.716 | 61.682 | 16.320 | 118.000 | 19.199 | 0.000 | 29.299 |
| mqtt | mqtt_qos_2 | - | 46.370 | 625.827 | 967.645 | 103.672 | 28.812 | 220.000 | 57.208 | 0.000 | 21.653 |

#### Zakljucci Scenario C

- `Kafka` drzi `0%` loss u svim burst profilima i to je najvazniji nalaz ove tabele.
- `Kafka` burst throughput ostaje veoma visok, u rasponu od `2684.398` do `3114.134 msg/s`, a pritisak sistema ne vidi se kroz loss vec kroz `Peak Lag`.
- `Peak Backlog` kod Kafka profila ostaje `0`, sto znaci da se backlog ne akumulira u aplikacionim queue-ovima, vec se opterecenje vidi kroz broker-side lag.
- Kod `MQTT` se vidi vrlo jasan trade-off izmedju pouzdanosti i latencije:
  - `QoS 0`: najbolji `p95` od `20.912 ms`, ali i najgori loss od `48.838%`
  - `QoS 1`: manji loss od `29.299%`, ali mnogo veci `p95` i backlog
  - `QoS 2`: najmanji loss u MQTT grupi, ali najgori CPU, najveci backlog i najduzi recovery
- `MQTT QoS 2` je najskuplja MQTT konfiguracija u ovom scenariju: `103.672%` CPU, `220` peak backlog i `57.208s` do oporavka na nulu.
- Iz celog skupa profila Scenario C najjasnije proizilazi da je `Kafka` bolji izbor kada je bitna burst stabilnost, dok je `MQTT` prikladniji samo kada je prioritet mali footprint i kada se moze prihvatiti veci gubitak ili veca latencija.

### 4.4 Scenario D: Real-Time Alerting

Scenario D meri end-to-end latenciju od trenutka generisanja kriticne vrednosti do emitovanja alarma iz `Analytics Service`. Kompletna tabela svih profila je:

| Broker | Config | Mode | Partitions | Alert Avg ms | Alert p95 ms | Alert Max ms | CPU % | RAM MB | Network MB | Peak Lag | Successful Repeats |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| kafka | kafka_acks_0_partitions_1_window_early | early | 1 | 9847.667 | 9848.000 | 9848.000 | 34.186 | 310.027 | 0.053 | 0.000 | 3/3 |
| kafka | kafka_acks_0_partitions_1_window_late | late | 1 | 11198.000 | 11200.000 | 11200.000 | 35.513 | 307.879 | 0.051 | 0.000 | 3/3 |
| kafka | kafka_acks_1_partitions_1_window_early | early | 1 | 9849.667 | 9851.000 | 9851.000 | 43.854 | 305.160 | 0.058 | 0.000 | 3/3 |
| kafka | kafka_acks_1_partitions_1_window_late | late | 1 | 11197.000 | 11198.000 | 11198.000 | 34.324 | 308.879 | 0.048 | 0.000 | 3/3 |
| kafka | kafka_acks_all_partitions_1_window_early | early | 1 | 9847.000 | 9848.000 | 9848.000 | 34.888 | 310.199 | 0.054 | 0.000 | 3/3 |
| kafka | kafka_acks_all_partitions_1_window_late | late | 1 | 11197.333 | 11198.000 | 11198.000 | 38.107 | 311.219 | 0.046 | 0.000 | 3/3 |
| mqtt | mqtt_qos_0_window_early | early | - | 6638.000 | 7759.000 | 7759.000 | 0.084 | 0.695 | 0.006 | 0.000 | 3/3 |
| mqtt | mqtt_qos_0_window_late | late | - | 7985.667 | 9307.000 | 9307.000 | 0.099 | 0.707 | 0.006 | 0.000 | 3/3 |
| mqtt | mqtt_qos_1_window_early | early | - | 6708.667 | 7678.000 | 7678.000 | 0.134 | 0.703 | 0.007 | 0.000 | 3/3 |
| mqtt | mqtt_qos_1_window_late | late | - | 8126.000 | 9027.000 | 9027.000 | 0.098 | 0.699 | 0.005 | 0.000 | 3/3 |
| mqtt | mqtt_qos_2_window_early | early | - | 6796.667 | 7595.000 | 7595.000 | 0.119 | 0.699 | 0.007 | 0.000 | 3/3 |
| mqtt | mqtt_qos_2_window_late | late | - | 8076.667 | 9180.000 | 9180.000 | 0.170 | 0.707 | 0.008 | 0.000 | 3/3 |

#### Zakljucci Scenario D

- U svim profilima `MQTT` ima manju alert latenciju od `Kafka`.
- Kod `MQTT` se jasno vidi efekat tumbling window-a:
  - `early` mod daje alert oko `6.6-6.8s`
  - `late` mod oko `8.0-8.1s`
- Kod `Kafka` je isti obrazac prisutan, ali pomeren na vise vrednosti:
  - `early` oko `9.8s`
  - `late` oko `11.2s`
- Izmedju samih `QoS` i `acks` konfiguracija razlike su male. To znaci da u ovom scenariju broker nije glavni izvor kasnjenja; dominantan faktor je `10s` tumbling window logika u analytics sloju.
- Razlika u resursima je ogromna:
  - `MQTT`: oko `0.695-0.707 MB` RAM
  - `Kafka`: oko `305-311 MB` RAM
- Scenario D zato vrlo jasno podrzava tezu da je `MQTT` prirodnija za edge alerting, dok `Kafka` ostaje smislenija kada je alerting samo deo sireg cloud data pipeline-a.

## 5. Odgovori na Inzenjerska Pitanja

### 5.1 Zasto je MQTT idealna za edge, a zasto nije pogodna za istorijsku analitiku velikih podataka?

Na osnovu kompletnih tabela, `MQTT` je veoma pogodna za edge zato sto ima izuzetno mali resursni footprint:
- u Scenario D trosi oko `0.7 MB` RAM
- u Scenario A na `100` uredjaja trosi oko `1.180-1.996 MB` RAM
- u Scenario B tool modu trosi `3.727-13.094 MB` RAM

To je red velicine mnogo manje od Kafke, koja se kroz scenarije krece od oko `305 MB` do preko `950 MB` RAM.

Medjutim, potpuni skup profila takodje pokazuje da `MQTT` postaje ogranicavajuca kada je potrebna velika skala i jaka pouzdanost:
- u Scenario A pri `10000` uredjaja i `QoS 1/2` dolazi do oko `99%` gubitka
- u Scenario C ili gubi mnogo poruka (`QoS 0`) ili dramaticno povecava latenciju, backlog i recovery vreme (`QoS 1/2`)

Zato je MQTT odlicna za edge ingest i laki alerting, ali nije dobar izbor kada sistem zahteva veliku skalu, replay-friendly obradu i strogu kontrolu backlog-a i istorije podataka.

### 5.2 Zasto Kafka dominira u data-intensive cloud sistemima i koja je cena njene skalabilnosti?

Kompletne tabele vrlo jasno pokazuju zasto `Kafka` dominira u data-intensive cloud sistemima:
- u Scenario A drzi `0%` loss i gotovo `10k msg/s` producer throughput na `10000` uredjaja
- u Scenario C drzi `0%` loss u svim burst profilima, uz `2.7k-3.1k msg/s` burst storage throughput
- u Scenario B daje jasan `Max Lag` signal i time omogucava observability recovery-ja

Cena te skalabilnosti je velika potrosnja resursa:
- Scenario A: cesto `340-480 MB` RAM
- Scenario C: oko `429-458 MB` RAM
- Scenario D: oko `305-311 MB` RAM
- Scenario B app mode: kod vise particija ide i preko `900 MB` RAM

Dakle, `Kafka` je odlican izbor za cloud i data-intensive sisteme, ali nije prirodan izbor za hardverski ogranicene edge servere. Na edge-u je realna samo na jacim gateway uredjajima i uz prihvatanje veceg CPU/RAM troska.

### 5.3 Uporedna tabela performansi

Za zavrsno poredjenje najkorisnije je gledati profile koji najbolje reprezentuju ponasanje oba brokera u svakom scenariju:

| Scenario | MQTT reprezentativno | Kafka reprezentativno | Zakljucak |
| --- | --- | --- | --- |
| A | `QoS 0`, `10000` uredjaja: `3700.414 msg/s`, `p95 2727.876 ms`, `99.689% CPU`, `53.715 MB RAM`, `0% loss` | `acks=1`, `8` particija, `10000` uredjaja: `9987.017 msg/s`, `p95 40 ms`, `274.529% CPU`, `474.023 MB RAM`, `0% loss` | Kafka je mnogo bolja za masovni ingest. |
| B | `QoS 2`, tool mode: `100.814 msg/s`, `p95 468.816 ms`, `37.849% CPU`, `13.094 MB RAM`, `0% loss` | `acks=1`, tool mode, `1` particija: `50.304 msg/s`, `p95 47392 ms`, `78.287% CPU`, `332.242 MB RAM`, `0% loss` | MQTT je laksa, Kafka daje bolju observability sliku. |
| C | `QoS 1`: `994.549 msg/s`, `p95 474.716 ms`, `61.682% CPU`, `16.320 MB RAM`, `29.299% loss` | `acks=1`, `8` particija: `3114.134 msg/s`, `p95 104 ms`, `100.560% CPU`, `449.809 MB RAM`, `0% loss` | Kafka bolje podnosi burst i backlog. |
| D | `QoS 0`, early: `p95 7759 ms`, `0.084% CPU`, `0.695 MB RAM` | `acks=1`, early: `p95 9851 ms`, `43.854% CPU`, `305.160 MB RAM` | MQTT je bolja za edge alerting. |

## 6. Opsti Zakljucak

Na osnovu kompletnih performance tabela za sva cetiri scenarija, moze se izvesti vrlo jasan opsti zakljucak:

- `MQTT` je bolja kada su prioritet:
  - mali CPU/RAM footprint
  - jednostavan deployment
  - edge ingest i laksi real-time alerting

- `Kafka` je bolja kada su prioritet:
  - skaliranje na veliki broj uredjaja
  - visok throughput
  - minimalan loss pod opterecenjem
  - observability kroz lag i particionisanje
  - cloud i data-intensive obrada

Najvazniji trade-off ovog projekta je sledeci:
- `MQTT` nudi malu cenu po pitanju resursa, ali pod velikim opterecenjem i jacim delivery garancijama brzo dolazi do ogranicenja
- `Kafka` donosi mnogo vecu pouzdanost i skalabilnost, ali uz vrlo visoku cenu u `CPU` i `RAM` potrosnji

Zbog toga je najispravnija inzenjerska preporuka:
- koristiti `MQTT` blize uredjajima i edge sloju
- koristiti `Kafka` u centralnom cloud sloju kada su potrebni throughput, replay, pouzdanost i analitika velikih tokova podataka

## 7. Izvori Eksperimentalnih Podataka

Ovaj izvestaj je sastavljen na osnovu sledecih tabela:
- [scenario_a_results_full_performance_table.md](../benchmarks/scenario_a_results_full_performance_table.md)
- [scenario_b_results_full_performance_table.md](../benchmarks/scenario_b_results_full_performance_table.md)
- [scenario_c_results_full_performance_table.md](../benchmarks/scenario_c_results_full_performance_table.md)
- [scenario_d_results_full_performance_table.md](../benchmarks/scenario_d_results_full_performance_table.md)
