# IoT Mikroservisi - MQTT vs Kafka Evaluacija

Projekat 2 iz predmeta `Internet stvari i servisa`.

Repo sadrzi asinhroni, event-driven IoT mikroservisni sistem koji radi nad dva brokera:
- `MQTT (Mosquitto)`
- `Apache Kafka` u `KRaft` rezimu

Sistem je namenjen za eksperimentalno poredjenje ova dva pristupa kroz cetiri scenarija:
- `Scenario A`: Massive Sensor Ingestion
- `Scenario B`: Edge Connectivity Failures
- `Scenario C`: Burst Event Load
- `Scenario D`: Real-Time Alerting

## Arhitektura

Sistem se sastoji od sledecih komponenti:

1. `Data Ingestion Service` (`Python / FastAPI`)
   - simulira IoT uredjaje
   - publikuje poruke na MQTT ili Kafka broker
   - podrzava `MQTT QoS 0/1/2` i `Kafka acks=0/1/all`
   - iznosi health i metrike

2. `Data Storage Service` (`Node.js / Express`)
   - cita poruke sa brokera
   - upisuje ih u `PostgreSQL`
   - koristi batching (`500` poruka) i podrzava `DISABLE_DB_WRITE=true`

3. `Analytics Service` (`Node.js / Express`)
   - radi `10s` tumbling window obradu
   - racuna prosecnu temperaturu
   - podize alert kada prosek predje prag
   - za Scenario D meri end-to-end alert latenciju

4. Prateci sloj
   - `PostgreSQL`
   - `Prometheus`
   - `Grafana`
   - `resource-monitor` za CPU/RAM/network metrike preko Docker stats API-ja

## Struktura Projekta

```text
project_2/
|-- docker-compose.yml
|-- README.md
|-- IoTS - Projekat 2.pdf
|-- quick_scenario_A.py
|-- quick_scenario_B.py
|-- quick_scenario_C.py
|-- quick_scenario_D.py
|-- analytics-service/
|-- benchmarks/
|   |-- README.md
|   |-- run_all_scenarios.py
|   |-- run_protocol_benchmarks.py
|   |-- run_scenario_a.py
|   |-- run_scenario_b.py
|   |-- run_scenario_c.py
|   |-- run_scenario_d.py
|   |-- scenario_a_results_full.json
|   |-- scenario_a_results_full_performance_table.md
|   |-- scenario_a_results_full_analysis.md
|   |-- scenario_b_results_full.json
|   |-- scenario_b_results_full_performance_table.md
|   |-- scenario_b_results_full_analysis.md
|   |-- scenario_c_results_full.json
|   |-- scenario_c_results_full_performance_table.md
|   |-- scenario_c_results_full_analysis.md
|   |-- scenario_d_results_full.json
|   |-- scenario_d_results_full_performance_table.md
|   |-- scenario_d_results_full_analysis.md
|-- config/
|-- data-ingestion/
|-- data-storage/
|-- db/
|   `-- init.sql
|-- full_report/
|   |-- tehnicki_izvestaj.md
|   `-- http_presentation/
|       |-- index.html
|       |-- styles.css
|       |-- script.js
|       `-- README.md
|-- report/
|   `-- technical_report.md
`-- resource-monitor/
```

## Zahtevi

Za rad su potrebni:
- `Docker Desktop` ili ekvivalentan Docker runtime
- `docker compose` (Compose V2)
- `Python 3`
- po potrebi `Node.js` za lokalne provere, iako se servisi izvrsavaju kroz kontejnere

## Pokretanje Sistemskog Staka

Iz korena projekta:

```bash
docker compose up -d
```

Korisni endpoint-i:
- `http://localhost:8000/health` - data-ingestion
- `http://localhost:8001/health` - data-storage
- `http://localhost:8002/health` - analytics-service

Gasenje staka:

```bash
docker compose down --remove-orphans
```

Ako treba i monitoring UI:

```bash
docker compose --profile monitoring up -d
```

Tada su dostupni i:
- `http://localhost:3000` - Grafana
- `http://localhost:9090` - Prometheus

Gasenje monitoring varijante radi sa istim profilom:

```bash
docker compose --profile monitoring down --remove-orphans
```

Napomena:
- ne mesati `docker compose --profile monitoring up -d` sa obicnim `docker compose down`
- ako se to uradi, mogu ostati stari `prometheus` i `grafana` kontejneri vezani za obrisanu Docker mrezu

## Pokretanje Benchmark Scenarija

Detaljnija dokumentacija benchmark runner-a je u:
- `benchmarks/README.md`

Metodologija merenja u ovom projektu:
- finalni runner-i u `benchmarks/` predstavljaju glavnu, projektnu varijantu eksperimenta
- za MQTT opterecenje koristi se `emqtt-bench`
- za Kafka opterecenje koristi se `kafka-producer-perf-test.sh`
- CPU/RAM/network metrike prikuplja `resource-monitor` preko Docker stats API-ja, tj. istog izvora kao `docker stats`
- u stres scenarijima `A` i `C` storage radi sa batching pristupom ili sa iskljucenim DB upisom da PostgreSQL ne postane glavno usko grlo
- `quick_scenario_*.py` skripte su skraceni CLI sanity/demo runner-i i nisu zamena za finalne benchmark artefakte iz foldera `benchmarks/`

### Scenario A

Massive ingest, `100 / 1000 / 10000` uredjaja, MQTT `QoS 0/1/2`, Kafka `acks=0/1/all`, Kafka particije `1/4/8`.

```bash
python benchmarks/run_scenario_a.py
```

Za kracu demonstraciju postoji i konfigurabilni CLI runner:
- `quick_scenario_A.py`
- radi tacno `jedan run po profilu`
- ne generise JSON/Markdown artefakte
- prikazuje rezultate samo u terminalu
- koristi benchmark alat direktno nad brokerom, pa je namenjen za kratak sanity check / prezentaciju

Podrazumevano pokretanje:

```bash
python quick_scenario_A.py
```

Korisni primeri:

```bash
python quick_scenario_A.py --broker mqtt --mqtt-qos 0 1 2
python quick_scenario_A.py --broker kafka --kafka-acks 0 1 all --kafka-partitions 1
python quick_scenario_A.py --broker both --devices 50 --duration-sec 1
python quick_scenario_A.py --build-images
```

Najbitnije opcije:
- `--broker mqtt|kafka|both`
- `--mqtt-qos ...`
- `--kafka-acks ...`
- `--kafka-partitions ...`
- `--devices ...`
- `--interval-sec ...`
- `--duration-sec ...`
- `--max-wait-sec ...`
- `--keep-stack-up`
- `--verbose`

Primeri ukratko:
- `python quick_scenario_A.py`
  - pokrece podrazumevani kratki Scenario A sanity check za MQTT i Kafka profile
- `python quick_scenario_A.py --broker mqtt --mqtt-qos 0 1 2`
  - testira samo MQTT profile sa QoS `0/1/2`
- `python quick_scenario_A.py --broker kafka --kafka-acks 0 1 all --kafka-partitions 1`
  - testira samo Kafka profile sa `acks=0/1/all` i jednom particijom
- `python quick_scenario_A.py --broker both --devices 50 --duration-sec 1`
  - dodatno skracuje demo tako sto koristi manji broj uredjaja i krace trajanje
- `python quick_scenario_A.py --build-images`
  - pre prvog pokretanja rebuild-uje slike servisa koje su potrebne quick runner-u

Ako vec postoji finalni JSON i treba samo regenerisati tabelu i analysis:

```bash
python benchmarks/run_scenario_a.py --results-file benchmarks/scenario_a_results_full.json --artifacts-only
```

### Scenario B

Outage / reconnect test sa `docker network disconnect`, MQTT i Kafka recovery, tool i app-buffered mod.

```bash
python benchmarks/run_scenario_b.py
```

Za kracu demonstraciju postoji i konfigurabilni CLI runner:
- `quick_scenario_B.py`
- radi tacno `jedan run po profilu`
- ne generise JSON/Markdown artefakte
- prikazuje rezultate samo u terminalu
- po default-u koristi `tool_benchmark` mod i kratke warmup/outage/recovery prozore

Podrazumevano pokretanje:

```bash
python quick_scenario_B.py
```

Korisni primeri:

```bash
python quick_scenario_B.py --broker mqtt --mqtt-qos 0 1 2
python quick_scenario_B.py --broker kafka --kafka-acks 1 all --kafka-partitions 1
python quick_scenario_B.py --modes tool_benchmark app_buffered --mqtt-qos 0
python quick_scenario_B.py --outage-sec 30 --warmup-sec 5 --post-reconnect-run-sec 15
python quick_scenario_B.py --build-images
```

Najbitnije opcije:
- `--broker mqtt|kafka|both`
- `--modes tool_benchmark|app_buffered`
- `--mqtt-qos ...`
- `--kafka-acks ...`
- `--kafka-partitions ...`
- `--devices ...`
- `--interval-sec ...`
- `--warmup-sec ...`
- `--outage-sec ...`
- `--post-reconnect-run-sec ...`
- `--max-wait-sec ...`
- `--keep-stack-up`
- `--verbose`

Primeri ukratko:
- `python quick_scenario_B.py`
  - pokrece kratki outage/reconnect sanity check za podrazumevane MQTT i Kafka profile
- `python quick_scenario_B.py --broker mqtt --mqtt-qos 0 1 2`
  - testira samo MQTT outage profile za QoS `0/1/2`
- `python quick_scenario_B.py --broker kafka --kafka-acks 1 all --kafka-partitions 1`
  - testira samo Kafka outage profile sa izabranim `acks` vrednostima
- `python quick_scenario_B.py --modes tool_benchmark app_buffered --mqtt-qos 0`
  - pored brzeg tool moda ukljucuje i `app_buffered` mod koji koristi `data-ingestion`
- `python quick_scenario_B.py --outage-sec 30 --warmup-sec 5 --post-reconnect-run-sec 15`
  - prilagodjava quick skriptu da vise lici na trajanje iz projektnog zadatka
- `python quick_scenario_B.py --build-images`
  - pre prvog pokretanja rebuild-uje slike servisa koje su potrebne quick runner-u

### Scenario C

Burst opterecenje `50 -> 5000 msg/s`, backlog, backpressure i recovery.

```bash
python benchmarks/run_scenario_c.py
```

Za kracu demonstraciju postoji i konfigurabilni CLI runner:
- `quick_scenario_C.py`
- radi tacno `jedan run po profilu`
- ne generise JSON/Markdown artefakte
- prikazuje rezultate samo u terminalu
- po default-u prati projektni zahtev i modeluje skok `50 -> 5000 msg/s`

Podrazumevano pokretanje:

```bash
python quick_scenario_C.py
```

Korisni primeri:

```bash
python quick_scenario_C.py --broker mqtt --mqtt-qos 0 1 2
python quick_scenario_C.py --broker kafka --kafka-acks 1 all --kafka-partitions 1
python quick_scenario_C.py --warmup-rate 50 --burst-rate 5000 --warmup-sec 2 --burst-sec 3 --recovery-sec 2
python quick_scenario_C.py --warmup-rate 20 --burst-rate 500 --warmup-sec 1 --burst-sec 1 --recovery-sec 1
python quick_scenario_C.py --build-images
```

Najbitnije opcije:
- `--broker mqtt|kafka|both`
- `--mqtt-qos ...`
- `--kafka-acks ...`
- `--kafka-partitions ...`
- `--warmup-rate ...`
- `--burst-rate ...`
- `--warmup-sec ...`
- `--burst-sec ...`
- `--recovery-sec ...`
- `--sample-interval-sec ...`
- `--kafka-lag-sample-interval-sec ...`
- `--drain-timeout-sec ...`
- `--max-wait-sec ...`
- `--keep-stack-up`
- `--verbose`

Primeri ukratko:
- `python quick_scenario_C.py`
  - pokrece podrazumevani Scenario C quick run sa skokom `50 -> 5000 msg/s`
- `python quick_scenario_C.py --broker mqtt --mqtt-qos 0 1 2`
  - testira samo MQTT burst profile za QoS `0/1/2`
- `python quick_scenario_C.py --broker kafka --kafka-acks 1 all --kafka-partitions 1`
  - testira samo Kafka burst profile sa izabranim `acks` vrednostima
- `python quick_scenario_C.py --warmup-rate 50 --burst-rate 5000 --warmup-sec 2 --burst-sec 3 --recovery-sec 2`
  - eksplicitno pokrece burst scenario koji odgovara projektnom opisu
- `python quick_scenario_C.py --warmup-rate 20 --burst-rate 500 --warmup-sec 1 --burst-sec 1 --recovery-sec 1`
  - koristi mnogo laksi demo profil za brzu prezentaciju ili lokalnu proveru
- `python quick_scenario_C.py --build-images`
  - pre prvog pokretanja rebuild-uje slike servisa koje su potrebne quick runner-u

### Scenario D

Real-time alerting benchmark, alert latencija i resursni footprint.

```bash
python benchmarks/run_scenario_d.py
```

Za kracu demonstraciju postoji i konfigurabilni CLI runner:
- `quick_scenario_D.py`
- radi tacno `jedan run po profilu`
- ne generise JSON/Markdown artefakte
- prikazuje rezultate samo u terminalu
- meri end-to-end vreme od slanja kriticne vrednosti do trenutka kada `Analytics Service` ispise alert
- po default-u koristi `early` window mod radi stabilnijeg i brzeg demo pokretanja

Podrazumevano pokretanje:

```bash
python quick_scenario_D.py
```

Korisni primeri:

```bash
python quick_scenario_D.py --broker mqtt --mqtt-qos 0 1 2
python quick_scenario_D.py --broker kafka --kafka-acks 1 all --kafka-partitions 1
python quick_scenario_D.py --window-modes early late
python quick_scenario_D.py --critical-count 3 --min-launch-lead-ms 5000
python quick_scenario_D.py --build-images
```

Najbitnije opcije:
- `--broker mqtt|kafka|both`
- `--mqtt-qos ...`
- `--kafka-acks ...`
- `--kafka-partitions ...`
- `--window-modes early|late`
- `--critical-count ...`
- `--early-after-flush-ms ...`
- `--late-before-flush-ms ...`
- `--min-launch-lead-ms ...`
- `--max-wait-sec ...`
- `--keep-stack-up`
- `--verbose`

Primeri ukratko:
- `python quick_scenario_D.py`
  - pokrece podrazumevani Scenario D quick run za MQTT i Kafka profile
- `python quick_scenario_D.py --broker mqtt --mqtt-qos 0 1 2`
  - testira samo MQTT alert profile za QoS `0/1/2`
- `python quick_scenario_D.py --broker kafka --kafka-acks 1 all --kafka-partitions 1`
  - testira samo Kafka alert profile sa izabranim `acks` vrednostima
- `python quick_scenario_D.py --window-modes early late`
  - izvrsava oba nacina pozicioniranja kriticnih poruka unutar `10s` tumbling window-a
- `python quick_scenario_D.py --critical-count 3 --min-launch-lead-ms 5000`
  - eksplicitno podesava broj kriticnih poruka i minimalni vremenski lead pre planiranog slanja
- `python quick_scenario_D.py --build-images`
  - pre prvog pokretanja rebuild-uje slike servisa koje su potrebne quick runner-u

## Rezultati i Artefakti

Za svaki scenario postoje tri glavna artefakta:
- `scenario_*_results_full.json`
- `scenario_*_results_full_performance_table.md`
- `scenario_*_results_full_analysis.md`

To znaci da trenutno postoje:
- `benchmarks/scenario_a_results_full.json`
- `benchmarks/scenario_a_results_full_performance_table.md`
- `benchmarks/scenario_a_results_full_analysis.md`
- `benchmarks/scenario_b_results_full.json`
- `benchmarks/scenario_b_results_full_performance_table.md`
- `benchmarks/scenario_b_results_full_analysis.md`
- `benchmarks/scenario_c_results_full.json`
- `benchmarks/scenario_c_results_full_performance_table.md`
- `benchmarks/scenario_c_results_full_analysis.md`
- `benchmarks/scenario_d_results_full.json`
- `benchmarks/scenario_d_results_full_performance_table.md`
- `benchmarks/scenario_d_results_full_analysis.md`

## Izvestaji

U repou postoje dve glavne tekstualne verzije izvestaja:

- `report/technical_report.md`
  - standardni tehnicki izvestaj zasnovan na finalnim rezultatima

- `full_report/tehnicki_izvestaj.md`
  - prosireni objedinjeni izvestaj koji koristi finalne performance tabele svih scenarija

## HTTP Prezentacija

U folderu:
- `full_report/http_presentation/`

nalazi se staticka web prezentacija projekta.

Pokretanje lokalno:

```bash
python -m http.server 8088 --directory full_report/http_presentation
```

Zatim otvoriti:

```text
http://localhost:8088
```

## Monitoring

Grafana dashboard i monitoring sloj sluze za vizuelni pregled:
- throughput metrika
- latencije
- CPU/RAM/network resursa
- storage i analytics brojaca

Grafana podrazumevano radi na:

```text
http://localhost:3000
```

Podrazumevani kredencijali:

```text
admin / admin
```

## Napomene

- Tokom stress testova visokog intenziteta, DB write moze biti iskljucen da PostgreSQL ne postane usko grlo umesto samog brokera.
- `run_all_scenarios.py` postoji kao legacy orchestrator, ali su za finalne eksperimente primarni namenski runner-i `run_scenario_a.py`, `run_scenario_b.py`, `run_scenario_c.py` i `run_scenario_d.py`.
- Ako se radi samo analiza i pisanje izvestaja, najvazniji ulazi su `scenario_*_results_full_performance_table.md` i `scenario_*_results_full_analysis.md`.
