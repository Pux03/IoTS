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
docker compose up -d --build
```

Korisni endpoint-i:
- `http://localhost:8000/health` - data-ingestion
- `http://localhost:8001/health` - data-storage
- `http://localhost:8002/health` - analytics-service
- `http://localhost:3000` - Grafana
- `http://localhost:9090` - Prometheus

Gasenje staka:

```bash
docker compose down --remove-orphans
```

## Pokretanje Benchmark Scenarija

Detaljnija dokumentacija benchmark runner-a je u:
- `benchmarks/README.md`

### Scenario A

Massive ingest, `100 / 1000 / 10000` uredjaja, MQTT `QoS 0/1/2`, Kafka `acks=0/1/all`, Kafka particije `1/4/8`.

```bash
python benchmarks/run_scenario_a.py
```

Ako vec postoji finalni JSON i treba samo regenerisati tabelu i analysis:

```bash
python benchmarks/run_scenario_a.py --results-file benchmarks/scenario_a_results_full.json --artifacts-only
```

### Scenario B

Outage / reconnect test sa `docker network disconnect`, MQTT i Kafka recovery, tool i app-buffered mod.

```bash
python benchmarks/run_scenario_b.py
```

### Scenario C

Burst opterecenje `50 -> 5000 msg/s`, backlog, backpressure i recovery.

```bash
python benchmarks/run_scenario_c.py
```

### Scenario D

Real-time alerting benchmark, alert latencija i resursni footprint.

```bash
python benchmarks/run_scenario_d.py
```

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
