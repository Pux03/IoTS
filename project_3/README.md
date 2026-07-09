# Projekat 3 - RFID IoT Sistem

Ovaj repozitorijum sada predstavlja pojednostavljen RFID IoT sistem za predmet `Internet stvari i servisa`.

Kratka operativna referenca sa endpoint-ima, topic-ima i tokom poruka nalazi se u:

- `docs/technical-reference.md`

Sistem koristi:

- `RFID-only` dogadjaje
- `MQTT (Mosquitto)` kao jedini broker
- `eKuiper` za jedan CEP alarm `UNAUTHORIZED_ACCESS`
- `MaaS` mikroservis za procenu rizika
- jednostavan web dashboard za pregled dogadjaja i alarma

Kafka, benchmark scenariji, temperaturni senzori, temperaturni alarmi i temperaturni ML model vise nisu deo Projekta 3.

## Arhitektura

```text
RFID Generator
        |
        | MQTT
        v
 MQTT Broker (Mosquitto)
        |
        +----------------+
        |                |
        v                v
Data Storage        eKuiper CEP
Service                |
                        v
                  rfid/alerts
                        |
                        v
                 Analytics Service
                        |
                        v
                  MaaS Service
                        |
                        v
                 Web Application
```

## MQTT topic-i

- RFID dogadjaji: `rfid/events`
- Alarmi: `rfid/alerts`
- Rezultati analitike: `rfid/analytics`

## Struktura projekta

```text
project_3/
|-- analytics/
|-- data-storage/
|-- db/
|-- docs/
|-- ekuiper/
|-- frontend/
|-- generator/
|-- maas/
|-- mqtt/
|-- docker-compose.yml
`-- README.md
```

MaaS sada prati odvojen ML workflow:

```text
maas/
|-- app.py
|-- generate_dataset.py
|-- train_model.py
|-- dataset.csv
|-- model.pkl
|-- requirements.txt
`-- Dockerfile
```

## Servisi

### `generator`

Python `FastAPI` servis koji:

- generise iskljucivo RFID dogadjaje
- publikuje ih na MQTT topic `rfid/events`
- izlaze `GET /health`, `GET /status` i `GET /metrics`

Primer dogadjaja:

```json
{
  "event_id": "12345",
  "timestamp": "2026-07-09T13:20:10Z",
  "device_id": "RFID-LAB-01",
  "card_uid": "A42F90B2",
  "access_granted": false,
  "door_id": "SERVER_ROOM",
  "zone": "SECOND_FLOOR",
  "signal_strength": -61,
  "battery_voltage": 3.58,
  "response_time_ms": 73,
  "event_type": "ACCESS_DENIED"
}
```

Dozvoljeni `event_type`:

- `ENTRY`
- `EXIT`
- `ACCESS_DENIED`
- `CARD_UNKNOWN`
- `FORCED_OPEN`

### `data-storage`

Node.js servis koji:

- subscribuje se na `rfid/events`
- upisuje RFID dogadjaje u PostgreSQL
- izlaze `GET /health`, `GET /status` i `GET /metrics`

### `ekuiper`

`eKuiper` je konfigurisan sa jednim pravilom:

- ako je `access_granted == false`
- generise alarm `UNAUTHORIZED_ACCESS`
- objavljuje ga na topic `rfid/alerts`

Napomena:

- zadrzano je opste pravilo za sva neodobrena RFID ocitavanja
- nije uvedeno dodatno ogranicenje po kriticnoj zoni zato sto je u specifikaciji `zone == "SERVER_ROOM"` u koliziji sa RFID modelom gde je `SERVER_ROOM` zapravo `door_id`, dok je zona npr. `SECOND_FLOOR`

### `analytics`

Node.js servis koji:

- prima `rfid/events`
- prima `rfid/alerts`
- vodi statistiku
- poziva MaaS za procenu rizika alarma
- objavljuje sazetak na `rfid/analytics`
- izlaze REST API za dashboard

Najvazniji endpoint-i:

- `GET /health`
- `GET /api/summary`
- `GET /api/events`
- `GET /api/alerts`
- `GET /api/dashboard`
- `GET /metrics`

### `maas`

Python `FastAPI` mikroservis sa `scikit-learn` modelom `RandomForestClassifier`.

Workflow:

- `generate_dataset.py` generise istorijski `dataset.csv` koriscenjem postojeceg RFID generatora
- `train_model.py` trenira model nad `dataset.csv` i cuva ga kao `model.pkl`
- `app.py` pri pokretanju samo ucitava `model.pkl` i koristi ga za inferenciju

Ulaz modela:

- `signal_strength`
- `response_time_ms`
- `battery_voltage`
- `zone`
- `door_id`
- `timestamp`
- `previous_failed_attempts`

Izlaz:

```json
{
  "risk_level": "HIGH"
}
```

Klase:

- `LOW`
- `MEDIUM`
- `HIGH`

### `frontend`

Jednostavan dashboard koji prikazuje:

- ukupan broj RFID dogadjaja
- broj dozvoljenih pristupa
- broj neovlascenih pristupa
- broj aktivnih uredjaja
- tabelu dogadjaja
- tabelu alarma sa MaaS procenom rizika
- grafik dogadjaja kroz vreme
- dozvoljeni vs neovlasceni pristupi
- dogadjaje po zonama

## Pokretanje

Iz korena projekta:

```bash
python maas/generate_dataset.py
python maas/train_model.py
docker compose up -d --build
```

Korisni URL-ovi:

- generator: `http://localhost:8000/health`
- data storage: `http://localhost:8001/health`
- analytics: `http://localhost:8002/health`
- maas: `http://localhost:8003/health`
- eKuiper REST API: `http://localhost:9081`
- dashboard: `http://localhost:8080`

Gasenje staka:

```bash
docker compose down
```

## Tok sistema

```text
RFID dogadjaj
        |
        v
MQTT Broker
        |
        v
eKuiper CEP
        |
        v
UNAUTHORIZED_ACCESS alarm
        |
        v
Analytics Service
        |
        v
MaaS procena rizika
        |
        v
Web Dashboard
```

## Napomene

- Projekat 3 vise nije benchmark projekat.
- Kafka se ne koristi.

