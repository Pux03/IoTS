# Tehnicka Referenca - Projekat 3 RFID IoT Sistem

Ovaj dokument je kratka operativna referenca za trenutnu implementaciju Projekta 3.

## Arhitektura

```text
RFID Generator
        |
        | MQTT: rfid/events
        v
 MQTT Broker (Mosquitto)
        |
        +--------------------------+
        |                          |
        v                          v
Data Storage Service         eKuiper CEP
                                   |
                                   | MQTT: rfid/alerts
                                   v
                            Analytics Service
                                   |
                                   +--> HTTP -> MaaS /predict
                                   |
                                   +--> MQTT: rfid/analytics
                                   |
                                   v
                              Web Dashboard
```

## Servisi i portovi

| Servis | Svrha | Port |
|---|---|---|
| `generator` | generise i objavljuje RFID dogadjaje | `8000` |
| `mqtt-broker` | Mosquitto broker | `1883` |
| `data-storage` | MQTT subscriber i PostgreSQL upis | `8001` |
| `db` | PostgreSQL baza | `5432` |
| `ekuiper` | CEP obrada | `9081` |
| `maas` | procena rizika | `8003` |
| `analytics` | statistika, alarmi, REST API | `8002` |
| `frontend` | dashboard | `8080` |

## MaaS ML workflow

MaaS je podeljen na tri jasno odvojene faze:

1. `generate_dataset.py`
   - koristi postojeci RFID generator iz `generator/generator.py`
   - generise istorijski skup RFID dogadjaja
   - cuva rezultat kao `dataset.csv`
2. `train_model.py`
   - ucitava `dataset.csv`
   - trenira `RandomForestClassifier` kroz `Pipeline`
   - cuva istrenirani model kao `model.pkl`
3. `app.py`
   - ne trenira model
   - pri pokretanju ucitava `model.pkl`
   - koristi model samo za inferenciju na `POST /predict`

Struktura:

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

## MQTT topic-i

| Topic | Producent | Potrosac | Svrha |
|---|---|---|---|
| `rfid/events` | `generator` | `data-storage`, `analytics`, `ekuiper` | svi RFID dogadjaji |
| `rfid/alerts` | `ekuiper` | `analytics` | CEP alarmi `UNAUTHORIZED_ACCESS`, `BRUTE_FORCE_ATTEMPT` |
| `rfid/analytics` | `analytics` | opcioni MQTT subscriber-i | sazetak statistike i poslednji alarm |

## RFID dogadjaj

Generator objavljuje iskljucivo RFID payload sledece forme:

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

## CEP pravila

Aktivna su dva eKuiper pravila koja objavljuju na `rfid/alerts`.

### Pravilo 1: `rfid_unauthorized_access`

- ulazni stream: `rfid_events`
- uslov: `access_granted = false`
- izlazni topic: `rfid/alerts`

```sql
SELECT "UNAUTHORIZED_ACCESS" AS alert, device_id, door_id, zone, card_uid, timestamp
FROM rfid_events
WHERE access_granted = false
```

Primer alarm poruke:

```json
{
  "alert": "UNAUTHORIZED_ACCESS",
  "device_id": "RFID-LAB-01",
  "door_id": "SERVER_ROOM",
  "zone": "SECOND_FLOOR",
  "card_uid": "A42F90B2",
  "timestamp": "2026-07-09T13:20:10Z"
}
```

### Pravilo 2: `rfid_brute_force_attempt`

- ulazni stream: `rfid_events`
- filtrira samo `ACCESS_DENIED`, `CARD_UNKNOWN` i `FORCED_OPEN`
- agregira po `device_id` unutar `TUMBLINGWINDOW(ss, 30)`
- aktivira se kada je `COUNT(*) >= 3`
- izlazni topic: `rfid/alerts`

```sql
SELECT
  "BRUTE_FORCE_ATTEMPT" AS alert,
  device_id,
  last_value(door_id, true) AS door_id,
  last_value(zone, true) AS zone,
  last_value(card_uid, true) AS card_uid,
  last_value(timestamp, true) AS timestamp,
  count(*) AS attempt_count,
  avg(response_time_ms) AS avg_response_time_ms,
  min(signal_strength) AS min_signal_strength,
  avg(battery_voltage) AS avg_battery_voltage,
  window_start() AS window_start_ms,
  window_end() AS window_end_ms
FROM rfid_events
WHERE event_type IN ("ACCESS_DENIED", "CARD_UNKNOWN", "FORCED_OPEN")
GROUP BY device_id, TUMBLINGWINDOW(ss, 30)
HAVING count(*) >= 3
```

Primer alarm poruke:

```json
{
  "alert": "BRUTE_FORCE_ATTEMPT",
  "device_id": "RFID-LAB-01",
  "door_id": "SERVER_ROOM",
  "zone": "SECOND_FLOOR",
  "card_uid": "A42F90B2",
  "timestamp": "2026-07-09T13:20:24Z",
  "attempt_count": 3,
  "avg_response_time_ms": 121.7,
  "min_signal_strength": -77,
  "avg_battery_voltage": 3.28,
  "window_start_ms": 1783603200000,
  "window_end_ms": 1783603230000
}
```

## MaaS servis

MaaS ne detektuje alarm, vec procenjuje rizik postojeceg CEP alarma kao sto su `UNAUTHORIZED_ACCESS` i `BRUTE_FORCE_ATTEMPT`.

### Endpoint

- `POST /predict`

### Ulaz

```json
{
  "attempt_count": 7,
  "avg_response_time_ms": 118.4,
  "min_signal_strength": -76,
  "avg_battery_voltage": 3.29,
  "zone": "SECOND_FLOOR",
  "door_id": "SERVER_ROOM",
  "timestamp": "2026-07-09T14:31:08Z",
  "previous_failed_attempts": 0,
  "avg_response_time_last5": 84.6,
  "denial_rate_last10": 0.3
}
```

### Izlaz

```json
{
  "risk_level": "HIGH"
}
```

Klase:

- `LOW`
- `MEDIUM`
- `HIGH`

Model:

- `RandomForestClassifier`

## REST API

### Generator

- `GET /health`
- `GET /status`
- `GET /metrics`

Primer:

- `http://localhost:8000/health`

### Data Storage

- `GET /health`
- `GET /status`
- `GET /metrics`

Primer:

- `http://localhost:8001/status`

### Analytics

- `GET /health`
- `GET /api/summary`
- `GET /api/events`
- `GET /api/alerts`
- `GET /api/dashboard`
- `GET /metrics`

Primer odgovora `GET /api/summary`:

```json
{
  "total_events": 40,
  "granted_accesses": 27,
  "unauthorized_accesses": 13,
  "active_devices": 5,
  "total_alerts": 12
}
```

### MaaS

- `GET /health`
- `POST /predict`

### Frontend

- `GET /`

URL:

- `http://localhost:8080`

## Dashboard podaci

Frontend koristi `analytics` API i prikazuje:

- ukupan broj RFID dogadjaja
- broj dozvoljenih pristupa
- broj neovlascenih pristupa
- broj aktivnih uredjaja
- tabelu poslednjih dogadjaja
- tabelu poslednjih alarma sa `risk_level`
- graf dogadjaja kroz vreme
- graf dozvoljeni vs neovlasceni pristupi
- graf dogadjaja po zoni

## Baza podataka

Tabela `events`:

| Kolona | Tip |
|---|---|
| `event_id` | `UUID` |
| `timestamp` | `TIMESTAMPTZ` |
| `device_id` | `VARCHAR(50)` |
| `card_uid` | `VARCHAR(32)` |
| `access_granted` | `BOOLEAN` |
| `door_id` | `VARCHAR(50)` |
| `zone` | `VARCHAR(50)` |
| `signal_strength` | `INT` |
| `battery_voltage` | `NUMERIC(4,2)` |
| `response_time_ms` | `INT` |
| `event_type` | `VARCHAR(50)` |

## Pokretanje

```bash
python maas/generate_dataset.py
python maas/train_model.py
docker compose up -d --build
```

Gasenje:

```bash
docker compose down
```

## Brza provera rada

Health provera:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

Dashboard:

```text
http://localhost:8080
```

Analytics snapshot preko MQTT:

```bash
mosquitto_sub -h localhost -t rfid/analytics -C 1
```
