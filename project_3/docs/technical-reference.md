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
| `rfid/alerts` | `ekuiper` | `analytics` | CEP alarmi `UNAUTHORIZED_ACCESS` |
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

## CEP pravilo

Aktivno je samo jedno eKuiper pravilo:

- ime pravila: `rfid_unauthorized_access`
- ulazni stream: `rfid_events`
- uslov: `access_granted = false`
- izlazni topic: `rfid/alerts`

SQL pravilo:

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

## MaaS servis

MaaS ne detektuje alarm, vec procenjuje rizik postojeceg `UNAUTHORIZED_ACCESS` alarma.

### Endpoint

- `POST /predict`

### Ulaz

```json
{
  "signal_strength": -69,
  "response_time_ms": 89,
  "battery_voltage": 3.17,
  "zone": "SECOND_FLOOR",
  "door_id": "SERVER_ROOM",
  "timestamp": "2026-07-09T14:31:08Z",
  "previous_failed_attempts": 0
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
