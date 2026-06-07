# IoT Mikroservisi - MQTT vs Kafka Evaluacija

Projekat 2 u okviru predmeta **Internet stvari i servisa**.
Ovaj projekat predstavlja asinhroni, event-driven sistem mikroservisa kontejnerizovanih pomoću Docker Compose-a, sa implementiranim scenarijima za uporednu evaluaciju protokola **MQTT (Mosquitto)** i **Apache Kafka**.

---

## 🏗️ Arhitektura Sistema

Sistem se sastoji od sledećih komponenti:

1. **Data Ingestion Service (Python/FastAPI)**:
   - Simulira rad velikog broja IoT uređaja.
   - Generiše realistične podatke o prolazima (Access Control System).
   - Podržava konfiguraciju MQTT QoS-a (0, 1, 2) i Kafka potvrda prijema (`acks=0, 1, all`).
   - Eksportuje metrike o poslatoj količini i brzinama na `/metrics`.

2. **Data Storage Service (Node.js)**:
   - Pretplatilac na MQTT/Kafka teme.
   - Upisuje događaje u **PostgreSQL** bazu u paketima (batching) od **500 poruka** radi optimizacije I/O podsistema.
   - Omogućava privremeno isključivanje upisa u bazu preko promenljive `DISABLE_DB_WRITE=true` radi testiranja propusne moći samog brokera.

3. **Analytics Service (Node.js)**:
   - Implementira **Tumbling Window** (fiksni vremenski prozor) od **10 sekundi**.
   - Izračunava prosečnu temperaturu u prozoru. Ako prosek pređe **50°C**, ispisuje kritičan alarm u logu.
   - Za Scenario D računa **end-to-end latenciju** od trenutka generisanja u simulatoru do alarmiranja.

4. **Monitoring Stack**:
   - **Prometheus**: Prikuplja metrike iz svih mikroservisa.
   - **cAdvisor**: Prikuplja resurse (CPU/RAM) svakog Docker kontejnera.
   - **Grafana**: Prekonfigurisana sa vizuelnim dashboard-om na portu `3000`.

---

## 📂 Struktura Projekta

```
/project_2
├── docker-compose.yml
├── db/
│   └── init.sql                 # Šema baze podataka
├── config/
│   ├── mosquitto/
│   │   └── mosquitto.conf       # MQTT konfiguracija
│   ├── prometheus/
│   │   └── prometheus.yml       # Prometheus scrape konfiguracija
│   └── grafana/
│       └── provisioning/        # Automatsko učitavanje dashborda i data source-a
├── data-ingestion/              # Python simulator (FastAPI)
├── data-storage/                # Node.js subscriber za PostgreSQL
├── analytics-service/           # Node.js stream analitika (Tumbling Window)
├── benchmarks/
│   ├── run_all_scenarios.py     # Automatska skripta za pokretanje scenarija
│   └── results.json             # Sačuvani rezultati merenja
└── README.md
```

---

## 🚀 Kako Pokrenuti Projekat

### Korak 1: Pokretanje Docker-a
Uverite se da je Docker Desktop pokrenut na vašem računaru.

### Korak 2: Pokretanje celokupnog staka
Pokrenite Docker Compose da izgradi i podigne sve servise:
```bash
docker-compose up -d --build
```

### Korak 3: Pokretanje benchmark scenarija
Pokrenite automatsku Python skriptu koja rekonfiguriše brokere, pokreće scenarije (A, B, C, D) i beleži podatke:
```bash
python benchmarks/run_all_scenarios.py
```

### Korak 4: Pregled metrika
- **Grafana Dashboard**: Pristupite na `http://localhost:3000` (Korisničko ime i šifra: `admin` / `admin`). Dashboard **"IoT MQTT vs Kafka Benchmark"** se učitava automatski i prikazuje protok, latenciju i resurse kontejnera u realnom vremenu.
- **PgAdmin / Postgres**: Port `5432` za pregled unetih događaja u bazi.

---

## 📊 Tehnički Izveštaj i Rezultati

Kompletan uporedni tehnički izveštaj sa detaljnim odgovorima na inženjerska pitanja se nalazi u fajlu **[technical_report.md](file:///c:/Users/Lenovo/Desktop/The%20Vault/Faks/IoTS/project_2/report/technical_report.md)**.
