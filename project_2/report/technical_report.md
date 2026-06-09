# Tehnicki Izvestaj: Uporedna Evaluacija MQTT i Kafka Brokera u IoT Arhitekturi

Ovaj izvestaj sumira rezultate projekta 2 kroz cetiri obavezna scenarija:
- Scenario A: Massive Sensor Ingestion
- Scenario B: Edge Connectivity Failures
- Scenario C: Burst Event Load
- Scenario D: Real-Time Alerting

Sve brojke u nastavku su zasnovane na stvarno izvrsenim benchmark artefaktima:
- [scenario_a_results_full.json](/c:/Users/jorda/Desktop/IoTS--Ptojekti/IoTS/project_2/benchmarks/scenario_a_results_full.json:1)
- [scenario_b_results_full.json](/c:/Users/jorda/Desktop/IoTS--Ptojekti/IoTS/project_2/benchmarks/scenario_b_results_full.json:1)
- [scenario_c_results_full.json](/c:/Users/jorda/Desktop/IoTS--Ptojekti/IoTS/project_2/benchmarks/scenario_c_results_full.json:1)
- [scenario_d_results_full.json](/c:/Users/jorda/Desktop/IoTS--Ptojekti/IoTS/project_2/benchmarks/scenario_d_results_full.json:1)

## 1. Kratak Opis Implementacije

Arhitektura sistema je asinhrona i event-driven, sa tri mikroservisa:
- `Data Ingestion Service` u Python/FastAPI sloju simulira IoT uredjaje i publikuje poruke.
- `Data Storage Service` u Node.js/Express sloju cita poruke sa brokera i upisuje ih u PostgreSQL, uz batching i mogucnost gasenja DB upisa tokom intenzivnih testova.
- `Analytics Service` u Node.js/Express sloju vrsi stream obradu nad 10-sekundnim tumbling window-om i podize alarm kada prosecna temperatura predje definisani prag.

Sistem podrzava dva brokera:
- `MQTT (Mosquitto)` sa `QoS 0`, `QoS 1` i `QoS 2`
- `Apache Kafka` u `KRaft` rezimu sa `acks=0`, `acks=1` i `acks=all`

Celokupan stack se pokrece kroz Docker Compose:
- [docker-compose.yml](/c:/Users/jorda/Desktop/IoTS--Ptojekti/IoTS/project_2/docker-compose.yml:1)

## 2. Metodologija Merenja

Za generisanje opterecenja i prikupljanje metrika korisceni su namenski alati:
- `emqtt-bench` za MQTT testove
- `kafka-producer-perf-test.sh` za Kafka testove
- Docker stats API preko `resource-monitor` servisa za CPU, RAM i mrezu

Tokom stress-testova visokog intenziteta:
- DB write je bio iskljucen kada je fokus bio na brokeru i potrosackom lancu
- `Data Storage` je i dalje zadrzao batching logiku za regularan rad

Napomena za Scenario B:
- `tool_benchmark` mod meri broker-level outage/recovery ponasanje sa namenskim alatima
- `app_buffered` mod ukljucuje i simulator/offline buffering logiku, pa meri realno end-to-end operativno ponasanje sistema

## 3. Rezultati Po Scenarijima

### 3.1 Scenario A: Massive Sensor Ingestion

Scenario A meri koliko sistem moze da skalira pri `100`, `1000` i `10000` uredjaja. Najvazniji nalaz je da je Kafka zadrzala `0%` loss kroz ceo izvrseni matrix, dok je MQTT ostao stabilan pri `QoS 0`, ali je pri `QoS 1` i `QoS 2` na `10000` uredjaja doslo do faktickog kolapsa isporuke.

Reprezentativni rezultati za `10000` uredjaja:

| Profil | Poslate | Primljene | Loss % | Producer msg/s | Consumer msg/s | p95 ms | CPU % | RAM MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 100000 | 100000 | 0.000 | 3700.414 | 2954.908 | 2727.876 | 99.689 | 53.715 |
| MQTT `QoS 1` | 100000 | 1056 | 98.944 | 3893.475 | 27.487 | 14739.130 | 102.220 | 45.262 |
| MQTT `QoS 2` | 100000 | 1040 | 98.960 | 3109.840 | 22.561 | 19745.846 | 100.763 | 63.020 |
| Kafka `acks=0`, `8` particija | 100000 | 100000 | 0.000 | 9980.040 | 7209.805 | 15.000 | 283.867 | 481.125 |
| Kafka `acks=1`, `8` particija | 100000 | 100000 | 0.000 | 9987.017 | 7505.817 | 40.000 | 274.529 | 474.023 |
| Kafka `acks=all`, `8` particija | 100000 | 100000 | 0.000 | 9964.129 | 7784.524 | 69.000 | 206.192 | 364.797 |

Zakljucci Scenario A:
- MQTT je veoma lagan pri `100` i `1000` uredjaja, ali pri `10000` i visim garancijama isporuke dolazi do dramaticnog pada efektivne isporuke.
- Kafka je kroz sve `acks` i kroz `1/4/8` particija zadrzala `0%` gubitka poruka.
- Cena Kafka stabilnosti je mnogo veci CPU/RAM footprint.

### 3.2 Scenario B: Edge Connectivity Failures

Scenario B simulira `30s` mreznog prekida nad publisher/simulator cvorom preko `docker network disconnect`, a zatim reconnect. Cilj je da se uporede recovery mehanizmi oba brokera.

#### Broker-level pogled (`tool_benchmark`)

| Profil | Storage msg/s | Loss % | p95 ms | CPU % | RAM MB | Ready s | First Analytics s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 109.224 | 50.000 | 30000.000 | 23.925 | 4.906 | 0.219 | 0.632 |
| MQTT `QoS 2` | 100.814 | 0.000 | 468.816 | 37.849 | 13.094 | 10.764 | 0.608 |
| Kafka `acks=1`, `1` particija | 50.304 | 0.000 | 47392.000 | 78.287 | 332.242 | 3.777 | 0.557 |

#### End-to-end pogled (`app_buffered`)

| Profil | Storage msg/s | Loss % | p95 ms | CPU % | RAM MB | Ready s | First Analytics s | Max Lag |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 165.093 | 0.151 | 55606.319 | 9.585 | 5.984 | 25.425 | 0.575 | - |
| MQTT `QoS 2` | 189.055 | 0.000 | 70095.600 | 54.042 | 18.215 | 23.268 | 0.653 | - |
| Kafka `acks=1`, `4` particije | 124.037 | 0.000 | 110394.869 | 96.854 | 884.828 | 0.006 | 9.585 | 16 |

Zakljucci Scenario B:
- U broker-only modu, `MQTT QoS 0` gubi oko polovine poruka nastalih tokom outage-a, jer ne postoji pouzdana potvrda i trajni replay mehanizam.
- `MQTT QoS 1/2` popravljaju isporuku, ali uz veci recovery overhead i rast latencije.
- Kafka zadrzava `0%` gubitka u reprezentativnim `acks=1/all` tool-benchmark run-ovima i daje eksplicitnu sliku kroz `CURRENT-OFFSET`, `LOG-END-OFFSET` i `LAG`.
- U `app_buffered` modu i MQTT i Kafka izgledaju bolje u smislu loss-a, ali ta slika ukljucuje i aplikaciono bufferovanje, a ne samo broker mehanizam.

### 3.3 Scenario C: Burst Event Load

Scenario C simulira nagli skok sa `50` na `5000 msg/s`, zatim povratak na `50 msg/s`. Fokus je na backlog-u, backpressure-u i vremenu oporavka.

Reprezentativni rezultati:

| Profil | Warmup Storage msg/s | Burst Storage msg/s | p95 ms | CPU % | RAM MB | Peak Backlog | Recovery to Zero s | Peak Lag | Loss % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 45.846 | 1737.992 | 20.912 | 45.366 | 11.977 | 1.000 | 19.643 | 0.000 | 48.838 |
| MQTT `QoS 1` | 46.220 | 994.549 | 474.716 | 61.682 | 16.320 | 118.000 | 19.199 | 0.000 | 29.299 |
| MQTT `QoS 2` | 46.370 | 625.827 | 967.645 | 103.672 | 28.812 | 220.000 | 57.208 | 0.000 | 21.653 |
| Kafka `acks=0`, `4` particije | 48.803 | 2870.532 | 43.000 | 87.424 | 433.301 | 0.000 | 1.706 | 391.000 | 0.000 |
| Kafka `acks=1`, `8` particija | 46.531 | 3114.134 | 104.000 | 100.560 | 449.809 | 0.000 | 0.000 | 410.000 | 0.000 |
| Kafka `acks=all`, `4` particije | 48.859 | 2974.453 | 48.000 | 101.473 | 434.863 | 0.000 | 0.000 | 445.000 | 0.000 |

Zakljucci Scenario C:
- MQTT je laksi za edge, ali pod burst opterecenjem trpi ili visok loss (`QoS 0`) ili visoku latenciju i recovery cenu (`QoS 1/2`).
- Kafka zadrzava `0%` loss i visoku burst propusnost, a pritisak se jasno vidi kroz consumer lag.
- Particionisanje pomaze paralelizmu i observability-ju, ali povecava resursni trosak.

### 3.4 Scenario D: Real-Time Alerting

Scenario D meri end-to-end vreme od generisanja kriticne vrednosti do alarma iz `Analytics Service`. Test je izvrsen u `early` i `late` modu u odnosu na granicu `10s` tumbling window-a, sa `3` ponavljanja po profilu.

Reprezentativni rezultati:

| Profil | Mod | Alert Avg ms | Alert p95 ms | Alert Max ms | CPU % | RAM MB | Peak Lag | Uspesna ponavljanja |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| MQTT `QoS 0` | early | 6638.000 | 7759.000 | 7759.000 | 0.084 | 0.695 | 0.000 | 3/3 |
| MQTT `QoS 2` | late | 8076.667 | 9180.000 | 9180.000 | 0.170 | 0.707 | 0.000 | 3/3 |
| Kafka `acks=1`, `1` particija | early | 9849.667 | 9851.000 | 9851.000 | 43.854 | 305.160 | 0.000 | 3/3 |
| Kafka `acks=all`, `1` particija | late | 11197.333 | 11198.000 | 11198.000 | 38.107 | 311.219 | 0.000 | 3/3 |

Zakljucci Scenario D:
- Na alert latenciju najvise utice `10s` tumbling window, ne sam broker.
- MQTT je u ovom scenariju brzi i drasticno laksi po resursima.
- Kafka je sporija u alerting putanji, ali zadrzava eksplicitnu observability sliku kroz lag i offset stanje.

## 4. Odgovori na Inzenjerska Pitanja

### 4.1 Zasto je MQTT idealan za edge, a zasto nije dobar za istorijsku analitiku velikih podataka?

MQTT je idealan za edge zato sto:
- trosi veoma malo resursa
- jednostavan je za deployment
- ima mali protokolni overhead

To je jasno iz scenarija:
- u Scenario D MQTT trosi oko `0.7 MB` RAM i oko `0.08-0.17%` CPU
- u Scenario B tool-benchmark modu MQTT trosi reda velicine `4-13 MB` RAM, dok je Kafka oko `332-344 MB`

Medjutim, MQTT postaje nepogodan kada je potrebna istorijska analitika zato sto:
- nema prirodni replay model kao Kafka log
- nema offset/lag observability koju ima Kafka
- pod velikim opterecenjem i jacim delivery garancijama pokazuje ogranicenja

Najjasniji primer je Scenario A:
- `MQTT QoS 1` i `QoS 2` na `10000` uredjaja imaju oko `99%` gubitka
- Kafka u istom scenariju zadrzava `0%` loss kroz ceo izvrseni matrix

### 4.2 Zasto Kafka dominira u data-intensive cloud sistemima i kolika je cena njene skalabilnosti?

Kafka dominira u cloud i data-intensive sistemima zato sto nudi:
- trajni log
- replay istorije
- offset-based recovery
- consumer lag observability
- particionisanje za paralelizam i skaliranje

To se vidi u rezultatima:
- Scenario A: Kafka drzi `0%` loss i oko `10k msg/s` producer throughput na `10000` uredjaja
- Scenario C: Kafka drzi `0%` loss pri burst-u `50 -> 5000 msg/s`, uz jasan prikaz `peak lag` vrednosti
- Scenario B: Kafka omogucava precizno pracenje `CURRENT-OFFSET`, `LOG-END-OFFSET` i `LAG` tokom outage/recovery toka

Cena te skalabilnosti je visoka:
- u Scenario C Kafka trosi oko `429-458 MB` RAM
- u Scenario D trosi oko `305-311 MB` RAM i desetine procenata CPU
- u Scenario B `app_buffered` mod ide i do `884-950 MB` RAM za vise particija

Zakljucak je da je Kafka realna na jacim edge gateway uredjajima, ali nije prirodan izbor za hardverski veoma ogranicene edge cvorove. Za cloud, odnosno za data-intensive obradu, njena cena resursa je opravdana dobijenom skalabilnoscu i observability-jem.

### 4.3 Uporedna tabela performansi na osnovu eksperimenata

Za potrebe zavrsnog poredjenja najkorisnije je gledati reprezentativne workload-e:

| Scenario | MQTT reprezentativno | Kafka reprezentativno | Zakljucak |
| --- | --- | --- | --- |
| A: Massive ingest | `QoS 0`, `10000` uredjaja: `3700.414 msg/s`, `p95 2727.876 ms`, `99.689% CPU`, `53.715 MB RAM`, `0% loss` | `acks=1`, `8` particija, `10000` uredjaja: `9987.017 msg/s`, `p95 40 ms`, `274.529% CPU`, `474.023 MB RAM`, `0% loss` | Kafka dominantna za skaliranje i pouzdanost; MQTT je laksi, ali znatno sporiji i losije podnosi jace garancije. |
| B: Outage recovery | `QoS 2`, tool mode: `100.814 msg/s`, `p95 468.816 ms`, `37.849% CPU`, `13.094 MB RAM`, `0% loss` | `acks=1`, tool mode, `1` particija: `50.304 msg/s`, `p95 47392 ms`, `78.287% CPU`, `332.242 MB RAM`, `0% loss`, `lag=0` na kraju | MQTT je laksi, Kafka daje mnogo bolju observability sliku i replay logiku. |
| C: Burst load | `QoS 1`: `994.549 msg/s`, `p95 474.716 ms`, `61.682% CPU`, `16.320 MB RAM`, `29.299% loss` | `acks=1`, `8` particija: `3114.134 msg/s`, `p95 104 ms`, `100.560% CPU`, `449.809 MB RAM`, `0% loss` | Kafka mnogo bolje podnosi burst i backlog; MQTT ostaje jeftiniji po resursima. |
| D: Alerting | `QoS 0`, early: `p95 7759 ms`, `0.084% CPU`, `0.695 MB RAM` | `acks=1`, early: `p95 9851 ms`, `43.854% CPU`, `305.160 MB RAM` | MQTT je bolji za edge alerting; Kafka je teza, ali cloud-friendly. |

## 5. Opsti Zakljucak

Na osnovu sva cetiri scenarija, izbor brokera zavisi od cilja sistema:

- `MQTT` je bolji izbor kada su prioritet mali footprint, jednostavnost i postavljanje na edge/senzorske uredjaje.
- `Kafka` je bolji izbor kada su prioritet throughput, istorijska obrada, replay, observability i rad u data-intensive cloud sistemu.

Prakticno tumacenje rezultata je sledece:
- Za male i srednje edge deployment-e MQTT je efikasan i jeftin.
- Za velike tokove podataka, recovery pod pritiskom, burst scenario i istorijsku analitiku Kafka je znatno robusnija.
- Najveca cena Kafka pristupa je visoka potrosnja CPU/RAM resursa.
- Najveca mana MQTT pristupa je to sto pod jacim delivery garancijama i velikim opterecenjem brzo postaje ogranicavajuci faktor.

Zbog toga je najbolji inzenjerski zakljucak:
- `MQTT` za edge ingest i lagane publish/subscriber topologije
- `Kafka` za centralni cloud sloj, dugotrajno cuvanje toka, vise potrosaca i ozbiljnu analitiku
