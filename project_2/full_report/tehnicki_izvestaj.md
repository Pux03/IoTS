# Tehnicki Izvestaj
## Uporedna Evaluacija MQTT i Kafka Brokera u IoT Mikroservisnoj Arhitekturi

## 1. Cilj Projekta

Cilj projekta je bio da se istraze performanse, skalabilnost i ogranicenja dva publish-subscribe message broker sistema, `MQTT (Mosquitto)` i `Apache Kafka`, u okviru jedne iste IoT mikroservisne arhitekture. Poseban fokus je bio na razumevanju trade-off odnosa izmedju:
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

Scenario A meri kako se sistem ponasa pri paralelnom radu `100`, `1000` i `10000` uredjaja.

### Klucne brojke

| Profil | Devices | Loss % | Producer msg/s | Consumer msg/s | p95 ms | CPU % | RAM MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 100 | 0.000 | 71.403 | 48.537 | 23.592 | 1.663 | 1.180 |
| MQTT `QoS 1` | 1000 | 0.000 | 707.714 | 477.418 | 479.335 | 25.501 | 8.359 |
| MQTT `QoS 2` | 10000 | 98.960 | 3109.840 | 22.561 | 19745.846 | 100.763 | 63.020 |
| Kafka `acks=0`, `8` particija | 100 | 0.000 | 99.950 | 76.272 | 3.000 | 57.982 | 322.742 |
| Kafka `acks=1`, `8` particija | 1000 | 0.000 | 998.303 | 772.618 | 19.000 | 271.705 | 457.176 |
| Kafka `acks=1`, `8` particija | 10000 | 0.000 | 9987.017 | 7505.817 | 40.000 | 274.529 | 474.023 |

### Zakljucci

- `Kafka` je zadrzala `0%` loss kroz ceo izvrseni matrix iz performance tabele.
- `MQTT` je bila stabilna pri `100` i `1000` uredjaja, ali pri `10000` uredjaja i jacim garancijama isporuke dolazi do drasticnog pada efektivne isporuke.
- Najkriticniji rezultat u tabeli je `MQTT QoS 1/2` na `10000` uredjaja:
  - `QoS 1`: `98.944%` loss
  - `QoS 2`: `98.960%` loss
- `MQTT QoS 0` na `10000` uredjaja zadrzava `0%` loss, ali je i dalje znatno sporija od Kafka konfiguracija, sa `3700.414 msg/s` producer throughput-a naspram oko `9964-9987 msg/s` kod Kafke.
- `Kafka` postiže mnogo veci throughput i mnogo nizi `p95`, ali uz daleko veci CPU i RAM footprint.

Glavni zakljucak Scenario A je da je `Kafka` znatno robusnija za masovni ingest, dok `MQTT` ostaje prihvatljiva na manjim skalama i u laganijim edge okruzenjima.

### 4.2 Scenario B: Edge Connectivity Failures

Scenario B meri ponasanje sistema tokom `30s` mreznog prekida i recovery mehanizme posle reconnect-a.

Po tabeli se Scenario B posmatra u dva moda:
- `tool_benchmark`: cistiji broker-level pogled
- `app_buffered`: end-to-end pogled sa aplikacionim buffering-om

### Klucne brojke: tool_benchmark

| Profil | Storage msg/s | Loss % | p95 ms | CPU % | RAM MB | Ready s | First Analytics s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 109.224 | 50.000 | 30000.000 | 23.925 | 4.906 | 0.219 | 0.632 |
| MQTT `QoS 1` | 18.958 | 37.185 | 30000.000 | 12.375 | 3.727 | 10.988 | - |
| MQTT `QoS 2` | 100.814 | 0.000 | 468.816 | 37.849 | 13.094 | 10.764 | 0.608 |
| Kafka `acks=0`, `1` particija | 48.276 | 3.970 | 47427.000 | 101.105 | 338.512 | 6.074 | - |
| Kafka `acks=1`, `1` particija | 50.304 | 0.000 | 47392.000 | 78.287 | 332.242 | 3.777 | 0.557 |
| Kafka `acks=all`, `1` particija | 50.778 | 0.000 | 45832.000 | 89.117 | 342.254 | 2.581 | 0.742 |

### Klucne brojke: app_buffered

| Profil | Storage msg/s | Loss % | p95 ms | CPU % | RAM MB | Ready s | First Analytics s | Max Lag |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 165.093 | 0.151 | 55606.319 | 9.585 | 5.984 | 25.425 | 0.575 | - |
| MQTT `QoS 1` | 188.062 | 0.000 | 71054.498 | 38.552 | 11.059 | 24.237 | 0.753 | - |
| MQTT `QoS 2` | 189.055 | 0.000 | 70095.600 | 54.042 | 18.215 | 23.268 | 0.653 | - |
| Kafka `acks=1`, `4` particije | 124.037 | 0.000 | 110394.869 | 96.854 | 884.828 | 0.006 | 9.585 | 16 |
| Kafka `acks=1`, `8` particija | 124.441 | 0.000 | 110844.025 | 139.023 | 941.844 | 0.005 | 9.557 | 16 |

### Zakljucci

- U `tool_benchmark` modu `MQTT QoS 0` ima najmanji reconnect overhead, ali gubi `50%` poruka.
- `MQTT QoS 2` znacajno popravlja pouzdanost, do `0%` loss, ali trazi vise vremena do pune spremnosti i veci resursni trosak.
- `Kafka acks=1` i `acks=all` u tool modu takodje daju `0%` loss, ali po cenu mnogo vise potrosnje resursa i vrlo visokog `p95`.
- U `app_buffered` modu `MQTT` izgleda bolje po pitanju loss-a, ali to ukljucuje i aplikacioni buffering, ne samo broker recovery.
- `Kafka` daje jasnu observability sliku kroz `Max Lag`, sto je velika prednost za cloud-side dijagnostiku i kontrolu recovery-ja.

Glavni zakljucak Scenario B je da je `MQTT` laganija i jednostavnija za edge reconnect slucajeve, dok `Kafka` daje mnogo bolju kontrolu i vidljivost recovery stanja, uz znatno veci resursni trosak.

### 4.3 Scenario C: Burst Event Load

Scenario C simulira nagli skok opterecenja sa `50` na `5000 msg/s`, uz pracenje backlog-a, backpressure-a i recovery vremena.

### Klucne brojke

| Profil | Burst Storage msg/s | p95 ms | CPU % | RAM MB | Peak Backlog | Recovery s | Peak Lag | Loss % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT `QoS 0` | 1737.992 | 20.912 | 45.366 | 11.977 | 1.000 | 19.643 | 0.000 | 48.838 |
| MQTT `QoS 1` | 994.549 | 474.716 | 61.682 | 16.320 | 118.000 | 19.199 | 0.000 | 29.299 |
| MQTT `QoS 2` | 625.827 | 967.645 | 103.672 | 28.812 | 220.000 | 57.208 | 0.000 | 21.653 |
| Kafka `acks=0`, `4` particije | 2870.532 | 43.000 | 87.424 | 433.301 | 0.000 | 1.706 | 391.000 | 0.000 |
| Kafka `acks=1`, `8` particija | 3114.134 | 104.000 | 100.560 | 449.809 | 0.000 | 0.000 | 410.000 | 0.000 |
| Kafka `acks=all`, `4` particije | 2974.453 | 48.000 | 101.473 | 434.863 | 0.000 | 0.000 | 445.000 | 0.000 |

### Zakljucci

- `MQTT QoS 0` daje najbolju latenciju unutar MQTT grupe, ali ima najveci gubitak poruka, skoro `49%`.
- Povecanjem MQTT garancije isporuke, loss se smanjuje, ali p95, backlog i recovery vreme rastu veoma znacajno.
- `MQTT QoS 2` je najskuplja konfiguracija po pitanju CPU, backlog-a i recovery vremena.
- `Kafka` kroz sve prikazane konfiguracije drzi `0%` loss, mnogo veci burst throughput i jasan signal pritiska kroz `Peak Lag`.
- Particionisanje kod Kafke ne daje uvek linearan rast throughput-a, ali omogucava bolju analizu backlog-a i ponašanja pod pritiskom.

Glavni zakljucak Scenario C je da `Kafka` mnogo bolje podnosi burst opterecenje, dok `MQTT` postaje problematična kada su istovremeno bitni i mala latencija i mala stopa gubitka.

### 4.4 Scenario D: Real-Time Alerting

Scenario D meri end-to-end latenciju od trenutka generisanja kriticne vrednosti do emitovanja alarma iz `Analytics Service`.

### Klucne brojke

| Profil | Mod | Alert Avg ms | Alert p95 ms | Alert Max ms | CPU % | RAM MB | Successful Repeats |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| MQTT `QoS 0` | early | 6638.000 | 7759.000 | 7759.000 | 0.084 | 0.695 | 3/3 |
| MQTT `QoS 1` | early | 6708.667 | 7678.000 | 7678.000 | 0.134 | 0.703 | 3/3 |
| MQTT `QoS 2` | late | 8076.667 | 9180.000 | 9180.000 | 0.170 | 0.707 | 3/3 |
| Kafka `acks=0` | early | 9847.667 | 9848.000 | 9848.000 | 34.186 | 310.027 | 3/3 |
| Kafka `acks=1` | early | 9849.667 | 9851.000 | 9851.000 | 43.854 | 305.160 | 3/3 |
| Kafka `acks=all` | late | 11197.333 | 11198.000 | 11198.000 | 38.107 | 311.219 | 3/3 |

### Zakljucci

- U ovom scenariju `MQTT` daje nizu alert latenciju od Kafke.
- Kod `MQTT` su alert vremena tipicno izmedju oko `6.6s` i `8.1s`, dok je kod `Kafka` oko `9.8s` do `11.2s`.
- Razlike izmedju `QoS` i `acks` konfiguracija su manje od uticaja same tumbling-window logike.
- `MQTT` trosi zanemarljivo malo resursa u odnosu na Kafku:
  - MQTT oko `0.695-0.707 MB` RAM
  - Kafka oko `305-311 MB` RAM

Glavni zakljucak Scenario D je da je `MQTT` pogodnija za real-time alerting na edge-u, dok je `Kafka` teza i sporija u ovom konkretnom alerting toku, iako ostaje bolja za cloud observability i siri data pipeline.

## 5. Odgovori na Inzenjerska Pitanja

### 5.1 Zasto je MQTT idealan za edge, a zasto nije pogodna za istorijsku analitiku velikih podataka?

Na osnovu tabela, `MQTT` je veoma pogodna za edge zato sto ima ekstremno mali resursni footprint:
- u Scenario D trosi oko `0.7 MB` RAM
- u Scenario A na `100` uredjaja trosi oko `1.180-1.996 MB` RAM
- u Scenario B tool modu trosi `3.727-13.094 MB` RAM

Ovo je mnogo manje od Kafke, koja se kroz sve scenarije krece od oko `305 MB` do preko `900 MB` RAM.

Medjutim, tabele takodje pokazuju da `MQTT` postaje ogranicavajuca kada je potrebna velika skala i jaka pouzdanost:
- u Scenario A pri `10000` uredjaja i `QoS 1/2` dolazi do oko `99%` gubitka poruka
- u Scenario C ili gubi mnogo poruka (`QoS 0`) ili dramaticno povecava latenciju i recovery vreme (`QoS 1/2`)

Zato je MQTT odlicna za edge ingest, ali nije dobar izbor kada sistem zahteva veliku skalu, replay-friendly obradu i jaku kontrolu backlog-a i istorije podataka.

### 5.2 Zasto Kafka dominira u data-intensive cloud sistemima i koja je cena njene skalabilnosti?

Tabele vrlo jasno pokazuju zasto `Kafka` dominira u data-intensive cloud sistemima:
- u Scenario A drzi `0%` loss i gotovo `10k msg/s` producer throughput na `10000` uredjaja
- u Scenario C drzi `0%` loss i `2.7k-3.1k msg/s` burst storage throughput
- u Scenario B daje jasan `Max Lag` signal i time omogucava observability recovery-ja

Cena te skalabilnosti je velika potrosnja resursa:
- Scenario A: cesto `340-480 MB` RAM
- Scenario C: oko `429-458 MB` RAM
- Scenario D: oko `305-311 MB` RAM
- Scenario B app mode: kod vise particija ide i preko `900 MB` RAM

Dakle, `Kafka` je odlican izbor za cloud i data-intensive sisteme, ali nije prirodan izbor za hardverski ogranicene edge servere. Na edge-u je moguca samo na jacim gateway uredjajima i uz prihvatanje veceg CPU/RAM troska.

### 5.3 Uporedna tabela performansi

Za finalno poredjenje najkorisnije je gledati reprezentativne profile po scenarijima:

| Scenario | MQTT reprezentativno | Kafka reprezentativno | Zakljucak |
| --- | --- | --- | --- |
| A | `QoS 0`, `10000` uredjaja: `3700.414 msg/s`, `p95 2727.876 ms`, `99.689% CPU`, `53.715 MB RAM`, `0% loss` | `acks=1`, `8` particija, `10000` uredjaja: `9987.017 msg/s`, `p95 40 ms`, `274.529% CPU`, `474.023 MB RAM`, `0% loss` | Kafka je mnogo bolja za masovni ingest. |
| B | `QoS 2`, tool mode: `100.814 msg/s`, `p95 468.816 ms`, `37.849% CPU`, `13.094 MB RAM`, `0% loss` | `acks=1`, tool mode, `1` particija: `50.304 msg/s`, `p95 47392 ms`, `78.287% CPU`, `332.242 MB RAM`, `0% loss` | MQTT je laksa, Kafka daje bolju observability sliku. |
| C | `QoS 1`: `994.549 msg/s`, `p95 474.716 ms`, `61.682% CPU`, `16.320 MB RAM`, `29.299% loss` | `acks=1`, `8` particija: `3114.134 msg/s`, `p95 104 ms`, `100.560% CPU`, `449.809 MB RAM`, `0% loss` | Kafka bolje podnosi burst i backlog. |
| D | `QoS 0`, early: `p95 7759 ms`, `0.084% CPU`, `0.695 MB RAM` | `acks=1`, early: `p95 9851 ms`, `43.854% CPU`, `305.160 MB RAM` | MQTT je bolja za edge alerting. |

## 6. Opsti Zakljucak

Na osnovu performance tabela za sva cetiri scenarija, moze se izvesti vrlo jasan opsti zakljucak:

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
