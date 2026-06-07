# Tehnički Izveštaj: Uporedna Evaluacija MQTT i Kafka Brokera u IoT Arhitekturi

Ovaj izveštaj analizira performanse, pouzdanost i skalabilnost protokola **MQTT (Mosquitto)** i **Apache Kafka** na osnovu eksperimentalnih IoT scenarija sprovedenih kroz kontejnerizovane mikroservise.

---

## 1. Kratak Opis Implementacije

Arhitektura sistema je osmišljena kao asinhroni, događajima-vođen (event-driven) sistem razvijen u dve tehnologije (**Python** i **Node.js**):
- **Data Ingestion Service (Python)**: Generiše pakete podataka o kretanju zaposlenih (prolazi kroz vrata) u realnom vremenu koristeći višenitne asinhrone simulatore uređaja. Podržava prebacivanje protokola (MQTT/Kafka) i nivoa pouzdanosti.
- **Data Storage Service (Node.js)**: Preuzima poruke sa odgovarajuće teme (topic) i vrši **batching (grupni upis)** u PostgreSQL bazu podataka na svakih 500 poruka. Ovo sprečava disk I/O usko grlo i optimizuje rad baze pod velikim opterećenjem.
- **Analytics Service (Node.js)**: Agregira podatke u **10-sekundnim Tumbling Window** prozorima. Ukoliko prosečna temperatura senzora u prozoru pređe 50°C, ispisuje se kritičan log alarm i meri se end-to-end latencija od nastanka critical event-a u simulatoru.

---

## 2. Uporedna Tabela Performansi

Sledeći rezultati su dobijeni kroz automatizovane scenarije testiranja sa 1000 aktivnih uređaja:

| Broker / Konfiguracija | Throughput (msg/s) | Gubitak poruka (%) | Vreme oporavka burst-a (s) | p95 Latencija alarma (ms) | Prosečan CPU (%) | Prosečan RAM (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **MQTT (QoS 0)** | 1000.00 | 0.58% | 3.42s | 8.40 ms | ~8% | ~12 MB |
| **MQTT (QoS 1)** | 1000.00 | 0.00% | 4.15s | 19.80 ms | ~14% | ~18 MB |
| **MQTT (QoS 2)** | 850.00 | 0.00% | 6.80s | 54.20 ms | ~22% | ~20 MB |
| **Kafka (acks=0)** | 1000.00 | 1.15% | 1.80s | 9.20 ms | ~18% | ~310 MB |
| **Kafka (acks=1)** | 1000.00 | 0.00% | 2.12s | 14.50 ms | ~24% | ~325 MB |
| **Kafka (acks=all)** | 1000.00 | 0.00% | 2.85s | 28.20 ms | ~32% | ~350 MB |

---

## 3. Odgovori na Inženjerska Pitanja

### 1. Zašto je MQTT idealan za postavljanje na samim edge uređajima (senzorima), a zašto postaje neadekvatan kada nam je potrebna istorijska analitika velikih podataka?

**Zašto je MQTT idealan za Edge:**
- **Ekstremno nizak overhead**: MQTT zaglavlje poruke može biti veliko svega 2 bajta, što štedi mrežni protok i bateriju na senzorima.
- **Jednostavnost protokola**: Klijentske biblioteke su male i zahtevaju minimalne procesorske i memorijske resurse (mogu raditi na bazičnim mikrokontrolerima poput ESP32 sa par kilobajta RAM-a).
- **Push model komunikacije**: Senzor ne mora stalno slati upite brokeru; broker gura poruke aktivnim klijentima samo kada se dogodi promena.

**Zašto je neadekvatan za Istorijsku Analitiku:**
- **Nema trajnog skladištenja (No Persistence by Design)**: MQTT broker je dizajniran kao tranzitna stanica. Poruka se prosleđuje aktivnim pretplatnicima i odmah briše iz memorije brokera. Broker ne čuva istoriju (osim jedne najnovije retained poruke po temi).
- **Nemogućnost rekapitulacije (No Replay)**: Ako se novi analitički servis poveže na broker, on ne može zatražiti "poruke poslate juče" niti pročitati podatke retroaktivno.
- **Teško skaliranje potrošača**: MQTT ne podržava nativno particionisanje tema sa raspodelom opterećenja potrošača na način na koji to radi Kafka (Consumer Groups).

---

### 2. Zašto Kafka dominira u data-intensive cloud sistemima, kolika je "cena" njene skalabilnosti u pogledu resursa i da li je realno pokretati je na hardverski ograničenim edge serverima?

**Zašto Kafka dominira u Cloudu:**
- **Distribuirani Commit Log**: Podaci se trajno upisuju na disk u sekvencijalni fajl (brz upis i čitanje zahvaljujući OS page cache-u i zero-copy tehnologiji).
- **Istorijski Replay i Pull model**: Potrošači sami povlače podatke brzinom koja im odgovara i mogu u bilo kom trenutku pomeriti svoj offset unazad da ponovo obrade istorijske podatke.
- **Skalabilnost kroz particionisanje**: Jedna tema se deli na više particija koje se raspoređuju na više brokera u klasteru, čime se postiže ogroman protok (milioni poruka u sekundi) bez zagušenja.

**Resursna cena skalabilnosti:**
- **Visok memorijski otisak**: Kafka radi na JVM-u (Java Virtual Machine), što zahteva dosta RAM-a. Takođe, oslanja se na slobodnu memoriju operativnog sistema za keširanje stranica diska (Page Cache).
- **Procesorski zahtevna**: KRaft koordinacioni mehanizam i stalna replikacija particija među brokerima troše znatne CPU resurse.
- Tipičan Kafka broker zahteva **najmanje 1-2 GB slobodnog RAM-a** samo za stabilan rad u praznom hodu, dok Mosquitto MQTT broker koristi **manje od 10 MB RAM-a**.

**Pokretanje na Edge serverima:**
- Pokretanje na hardverski vrlo ograničenim uređajima (kao što su Raspberry Pi Zero, pametni senzori ili ruteri) je **nerealno i neefikasno**.
- Pokretanje na jačim Edge gateway uređajima (industrijski računari sa 4-8 GB RAM-a) u jednonodnom **KRaft režimu** (kako je i konfigurisano u našem projektu) jeste izvodljivo i korisno ukoliko je potrebno lokalno skladištiti i garantovati isporuku podataka pre slanja u cloud, ali i dalje predstavlja značajan resursni teret u poređenju sa MQTT-om.

---

### 3. Analiza Scenarija B (Mrežni prekid i oporavak staka)

Tokom simulacije mrežnog prekida od 30 sekundi na simulatoru uređaja:
- **Kod MQTT-a (sa QoS 0)**: Simulator je odbacivao poruke jer veza nije postojala, a broker nije čuvao sesiju. Nakon ponovnog povezivanja, prenos je nastavljen, ali su poruke nastale tokom tih 30 sekundi zauvek izgubljene. Kod QoS 1, klijent je akumulirao poruke u memorijskom baferu i nakon uspostavljanja veze ih poslao odjednom (burst). Međutim, ako bi se klijent ugasio tokom prekida, te poruke bi nestale.
- **Kod Kafke**: Kafka klijent ima ugrađen interni bafer (`buffer.memory`). Tokom prekida mreže, simulator je bez prekida generisao poruke i smeštao ih u bafer. Kada je veza ponovo uspostavljena, Kafka klijent je u velikim paketima (batch) poslao sve nagomilane poruke brokeru bez ikakvog gubitka podataka. Potrošač je zahvaljujući offset mehanizmu nastavio tačno tamo gde je stao, garantujući *at-least-once* isporuku čak i u nestabilnim mrežnim uslovima.

---

## 4. Zaključak: Donošenje Odluke o Izboru (Trade-off)

- **Izaberite MQTT**: Kada radite sa mikrokontrolerima, jeftinim senzorima sa baterijskim napajanjem i nestabilnim mobilnim mrežama (2G/3G/4G) gde je bitna ušteda protoka i resursa, a istorijski podaci se odmah skladište u neku bazu sa strane.
- **Izaberite Kafku**: Kada razvijate data-intensive sisteme u cloudu, gde stotine mikroservisa treba da konzumiraju i analiziraju iste tokove podataka, gde je istorijska analitika ključna i gde je gubitak bilo koje poruke neprihvatljiv.
