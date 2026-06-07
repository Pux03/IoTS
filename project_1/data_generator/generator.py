import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_batch

# =====================================
# DATABASE CONFIG
# =====================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "access_control_system",
    "user": "admin",
    "password": "admin"
}

# =====================================
# GENERATOR CONFIG
# =====================================

EVENTS_TO_GENERATE = 100000
BATCH_SIZE = 1000

DEVICES = [
    "RFID-ENT-01",
    "RFID-ENT-02",
    "RFID-WH-01",
    "RFID-OFFICE-01",
    "RFID-LAB-01",
]

DOORS = [
    "MAIN_GATE",
    "GARAGE",
    "SERVER_ROOM",
    "OFFICE_A",
    "OFFICE_B",
    "LAB",
    "WAREHOUSE",
]

ZONES = {
    "MAIN_GATE": "GROUND_FLOOR",
    "GARAGE": "GARAGE",
    "SERVER_ROOM": "SECOND_FLOOR",
    "OFFICE_A": "FIRST_FLOOR",
    "OFFICE_B": "FIRST_FLOOR",
    "LAB": "LAB_BLOCK",
    "WAREHOUSE": "WAREHOUSE",
}

# =====================================
# HELPERS
# =====================================

def generate_uid():
    return ":".join(
        f"{random.randint(0, 255):02X}"
        for _ in range(4)
    )

def random_timestamp():
    now = datetime.utcnow()

    delta = timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    return now - delta

def generate_event():
    door = random.choice(DOORS)

    # server room ima više odbijanja
    if door == "SERVER_ROOM":
        access_granted = random.random() < 0.65
    else:
        access_granted = random.random() < 0.9

    if access_granted:
        event_type = random.choice([
            "ENTRY",
            "EXIT"
        ])
    else:
        event_type = random.choice([
            "ACCESS_DENIED",
            "FORCED_OPEN"
        ])

    return (
        str(uuid.uuid4()),               # event_id
        random_timestamp(),             # timestamp
        random.choice(DEVICES),         # device_id
        generate_uid(),                 # card_uid
        access_granted,                 # access_granted
        door,                           # door_id
        ZONES[door],                    # zone
        random.randint(-90, -30),       # signal_strength
        round(random.uniform(3.4, 4.2), 2),  # battery_voltage
        random.randint(10, 300),        # response_time_ms
        event_type,                     # event_type
        round(random.uniform(18, 40), 1)  # temperature
    )

# =====================================
# MAIN
# =====================================

print("Connecting to PostgreSQL...")

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print("Connected.")
print(f"Generating and inserting {EVENTS_TO_GENERATE} events...")

insert_query = """
INSERT INTO events (
    event_id,
    timestamp,
    device_id,
    card_uid,
    access_granted,
    door_id,
    zone,
    signal_strength,
    battery_voltage,
    response_time_ms,
    event_type,
    temperature
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
"""

batch = []

for i in range(EVENTS_TO_GENERATE):

    batch.append(generate_event())

    if len(batch) >= BATCH_SIZE:
        execute_batch(cursor, insert_query, batch)

        conn.commit()

        print(f"Inserted {i + 1} events")

        batch.clear()

# insert remaining
if batch:
    execute_batch(cursor, insert_query, batch)
    conn.commit()

cursor.close()
conn.close()

print("DONE.")