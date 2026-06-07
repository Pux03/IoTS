import random
import uuid
from datetime import datetime

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

def generate_uid():
    return ":".join(
        f"{random.randint(0, 255):02X}"
        for _ in range(4)
    )

def generate_event(device_id=None, critical_temp=False):
    door = random.choice(DOORS)

    # server room has more denials
    if door == "SERVER_ROOM":
        access_granted = random.random() < 0.65
    else:
        access_granted = random.random() < 0.9

    if access_granted:
        event_type = random.choice(["ENTRY", "EXIT"])
    else:
        event_type = random.choice(["ACCESS_DENIED", "FORCED_OPEN"])

    # If critical_temp is True, we generate temp > 50
    if critical_temp:
        temp = round(random.uniform(51.0, 65.0), 1)
    else:
        temp = round(random.uniform(18.0, 40.0), 1)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "device_id": device_id if device_id else random.choice(DEVICES),
        "card_uid": generate_uid(),
        "access_granted": access_granted,
        "door_id": door,
        "zone": ZONES[door],
        "signal_strength": int(random.randint(-90, -30)),
        "battery_voltage": float(round(random.uniform(3.4, 4.2), 2)),
        "response_time_ms": int(random.randint(10, 300)),
        "event_type": event_type,
        "temperature": float(temp)
    }
