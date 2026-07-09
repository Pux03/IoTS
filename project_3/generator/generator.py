import random
import uuid
from datetime import datetime, timezone


DEVICE_IDS = [
    "RFID-ENT-01",
    "RFID-ENT-02",
    "RFID-WH-01",
    "RFID-OFFICE-01",
    "RFID-LAB-01",
]

DOOR_TO_ZONE = {
    "MAIN_GATE": "GROUND_FLOOR",
    "GARAGE": "GARAGE",
    "SERVER_ROOM": "SECOND_FLOOR",
    "OFFICE_A": "FIRST_FLOOR",
    "OFFICE_B": "FIRST_FLOOR",
    "LAB": "LAB_BLOCK",
    "WAREHOUSE": "WAREHOUSE",
}

DOOR_IDS = list(DOOR_TO_ZONE.keys())
DENIED_EVENT_TYPES = ["ACCESS_DENIED", "CARD_UNKNOWN", "FORCED_OPEN"]
GRANTED_EVENT_TYPES = ["ENTRY", "EXIT"]


def generate_card_uid() -> str:
    return "".join(random.choices("0123456789ABCDEF", k=8))


def generate_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def choose_access_outcome(door_id: str) -> tuple[bool, str]:
    denial_probability = 0.18
    if door_id == "SERVER_ROOM":
        denial_probability = 0.42
    elif door_id in {"LAB", "WAREHOUSE"}:
        denial_probability = 0.24

    access_granted = random.random() >= denial_probability
    if access_granted:
        return True, random.choice(GRANTED_EVENT_TYPES)
    return False, random.choice(DENIED_EVENT_TYPES)


def generate_event(device_id: str | None = None) -> dict:
    door_id = random.choice(DOOR_IDS)
    access_granted, event_type = choose_access_outcome(door_id)
    zone = DOOR_TO_ZONE[door_id]

    signal_strength = random.randint(-78, -38)
    response_time_ms = random.randint(25, 180)
    battery_voltage = round(random.uniform(3.25, 4.15), 2)

    if not access_granted:
        signal_strength -= random.randint(0, 8)
        response_time_ms += random.randint(10, 60)
        battery_voltage = round(max(3.1, battery_voltage - random.uniform(0.0, 0.2)), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": generate_timestamp(),
        "device_id": device_id or random.choice(DEVICE_IDS),
        "card_uid": generate_card_uid(),
        "access_granted": access_granted,
        "door_id": door_id,
        "zone": zone,
        "signal_strength": signal_strength,
        "battery_voltage": battery_voltage,
        "response_time_ms": response_time_ms,
        "event_type": event_type,
    }
