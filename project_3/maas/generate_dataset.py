import argparse
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys

import pandas as pd


MAAS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MAAS_DIR.parent
GENERATOR_DIR = PROJECT_ROOT / "generator"
if str(GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATOR_DIR))

from generator import DEVICE_IDS, DENIED_EVENT_TYPES, generate_event  # noqa: E402


DATASET_PATH = MAAS_DIR / "dataset.csv"
SINGLE_EVENT_SPACING = timedelta(minutes=1)
BURST_EVENT_SPACING = timedelta(seconds=2)
POST_BURST_SPACING = timedelta(seconds=45)


def safe_event_hour(timestamp: str) -> int:
    try:
        normalized = timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).hour
    except ValueError:
        return 12


def rolling_average(values: deque[int], fallback: int) -> float:
    if not values:
        return float(fallback)
    return round(sum(values) / len(values), 2)


def rolling_denial_rate(values: deque[int]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 4)


def risk_score(row: dict) -> int:
    score = 0

    if row["door_id"] == "SERVER_ROOM":
        score += 2
    if row["zone"] in {"SECOND_FLOOR", "LAB_BLOCK"}:
        score += 1
    if row["min_signal_strength"] <= -72:
        score += 1
    if row["avg_response_time_ms"] >= 130:
        score += 1
    if row["avg_response_time_last5"] >= 120:
        score += 1
    if row["avg_battery_voltage"] <= 3.4:
        score += 1
    if row["event_hour"] <= 5 or row["event_hour"] >= 22:
        score += 1
    if row["previous_failed_attempts"] >= 3:
        score += 2
    elif row["previous_failed_attempts"] >= 1:
        score += 1
    if row["denial_rate_last10"] >= 0.6:
        score += 2
    elif row["denial_rate_last10"] >= 0.3:
        score += 1
    if row["attempt_count"] >= 12:
        score += 3
    elif row["attempt_count"] >= 8:
        score += 2
    elif row["attempt_count"] >= 5:
        score += 1

    return score


def label_from_score(score: int) -> str:
    if score >= 5:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"


def apply_event_history(
    event: dict,
    failed_attempts_by_card: dict[str, int],
    response_history_by_device: dict[str, deque[int]],
    denial_history_by_device: dict[str, deque[int]],
) -> None:
    if not event["access_granted"]:
        failed_attempts_by_card[event["card_uid"]] = failed_attempts_by_card.get(event["card_uid"], 0) + 1

    response_history_by_device[event["device_id"]].append(int(event["response_time_ms"]))
    denial_history_by_device[event["device_id"]].append(0 if event["access_granted"] else 1)


def build_single_row(
    event: dict,
    previous_failed_attempts: int,
    avg_response_time_last5: float,
    denial_rate_last10: float,
) -> dict:
    row = {
        "sample_kind": "single_event",
        "event_id": event["event_id"],
        "timestamp": event["timestamp"],
        "device_id": event["device_id"],
        "card_uid": event["card_uid"],
        "access_granted": event["access_granted"],
        "door_id": event["door_id"],
        "zone": event["zone"],
        "event_type": event["event_type"],
        "attempt_count": 1,
        "avg_response_time_ms": float(event["response_time_ms"]),
        "min_signal_strength": int(event["signal_strength"]),
        "avg_battery_voltage": float(event["battery_voltage"]),
        "event_hour": safe_event_hour(event["timestamp"]),
        "previous_failed_attempts": previous_failed_attempts,
        "avg_response_time_last5": avg_response_time_last5,
        "denial_rate_last10": denial_rate_last10,
    }
    row["risk_level"] = label_from_score(risk_score(row))
    return row


def create_burst_events(device_id: str, start_time: datetime) -> list[dict]:
    burst_size = random.randint(3, 15)
    template_event = generate_event(device_id=device_id)
    card_uid = template_event["card_uid"]
    door_id = template_event["door_id"]
    zone = template_event["zone"]
    events: list[dict] = []

    for index in range(burst_size):
        event = generate_event(device_id=device_id)
        event["timestamp"] = (
            start_time + (index * BURST_EVENT_SPACING)
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        event["card_uid"] = card_uid
        event["door_id"] = door_id
        event["zone"] = zone
        event["access_granted"] = False
        event["event_type"] = random.choice(DENIED_EVENT_TYPES)
        event["signal_strength"] = min(int(event["signal_strength"]), random.randint(-82, -54))
        event["response_time_ms"] = max(int(event["response_time_ms"]), random.randint(70, 185))
        event["battery_voltage"] = round(min(float(event["battery_voltage"]), random.uniform(3.1, 3.95)), 2)
        events.append(event)

    return events


def build_burst_row(
    events: list[dict],
    response_history_by_device: dict[str, deque[int]],
    denial_history_by_device: dict[str, deque[int]],
) -> dict:
    last_event = events[-1]
    device_id = last_event["device_id"]
    row = {
        "sample_kind": "brute_force_burst",
        "event_id": last_event["event_id"],
        "timestamp": last_event["timestamp"],
        "device_id": device_id,
        "card_uid": last_event["card_uid"],
        "access_granted": False,
        "door_id": last_event["door_id"],
        "zone": last_event["zone"],
        "event_type": "BRUTE_FORCE_ATTEMPT",
        "attempt_count": len(events),
        "avg_response_time_ms": round(
            sum(int(event["response_time_ms"]) for event in events) / len(events),
            2,
        ),
        "min_signal_strength": min(int(event["signal_strength"]) for event in events),
        "avg_battery_voltage": round(
            sum(float(event["battery_voltage"]) for event in events) / len(events),
            2,
        ),
        "event_hour": safe_event_hour(last_event["timestamp"]),
        "previous_failed_attempts": max(len(events) - 1, 0),
        "avg_response_time_last5": rolling_average(
            response_history_by_device[device_id],
            int(last_event["response_time_ms"]),
        ),
        "denial_rate_last10": rolling_denial_rate(denial_history_by_device[device_id]),
    }
    row["risk_level"] = label_from_score(risk_score(row))
    return row


def should_generate_burst(
    row_index: int,
    samples: int,
    target_burst_rows: int,
    generated_burst_rows: int,
) -> bool:
    remaining_rows = samples - row_index
    remaining_bursts = target_burst_rows - generated_burst_rows

    if remaining_bursts <= 0:
        return False

    if remaining_rows <= remaining_bursts:
        return True

    return random.random() < (remaining_bursts / remaining_rows)


def build_dataset(samples: int, burst_ratio: float) -> pd.DataFrame:
    rows = []
    failed_attempts_by_card: dict[str, int] = {}
    response_history_by_device: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=5))
    denial_history_by_device: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=10))
    generated_burst_rows = 0
    target_burst_rows = max(1, round(samples * burst_ratio))
    time_cursor = datetime.now(timezone.utc) - timedelta(minutes=samples)

    for row_index in range(samples):
        if should_generate_burst(row_index, samples, target_burst_rows, generated_burst_rows):
            burst_events = create_burst_events(random.choice(DEVICE_IDS), time_cursor)
            for event in burst_events:
                apply_event_history(
                    event,
                    failed_attempts_by_card,
                    response_history_by_device,
                    denial_history_by_device,
                )

            rows.append(build_burst_row(burst_events, response_history_by_device, denial_history_by_device))
            generated_burst_rows += 1
            last_burst_timestamp = datetime.fromisoformat(
                burst_events[-1]["timestamp"].replace("Z", "+00:00")
            )
            time_cursor = last_burst_timestamp + POST_BURST_SPACING
            continue

        event = generate_event()
        event["timestamp"] = time_cursor.isoformat(timespec="seconds").replace("+00:00", "Z")
        previous_failed_attempts = failed_attempts_by_card.get(event["card_uid"], 0)
        avg_response_time_last5 = rolling_average(
            response_history_by_device[event["device_id"]],
            int(event["response_time_ms"]),
        )
        denial_rate_last10 = rolling_denial_rate(denial_history_by_device[event["device_id"]])

        rows.append(
            build_single_row(
                event,
                previous_failed_attempts,
                avg_response_time_last5,
                denial_rate_last10,
            )
        )
        apply_event_history(event, failed_attempts_by_card, response_history_by_device, denial_history_by_device)
        time_cursor += SINGLE_EVENT_SPACING

    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a historical RFID dataset for MaaS training.")
    parser.add_argument("--samples", type=int, default=10000, help="Number of RFID events to generate.")
    parser.add_argument(
        "--burst-ratio",
        type=float,
        default=0.18,
        help="Share of dataset rows that should simulate brute-force burst scenarios.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = build_dataset(args.samples, args.burst_ratio)
    dataset.to_csv(DATASET_PATH, index=False)

    print(f"Saved dataset to {DATASET_PATH}")
    print(f"Samples: {len(dataset)}")
    print("Sample kind distribution:")
    print(dataset["sample_kind"].value_counts().sort_index().to_string())
    print("Attempt count distribution (top 10):")
    print(dataset["attempt_count"].value_counts().sort_index().tail(10).to_string())
    print("Risk distribution:")
    print(dataset["risk_level"].value_counts().sort_index().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
