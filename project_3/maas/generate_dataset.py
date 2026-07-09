import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pandas as pd


MAAS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MAAS_DIR.parent
GENERATOR_DIR = PROJECT_ROOT / "generator"
if str(GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATOR_DIR))

from generator import generate_event  # noqa: E402


DATASET_PATH = MAAS_DIR / "dataset.csv"


def safe_event_hour(timestamp: str) -> int:
    try:
        normalized = timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).hour
    except ValueError:
        return 12


def risk_score(row: dict) -> int:
    score = 0

    if row["door_id"] == "SERVER_ROOM":
        score += 2
    if row["zone"] in {"SECOND_FLOOR", "LAB_BLOCK"}:
        score += 1
    if row["signal_strength"] <= -72:
        score += 1
    if row["response_time_ms"] >= 130:
        score += 1
    if row["battery_voltage"] <= 3.4:
        score += 1
    if row["event_hour"] <= 5 or row["event_hour"] >= 22:
        score += 1
    if row["previous_failed_attempts"] >= 3:
        score += 2
    elif row["previous_failed_attempts"] >= 1:
        score += 1

    return score


def label_from_score(score: int) -> str:
    if score >= 5:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"


def build_dataset(samples: int) -> pd.DataFrame:
    rows = []
    failed_attempts_by_card: dict[str, int] = {}
    historical_start = datetime.now(timezone.utc) - timedelta(minutes=samples)

    for index in range(samples):
        event = generate_event()
        event_timestamp = historical_start + timedelta(minutes=index)
        event["timestamp"] = event_timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")
        previous_failed_attempts = failed_attempts_by_card.get(event["card_uid"], 0)
        event_hour = safe_event_hour(event["timestamp"])

        row = {
            "event_id": event["event_id"],
            "timestamp": event["timestamp"],
            "device_id": event["device_id"],
            "card_uid": event["card_uid"],
            "access_granted": event["access_granted"],
            "door_id": event["door_id"],
            "zone": event["zone"],
            "signal_strength": event["signal_strength"],
            "battery_voltage": event["battery_voltage"],
            "response_time_ms": event["response_time_ms"],
            "event_type": event["event_type"],
            "event_hour": event_hour,
            "previous_failed_attempts": previous_failed_attempts,
        }
        row["risk_level"] = label_from_score(risk_score(row))
        rows.append(row)

        if not event["access_granted"]:
            failed_attempts_by_card[event["card_uid"]] = previous_failed_attempts + 1

    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a historical RFID dataset for MaaS training.")
    parser.add_argument("--samples", type=int, default=10000, help="Number of RFID events to generate.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = build_dataset(args.samples)
    dataset.to_csv(DATASET_PATH, index=False)

    print(f"Saved dataset to {DATASET_PATH}")
    print(f"Samples: {len(dataset)}")
    print("Risk distribution:")
    print(dataset["risk_level"].value_counts().sort_index().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
