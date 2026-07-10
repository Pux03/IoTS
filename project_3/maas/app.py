from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
import joblib
import pandas as pd
from pydantic import BaseModel


MODEL_PATH = Path(__file__).with_name("model.pkl")
FEATURE_COLUMNS = [
    "attempt_count",
    "avg_response_time_ms",
    "min_signal_strength",
    "avg_battery_voltage",
    "zone",
    "door_id",
    "event_hour",
    "previous_failed_attempts",
    "avg_response_time_last5",
    "denial_rate_last10",
]
RISK_LABELS = ["LOW", "MEDIUM", "HIGH"]

if not MODEL_PATH.exists():
    raise FileNotFoundError(
        f"Missing trained model at {MODEL_PATH}. Run generate_dataset.py and train_model.py first."
    )

model = joblib.load(MODEL_PATH)
app = FastAPI(title="RFID MaaS Service")


class RiskRequest(BaseModel):
    attempt_count: int = 1
    avg_response_time_ms: float
    min_signal_strength: int
    avg_battery_voltage: float
    zone: str
    door_id: str
    timestamp: str
    previous_failed_attempts: int = 0
    avg_response_time_last5: float | None = None
    denial_rate_last10: float | None = None


def safe_event_hour(timestamp: str) -> int:
    try:
        normalized = timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).hour
    except ValueError:
        return 12


def build_feature_frame(payload: RiskRequest) -> pd.DataFrame:
    avg_response_time_last5 = (
        float(payload.avg_response_time_last5)
        if payload.avg_response_time_last5 is not None
        else float(payload.avg_response_time_ms)
    )
    denial_rate_last10 = (
        float(payload.denial_rate_last10)
        if payload.denial_rate_last10 is not None
        else 0.0
    )

    return pd.DataFrame(
        [
            {
                "attempt_count": payload.attempt_count,
                "avg_response_time_ms": payload.avg_response_time_ms,
                "min_signal_strength": payload.min_signal_strength,
                "avg_battery_voltage": payload.avg_battery_voltage,
                "zone": payload.zone,
                "door_id": payload.door_id,
                "event_hour": safe_event_hour(payload.timestamp),
                "previous_failed_attempts": payload.previous_failed_attempts,
                "avg_response_time_last5": avg_response_time_last5,
                "denial_rate_last10": denial_rate_last10,
            }
        ],
        columns=FEATURE_COLUMNS,
    )


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "model": "RandomForestClassifier",
        "classes": RISK_LABELS,
        "model_path": MODEL_PATH.name,
        "model_loaded": True,
    }


@app.post("/predict")
def predict(payload: RiskRequest) -> dict:
    feature_frame = build_feature_frame(payload)
    prediction = model.predict(feature_frame)[0]
    return {"risk_level": str(prediction)}
