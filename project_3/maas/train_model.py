from pathlib import Path

from joblib import dump
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


MAAS_DIR = Path(__file__).resolve().parent
DATASET_PATH = MAAS_DIR / "dataset.csv"
MODEL_PATH = MAAS_DIR / "model.pkl"
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
TARGET_COLUMN = "risk_level"
CATEGORICAL_COLUMNS = ["zone", "door_id"]


def aggregate_feature_importances(model: Pipeline) -> list[tuple[str, float]]:
    preprocessor = model.named_steps["preprocessor"]
    classifier = model.named_steps["classifier"]
    transformed_names = preprocessor.get_feature_names_out()
    aggregated: dict[str, float] = {}

    for name, importance in zip(transformed_names, classifier.feature_importances_):
        source_name = name.split("__", 1)[1] if "__" in name else name
        base_name = source_name

        for column in CATEGORICAL_COLUMNS:
            prefix = f"{column}_"
            if source_name.startswith(prefix):
                base_name = column
                break

        aggregated[base_name] = aggregated.get(base_name, 0.0) + float(importance)

    return sorted(aggregated.items(), key=lambda item: item[1], reverse=True)


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_COLUMNS),
        ],
        remainder="passthrough",
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=200,
                    max_depth=10,
                    random_state=42,
                    class_weight="balanced",
                ),
            ),
        ]
    )


def main() -> int:
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Missing dataset at {DATASET_PATH}. Run generate_dataset.py before training."
        )

    dataset = pd.read_csv(DATASET_PATH)
    features = dataset[FEATURE_COLUMNS]
    target = dataset[TARGET_COLUMN]

    x_train, x_test, y_train, y_test = train_test_split(
        features,
        target,
        test_size=0.2,
        random_state=42,
        stratify=target,
    )

    evaluation_model = build_pipeline()
    evaluation_model.fit(x_train, y_train)
    predictions = evaluation_model.predict(x_test)

    print(f"Loaded dataset from {DATASET_PATH}")
    print(f"Samples: {len(dataset)}")
    print(f"Accuracy: {accuracy_score(y_test, predictions):.4f}")
    print("Classification report:")
    print(classification_report(y_test, predictions))

    final_model = build_pipeline()
    final_model.fit(features, target)
    print("Aggregated feature importances:")
    for feature_name, importance in aggregate_feature_importances(final_model):
        print(f"  {feature_name}: {importance:.4f}")
    dump(final_model, MODEL_PATH)
    print(f"Saved trained model to {MODEL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
