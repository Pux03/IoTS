import json
import os
import sys
import time
from pathlib import Path
from urllib import error, request


BASE_DIR = Path(__file__).resolve().parent
EKUIPER_URL = os.getenv("EKUIPER_URL", "http://ekuiper:9081")
STREAM_NAME = "rfid_events"
STREAM_SQL = (BASE_DIR / "rfid_stream.sql").read_text(encoding="utf-8").strip()
RULE_FILES = [
    "unauthorized_access_rule.json",
    "brute_force_rule.json",
]
RULE_DEFINITIONS = [
    json.loads((BASE_DIR / rule_file).read_text(encoding="utf-8"))
    for rule_file in RULE_FILES
]


def get(url: str) -> tuple[int, str]:
    req = request.Request(url, method="GET")
    with request.urlopen(req, timeout=5) as response:
        return response.status, response.read().decode("utf-8")


def post_json(url: str, payload: dict) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=10) as response:
        return response.status, response.read().decode("utf-8")


def post_empty(url: str) -> tuple[int, str]:
    req = request.Request(url, data=b"", method="POST")
    with request.urlopen(req, timeout=10) as response:
        return response.status, response.read().decode("utf-8")


def delete(url: str) -> tuple[int, str]:
    req = request.Request(url, method="DELETE")
    with request.urlopen(req, timeout=10) as response:
        return response.status, response.read().decode("utf-8")


def wait_for_ekuiper() -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            status, _ = get(f"{EKUIPER_URL}/streams")
            if status == 200:
                print("eKuiper REST API is ready.")
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Timed out while waiting for eKuiper REST API.")


def delete_rule(rule_definition: dict) -> None:
    rule_id = rule_definition["id"]
    try:
        status, body = delete(f"{EKUIPER_URL}/rules/{rule_id}")
        print(f"Rule delete response for {rule_id}: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if exc.code == 404 or "not found" in body.lower():
            print(f"Rule {rule_id} does not exist yet.")
            return
        raise RuntimeError(f"Failed to delete rule: {exc.code} {body}") from exc


def delete_stream() -> None:
    try:
        status, body = delete(f"{EKUIPER_URL}/streams/{STREAM_NAME}")
        print(f"Stream delete response: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if exc.code == 404 or "not found" in body.lower():
            print("RFID stream does not exist yet.")
            return
        raise RuntimeError(f"Failed to delete stream: {exc.code} {body}") from exc


def create_stream() -> None:
    try:
        status, body = post_json(f"{EKUIPER_URL}/streams", {"sql": STREAM_SQL})
        print(f"Stream create response: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"Failed to create stream: {exc.code} {body}") from exc


def create_rule(rule_definition: dict) -> None:
    rule_id = rule_definition["id"]
    try:
        status, body = post_json(f"{EKUIPER_URL}/rules", rule_definition)
        print(f"Rule create response for {rule_id}: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"Failed to create rule: {exc.code} {body}") from exc


def start_rule(rule_definition: dict) -> None:
    rule_id = rule_definition["id"]
    try:
        status, body = post_empty(f"{EKUIPER_URL}/rules/{rule_id}/start")
        print(f"Rule start response for {rule_id}: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if "already running" in body.lower():
            print(f"Rule {rule_id} is already running.")
            return
        raise RuntimeError(f"Failed to start rule: {exc.code} {body}") from exc


def main() -> int:
    try:
        wait_for_ekuiper()
        for rule_definition in RULE_DEFINITIONS:
            delete_rule(rule_definition)
        delete_stream()
        create_stream()
        for rule_definition in RULE_DEFINITIONS:
            create_rule(rule_definition)
            start_rule(rule_definition)
        return 0
    except Exception as exc:
        print(f"eKuiper bootstrap failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
