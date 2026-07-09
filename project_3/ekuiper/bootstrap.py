import json
import os
import sys
import time
from pathlib import Path
from urllib import error, request


BASE_DIR = Path(__file__).resolve().parent
EKUIPER_URL = os.getenv("EKUIPER_URL", "http://ekuiper:9081")
STREAM_SQL = (BASE_DIR / "rfid_stream.sql").read_text(encoding="utf-8").strip()
RULE_DEFINITION = json.loads((BASE_DIR / "unauthorized_access_rule.json").read_text(encoding="utf-8"))


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


def create_stream() -> None:
    try:
        status, body = post_json(f"{EKUIPER_URL}/streams", {"sql": STREAM_SQL})
        print(f"Stream create response: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if "already exists" in body.lower():
            print("RFID stream already exists.")
            return
        raise RuntimeError(f"Failed to create stream: {exc.code} {body}") from exc


def create_rule() -> None:
    try:
        status, body = post_json(f"{EKUIPER_URL}/rules", RULE_DEFINITION)
        print(f"Rule create response: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if "already exists" in body.lower():
            print("RFID unauthorized access rule already exists.")
            return
        raise RuntimeError(f"Failed to create rule: {exc.code} {body}") from exc


def start_rule() -> None:
    try:
        status, body = post_empty(f"{EKUIPER_URL}/rules/{RULE_DEFINITION['id']}/start")
        print(f"Rule start response: {status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        if "already running" in body.lower():
            print("RFID unauthorized access rule is already running.")
            return
        raise RuntimeError(f"Failed to start rule: {exc.code} {body}") from exc


def main() -> int:
    try:
        wait_for_ekuiper()
        create_stream()
        create_rule()
        start_rule()
        return 0
    except Exception as exc:
        print(f"eKuiper bootstrap failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
