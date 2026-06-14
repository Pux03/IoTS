import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 10,
  duration: __ENV.DURATION || "30s",
};

function randomHex(size) {
  const chars = "0123456789abcdef";
  let value = "";
  for (let i = 0; i < size; i += 1) {
    value += chars[Math.floor(Math.random() * chars.length)];
  }
  return value;
}

function uuid() {
  return `${randomHex(8)}-${randomHex(4)}-${randomHex(4)}-${randomHex(4)}-${randomHex(12)}`;
}

export default function () {
  const payload = JSON.stringify({
    event_id: uuid(),
    timestamp: new Date().toISOString(),
    device_id: `RFID-REST-${__VU}`,
    card_uid: randomHex(8).toUpperCase(),
    access_granted: Math.random() > 0.1,
    door_id: "MAIN_GATE",
    zone: "GROUND_FLOOR",
    signal_strength: -50 - Math.floor(Math.random() * 25),
    battery_voltage: 3.6 + Math.random() * 0.4,
    response_time_ms: 10 + Math.floor(Math.random() * 120),
    event_type: "RFID_SCAN",
    temperature: 21 + Math.random() * 5,
  });

  const res = http.post("http://rest-service:8080/api/events", payload, {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is success": (r) => r.status >= 200 && r.status < 300,
  });
}
