import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS) : 10,
  duration: __ENV.DURATION || "10s",
};

function generateRandomCardUid() {
  const chars = "0123456789ABCDEF";
  let uid = "";
  for (let i = 0; i < 8; i++) {
    uid += chars[Math.floor(Math.random() * chars.length)];
    if (i % 2 === 1 && i < 7) uid += " ";
  }
  return uid;
}

export default function () {
  const url = "http://rest-service:8080/api/events";

  const payload = JSON.stringify({
    device_id: `ESP32-NODE-${__VU}`,
    card_uid: generateRandomCardUid(),
    access_granted: Math.random() > 0.1,
    battery_voltage: 3.7,
    temperature: 22.5,
    signal_strength: -50,
    timestamp: new Date().toISOString(),
    event_type: "RFID_SCAN",
    zone: "Blok A",
    door_id: "DOOR-1",
    response_time_ms: 10,
    event_id: crypto.randomUUID(),
  });

  const res = http.post(url, payload, {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is success": (r) => r.status >= 200 && r.status < 300,
  });
}
