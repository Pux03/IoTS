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
  const query = `
    mutation CreateEvent($input: EventInput!) {
      createEvent(input: $input) {
        id
        eventId
      }
    }
  `;

  const variables = {
    input: {
      eventId: uuid(),
      timestamp: new Date().toISOString(),
      deviceId: `RFID-GQL-${__VU}`,
      cardUid: randomHex(8).toUpperCase(),
      accessGranted: Math.random() > 0.1,
      doorId: "MAIN_GATE",
      zone: "GROUND_FLOOR",
      signalStrength: -50 - Math.floor(Math.random() * 25),
      batteryVoltage: 3.6 + Math.random() * 0.4,
      responseTimeMs: 10 + Math.floor(Math.random() * 120),
      eventType: "RFID_SCAN",
      temperature: 21 + Math.random() * 5,
    },
  };

  const res = http.post("http://graphql-service:4000/graphql", JSON.stringify({ query, variables }), {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
    "no graphql errors": (r) => !r.json("errors"),
  });
}
