import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 10,
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const query = `
    query HeavyEvents {
      heavyEvents(fromDate: "2026-01-01T00:00:00Z", pageSize: 50) {
        deviceId
        eventType
        zone
        count
        averageResponseTimeMs
        averageBatteryVoltage
      }
    }
  `;

  const res = http.post("http://graphql-service:4000/graphql", JSON.stringify({ query }), {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
    "no graphql errors": (r) => !r.json("errors"),
    "results returned": (r) => r.json("data.heavyEvents").length > 0,
  });
}
