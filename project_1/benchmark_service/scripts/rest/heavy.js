import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 10,
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const url = "http://rest-service:8080/api/events/heavy?fromDate=2026-01-01T00:00:00Z&pageSize=50";
  const res = http.get(url, {
    headers: { Accept: "application/json" },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
    "body is not empty": (r) => r.body.length > 0,
  });
}
