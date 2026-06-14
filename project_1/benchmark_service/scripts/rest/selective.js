import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 10,
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const res = http.get("http://rest-service:8080/api/events/selective?page=1&pageSize=50", {
    headers: { Accept: "application/json" },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
    "body is not empty": (r) => r.body.length > 0,
  });
}
