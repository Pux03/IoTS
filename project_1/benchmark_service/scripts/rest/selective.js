import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS) : 10,
  duration: __ENV.DURATION || "10s",
};

export default function () {
  // Definišemo parametre za paginaciju (npr. uvek vučemo prvu stranicu od 50 elemenata)
  const page = 1;
  const pageSize = 50;

  // URL sa query parametrima za tvoj selective endpoint
  const url = `http://rest-service:8080/api/events/selective?page=${page}&pageSize=${pageSize}`;

  // Šaljemo GET zahtev (kod GET-a nam ne treba payload/body)
  const res = http.get(url, {
    headers: { Accept: "application/json" },
  });

  // Provera da li je status uspešan (200 OK)
  check(res, {
    "status is 200": (r) => r.status === 200,
    "response body is not empty": (r) => r.body.length > 0,
  });
}
