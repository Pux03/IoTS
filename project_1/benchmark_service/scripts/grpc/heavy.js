import grpc from "k6/net/grpc";
import { check } from "k6";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 10,
  duration: __ENV.DURATION || "30s",
};

const client = new grpc.Client();
client.load(["../../protos"], "access_control_system.proto");

export default function () {
  client.connect("grpc-service:8080", { plaintext: true });

  const response = client.invoke("accesscontrol.AccessControlService/GetHeavyQuery", {
    from_date: "2026-01-01T00:00:00Z",
    page_size: 50,
  });

  check(response, {
    "status is OK": (r) => r && r.status === grpc.StatusOK,
    "results returned": (r) => r && r.message.results.length > 0,
  });

  client.close();
}
