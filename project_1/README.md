# IoTS Project 1 - Synchronous Protocol Benchmark

This project compares REST, gRPC, and GraphQL in an IoT access-control telemetry system. The dataset represents RFID edge devices that emit time-series access events with timestamp, device id, RFID card id, access status, door/zone, signal strength, battery voltage, response time, event type, and temperature.

## Architecture

- `postgres`: shared PostgreSQL database with indexes optimized for IoT time-series access.
- `rest_service`: ASP.NET Core REST API with JSON responses and Swagger/OpenAPI.
- `grpc_service`: ASP.NET Core gRPC API with Protobuf messages.
- `graphql_service`: Node.js/Apollo GraphQL API with field selection to avoid over-fetching.
- `benchmark_service`: Node.js service that starts k6 scripts and streams logs to the frontend.
- `benchmark_frontend`: Vite/React benchmark dashboard.

## Database

The database is initialized by `database/init.sql` when the `postgres_data` volume is first created. It creates:

- `events` table
- `idx_events_timestamp`
- `idx_events_device_id`
- `idx_events_device_timestamp`
- `idx_events_event_type_timestamp`
- 20,000 seed IoT RFID events for immediate benchmark runs

To reset the seeded database:

```powershell
docker compose down -v
docker compose up --build
```

## Run

```powershell
docker compose up --build
```

Services:

- REST Swagger: `http://localhost:8080`
- gRPC: `localhost:50051`
- GraphQL: `http://localhost:4000/graphql`
- Benchmark service: `http://localhost:3000`
- Frontend: start separately from `benchmark_frontend` with `npm install` and `npm run dev`

## API Surface

REST:

- `GET /api/events?page=1&pageSize=50`
- `GET /api/events/filter?deviceId=RFID-ENT-01`
- `GET /api/events/selective?page=1&pageSize=50`
- `GET /api/events/heavy?fromDate=2026-01-01T00:00:00Z&pageSize=50`
- `POST /api/events`

gRPC service `accesscontrol.AccessControlService`:

- `GetEvent`
- `GetEvents`
- `GetSelectiveEvents`
- `GetHeavyQuery`
- `CreateEvent`

GraphQL:

- `events(page, pageSize, deviceId)`
- `event(id)`
- `selectiveEvents(page, pageSize)`
- `heavyEvents(deviceId, cardUid, eventType, fromDate, toDate, searchTerm, pageSize)`
- `createEvent(input)`

## Required IoT Scenarios

Scenario A - High-Frequency Ingestion:

- REST: `benchmark_service/scripts/rest/high-frequency.js`
- gRPC: `benchmark_service/scripts/grpc/high-frequency.js`
- GraphQL: `benchmark_service/scripts/graphql/high-frequency.js`

Scenario B - Selective Monitoring:

- REST: `benchmark_service/scripts/rest/selective.js`
- gRPC: `benchmark_service/scripts/grpc/selective.js`
- GraphQL: `benchmark_service/scripts/graphql/selective.js`

Scenario C - Heavy Querying:

- REST: `benchmark_service/scripts/rest/heavy.js`
- gRPC: `benchmark_service/scripts/grpc/heavy.js`
- GraphQL: `benchmark_service/scripts/graphql/heavy.js`

## k6 Evaluation

Run from the benchmark dashboard or directly inside the benchmark container:

```powershell
docker compose exec benchmark-service k6 run scripts/rest/high-frequency.js -e VUS=10 -e DURATION=30s
docker compose exec benchmark-service k6 run scripts/rest/high-frequency.js -e VUS=100 -e DURATION=30s
docker compose exec benchmark-service k6 run scripts/rest/high-frequency.js -e VUS=500 -e DURATION=30s
```

Repeat the command for each protocol and scenario. The required metrics are printed by k6:

- `http_req_duration`: average latency
- `http_req_duration{... p(95) ...}`: p95 latency
- `http_reqs`: request rate / RPS
- `checks`: successful logical checks

## Response Size Measurement

Use the same logical request for each protocol:

- REST selective: `GET http://localhost:8080/api/events/selective?page=1&pageSize=50`
- GraphQL selective:

```graphql
query {
  selectiveEvents(page: 1, pageSize: 50) {
    deviceId
    cardUid
  }
}
```

- gRPC selective: `GetSelectiveEvents { page: 1, page_size: 50 }`

For REST and GraphQL, Postman Console shows response body size in bytes. For gRPC, use Postman gRPC with reflection enabled on `localhost:50051`; the binary Protobuf payload is expected to be smaller than JSON for the same selected fields.

## CPU and RAM Measurement

Run this in another terminal while k6 is active:

```powershell
docker stats iot-rest-service iot-grpc-service iot-graphql-service iot-access-control-system-postgres iot-benchmark-service
```

Record CPU and memory for the same VU levels: 10, 100, and 500. Compare the serialization/deserialization cost of JSON REST, JSON GraphQL, and binary Protobuf gRPC.

## Results Table Template

| Protocol | Scenario | VUs | Avg latency | p95 latency | RPS | Response bytes | CPU | RAM |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| REST | High-frequency ingestion | 10 |  |  |  |  |  |  |
| gRPC | High-frequency ingestion | 10 |  |  |  |  |  |  |
| GraphQL | High-frequency ingestion | 10 |  |  |  |  |  |  |
| REST | Selective monitoring | 100 |  |  |  |  |  |  |
| gRPC | Selective monitoring | 100 |  |  |  |  |  |  |
| GraphQL | Selective monitoring | 100 |  |  |  |  |  |  |
| REST | Heavy querying | 500 |  |  |  |  |  |  |
| gRPC | Heavy querying | 500 |  |  |  |  |  |  |
| GraphQL | Heavy querying | 500 |  |  |  |  |  |  |

## Expected Analysis

- REST is easiest to consume and document with Swagger, but JSON payloads include field names and have higher payload overhead.
- gRPC should perform best for compact payload size and CPU efficiency because Protobuf is binary and schema-driven.
- GraphQL is useful in the selective monitoring scenario because the client asks only for `deviceId` and `cardUid`, avoiding over-fetching that would matter on weak mobile/edge links.
- Heavy querying is mostly database-bound; the database indexes on timestamp and device id are more important than protocol overhead for large historical ranges.
