const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const app = express();
app.use(express.json());

const allowedOrigins = [
  "http://localhost:5173",
  "http://127.0.0.1:5173",
];

app.use(
  cors({
    origin: allowedOrigins,
  }),
);

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
  },
});

io.on("connection", (socket) => {
  console.log("Frontend connected");
});

function pickMetric(summary, name) {
  const metric = summary.metrics?.[name];
  if (!metric) return null;

  return {
    avg: metric.avg ?? null,
    min: metric.min ?? null,
    med: metric.med ?? null,
    max: metric.max ?? null,
    p90: metric["p(90)"] ?? null,
    p95: metric["p(95)"] ?? null,
    rate: metric.rate ?? null,
    count: metric.count ?? null,
    passes: metric.passes ?? null,
    fails: metric.fails ?? null,
    value: metric.value ?? null,
  };
}

function parseK6Summary(summary, meta) {
  const httpDuration = pickMetric(summary, "http_req_duration");
  const grpcDuration = pickMetric(summary, "grpc_req_duration");
  const iterationDuration = pickMetric(summary, "iteration_duration");
  const requests = pickMetric(summary, "http_reqs") || pickMetric(summary, "grpc_reqs");
  const checks = pickMetric(summary, "checks");
  const dataReceived = pickMetric(summary, "data_received");
  const dataSent = pickMetric(summary, "data_sent");
  const vus = pickMetric(summary, "vus");
  const vusMax = pickMetric(summary, "vus_max");

  return {
    ...meta,
    finishedAt: new Date().toISOString(),
    state: "completed",
    latency: {
      primary: httpDuration ? "http_req_duration" : grpcDuration ? "grpc_req_duration" : "iteration_duration",
      avgMs: httpDuration?.avg ?? grpcDuration?.avg ?? iterationDuration?.avg ?? null,
      p95Ms: httpDuration?.p95 ?? grpcDuration?.p95 ?? iterationDuration?.p95 ?? null,
      maxMs: httpDuration?.max ?? grpcDuration?.max ?? iterationDuration?.max ?? null,
    },
    throughput: {
      rps: requests?.rate ?? null,
      totalRequests: requests?.count ?? null,
      iterationsPerSecond: summary.metrics?.iterations?.rate ?? null,
      totalIterations: summary.metrics?.iterations?.count ?? null,
    },
    checks: {
      rate: checks?.rate ?? null,
      passes: checks?.passes ?? null,
      fails: checks?.fails ?? null,
    },
    network: {
      dataReceivedBytes: dataReceived?.count ?? null,
      dataSentBytes: dataSent?.count ?? null,
    },
    vus: {
      current: vus?.value ?? null,
      max: vusMax?.value ?? Number(meta.vus),
    },
    rawMetrics: summary.metrics,
  };
}

app.post("/benchmark/start", (req, res) => {
  const { protocol, scenario, vus, duration } = req.body;
  const allowedProtocols = new Set(["rest", "grpc", "graphql"]);
  const allowedScenarios = new Set(["high-frequency", "selective", "heavy"]);

  if (!allowedProtocols.has(protocol) || !allowedScenarios.has(scenario)) {
    return res.status(400).json({ error: "Invalid protocol or scenario." });
  }

  console.log("Starting k6:", req.body);

  const startedAt = new Date().toISOString();
  const summaryPath = path.join(
    os.tmpdir(),
    `k6-summary-${protocol}-${scenario}-${Date.now()}.json`,
  );

  io.emit("benchmark-result", {
    protocol,
    scenario,
    vus,
    duration,
    startedAt,
    state: "running",
  });

  const k6 = spawn("k6", [
    "run",
    `scripts/${protocol}/${scenario}.js`,
    "--summary-export",
    summaryPath,
    "-e",
    `VUS=${vus}`,
    "-e",
    `DURATION=${duration}`,
  ]);

  k6.stdout.on("data", (data) => {
    const msg = data.toString();
    console.log(msg);
    io.emit("benchmark-log", msg);
  });

  k6.stderr.on("data", (data) => {
    const msg = data.toString();
    console.error(msg);
    io.emit("benchmark-log", msg);
  });

  k6.on("close", () => {
    io.emit("benchmark-log", "TEST FINISHED");

    try {
      const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
      const result = parseK6Summary(summary, {
        protocol,
        scenario,
        vus,
        duration,
        startedAt,
      });
      io.emit("benchmark-result", result);
      fs.unlink(summaryPath, () => {});
    } catch (error) {
      io.emit("benchmark-result", {
        protocol,
        scenario,
        vus,
        duration,
        startedAt,
        finishedAt: new Date().toISOString(),
        state: "failed",
        error: error.message,
      });
    }
  });

  res.json({ status: "started" });
});

server.listen(3000, () => {
  console.log("Benchmark service running on port 3000");
});
