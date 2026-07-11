function parseDecimal(value) {
  if (!value) return null;
  return Number(value.replace(",", "."));
}

function parseDurationToMs(value, unit) {
  const number = parseDecimal(value);
  if (number === null || Number.isNaN(number)) return null;

  switch (unit) {
    case "s":
      return number * 1000;
    case "us":
    case "µs":
      return number / 1000;
    case "ns":
      return number / 1000000;
    case "ms":
    default:
      return number;
  }
}

function parseBytes(value, unit) {
  const number = parseDecimal(value);
  if (number === null || Number.isNaN(number)) return null;

  const normalizedUnit = unit?.toLowerCase();
  const multipliers = {
    b: 1,
    kb: 1000,
    mb: 1000 * 1000,
    gb: 1000 * 1000 * 1000,
    kib: 1024,
    mib: 1024 * 1024,
    gib: 1024 * 1024 * 1024,
  };

  return Math.round(number * (multipliers[normalizedUnit] || 1));
}

function findLine(lines, metricName) {
  return lines.find((line) => line.trimStart().startsWith(`${metricName}.`));
}

function parseTrendLine(line) {
  if (!line) return null;

  const match = line.match(
    /avg=([\d.,]+)(ns|us|µs|ms|s)\s+min=([\d.,]+)(ns|us|µs|ms|s)\s+med=([\d.,]+)(ns|us|µs|ms|s)\s+max=([\d.,]+)(ns|us|µs|ms|s)\s+p\(90\)=([\d.,]+)(ns|us|µs|ms|s)\s+p\(95\)=([\d.,]+)(ns|us|µs|ms|s)/,
  );

  if (!match) return null;

  return {
    avgMs: parseDurationToMs(match[1], match[2]),
    minMs: parseDurationToMs(match[3], match[4]),
    medMs: parseDurationToMs(match[5], match[6]),
    maxMs: parseDurationToMs(match[7], match[8]),
    p90Ms: parseDurationToMs(match[9], match[10]),
    p95Ms: parseDurationToMs(match[11], match[12]),
  };
}

function parseRateLine(line) {
  if (!line) return null;

  const match = line.match(/:\s+(\d+)\s+([\d.,]+)\/s/);
  if (!match) return null;

  return {
    count: Number(match[1]),
    rate: parseDecimal(match[2]),
  };
}

function parseChecksSucceeded(line) {
  if (!line) return null;

  const match = line.match(/:\s+([\d.,]+)%\s+(\d+)\s+out of\s+(\d+)/);
  if (!match) return null;

  return {
    rate: parseDecimal(match[1]) / 100,
    passes: Number(match[2]),
    total: Number(match[3]),
  };
}

function parseChecksFailed(line) {
  if (!line) return null;

  const match = line.match(/:\s+([\d.,]+)%\s+(\d+)\s+out of\s+(\d+)/);
  if (!match) return null;

  return {
    rate: parseDecimal(match[1]) / 100,
    fails: Number(match[2]),
    total: Number(match[3]),
  };
}

function parseNetworkLine(line) {
  if (!line) return null;

  const match = line.match(/:\s+([\d.,]+)\s+([KMGT]?i?B|B)\s+([\d.,]+)\s+([KMGT]?i?B|B)\/s/i);
  if (!match) return null;

  return {
    bytes: parseBytes(match[1], match[2]),
    bytesPerSecond: parseBytes(match[3], match[4]),
  };
}

function parseVusLine(line) {
  if (!line) return null;

  const match = line.match(/:\s+(\d+)\s+min=(\d+)\s+max=(\d+)/);
  if (!match) return null;

  return {
    current: Number(match[1]),
    min: Number(match[2]),
    max: Number(match[3]),
  };
}

function parseFinalRunningLine(lines) {
  const line = [...lines].reverse().find((item) => item.includes("complete and") && item.includes("iterations"));
  if (!line) return null;

  const match = line.match(/,\s+(\d+)\s+complete and\s+(\d+)\s+interrupted iterations/);
  if (!match) return null;

  return {
    completeIterations: Number(match[1]),
    interruptedIterations: Number(match[2]),
  };
}

export function parseK6Logs(text, fallback = {}) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter(Boolean);

  const hasTotalResults = lines.some((line) => line.includes("TOTAL RESULTS"));
  const hasFinished = lines.some((line) => line.includes("TEST FINISHED"));
  const hasError = lines.some((line) => line.toLowerCase().includes("error"));

  if (!hasTotalResults && !hasFinished) return null;

  const httpTrend = parseTrendLine(findLine(lines, "http_req_duration"));
  const grpcTrend = parseTrendLine(findLine(lines, "grpc_req_duration"));
  const iterationTrend = parseTrendLine(findLine(lines, "iteration_duration"));
  const latencyTrend = httpTrend || grpcTrend || iterationTrend;

  const httpReqs = parseRateLine(findLine(lines, "http_reqs"));
  const grpcReqs = parseRateLine(findLine(lines, "grpc_reqs"));
  const iterations = parseRateLine(findLine(lines, "iterations"));
  const checksTotal = parseRateLine(findLine(lines, "checks_total"));
  const checksSucceeded = parseChecksSucceeded(findLine(lines, "checks_succeeded"));
  const checksFailed = parseChecksFailed(findLine(lines, "checks_failed"));
  const dataReceived = parseNetworkLine(findLine(lines, "data_received"));
  const dataSent = parseNetworkLine(findLine(lines, "data_sent"));
  const vus = parseVusLine(findLine(lines, "vus"));
  const vusMax = parseVusLine(findLine(lines, "vus_max"));
  const finalRunning = parseFinalRunningLine(lines);

  return {
    ...fallback,
    state: hasError && !hasTotalResults ? "failed" : hasFinished ? "completed" : "running",
    source: "live_logs",
    latency: {
      primary: httpTrend ? "http_req_duration" : grpcTrend ? "grpc_req_duration" : "iteration_duration",
      avgMs: latencyTrend?.avgMs ?? null,
      minMs: latencyTrend?.minMs ?? null,
      medMs: latencyTrend?.medMs ?? null,
      maxMs: latencyTrend?.maxMs ?? null,
      p90Ms: latencyTrend?.p90Ms ?? null,
      p95Ms: latencyTrend?.p95Ms ?? null,
    },
    throughput: {
      rps: httpReqs?.rate ?? grpcReqs?.rate ?? iterations?.rate ?? null,
      totalRequests: httpReqs?.count ?? grpcReqs?.count ?? checksTotal?.count ?? null,
      iterationsPerSecond: iterations?.rate ?? null,
      totalIterations: iterations?.count ?? finalRunning?.completeIterations ?? null,
      interruptedIterations: finalRunning?.interruptedIterations ?? null,
    },
    checks: {
      rate: checksSucceeded?.rate ?? null,
      passes: checksSucceeded?.passes ?? null,
      fails: checksFailed?.fails ?? null,
      total: checksSucceeded?.total ?? checksFailed?.total ?? checksTotal?.count ?? null,
    },
    network: {
      dataReceivedBytes: dataReceived?.bytes ?? null,
      dataReceivedRateBytes: dataReceived?.bytesPerSecond ?? null,
      dataSentBytes: dataSent?.bytes ?? null,
      dataSentRateBytes: dataSent?.bytesPerSecond ?? null,
    },
    vus: {
      current: vus?.current ?? null,
      min: vus?.min ?? null,
      max: vusMax?.max ?? vus?.max ?? null,
    },
  };
}