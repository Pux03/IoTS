function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(digits);
}

function formatSeconds(msValue, digits = 3) {
  if (msValue === null || msValue === undefined || Number.isNaN(Number(msValue))) return "-";
  return (Number(msValue) / 1000).toFixed(digits);
}

function formatBytes(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  const units = ["B", "KB", "MB", "GB"];
  let size = Number(value);
  let unit = 0;

  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }

  return `${size.toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
}

function titleCase(value) {
  if (!value) return "-";
  return value
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export default function ResultsPanel({ result }) {
  const state = result?.state || "idle";

  const cards = [
    {
      label: "Avg latency",
      value: `${formatSeconds(result?.latency?.avgMs)} s`,
      hint: result?.latency?.primary || "waiting",
    },
    {
      label: "p95 latency",
      value: `${formatSeconds(result?.latency?.p95Ms)} s`,
      hint: "tail latency",
    },
    {
      label: "RPS",
      value: formatNumber(result?.throughput?.rps),
      hint: `${result?.throughput?.totalRequests ?? 0} requests`,
    },
    {
      label: "Checks",
      value:
        result?.checks?.rate === null || result?.checks?.rate === undefined
          ? "-"
          : `${formatNumber(result.checks.rate * 100, 1)}%`,
      hint: `${result?.checks?.passes ?? 0} pass / ${result?.checks?.fails ?? 0} fail`,
    },
  ];

  return (
    <section className="panel results-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">results</p>
          <h2>Benchmark summary</h2>
        </div>
        <span className={`status-pill ${state}`}>{state}</span>
      </div>

      {!result ? (
        <p className="empty-state">Choose a protocol and scenario, then start a test.</p>
      ) : (
        <>
          <div className="run-meta">
            <span>{(result.protocol || "-").toUpperCase()}</span>
            <span>{titleCase(result.scenario)}</span>
            <span>{result.requestedVus ?? result.vus?.max ?? "-"} VUs</span>
            <span>{result.duration || "-"}</span>
          </div>

          <div className="metric-grid">
            {cards.map((card) => (
              <article className="metric-card" key={card.label}>
                <span>{card.label}</span>
                <strong>{card.value}</strong>
                <small>{card.hint}</small>
              </article>
            ))}
          </div>

          <div className="details-grid">
            <div>
              <span>Data received</span>
              <strong>{formatBytes(result.network?.dataReceivedBytes)}</strong>
            </div>
            <div>
              <span>Data sent</span>
              <strong>{formatBytes(result.network?.dataSentBytes)}</strong>
            </div>
            <div>
              <span>Iterations</span>
              <strong>{result.throughput?.totalIterations ?? "-"}</strong>
            </div>
            <div>
              <span>Max VUs</span>
              <strong>{result.vus?.max ?? result.vus ?? "-"}</strong>
            </div>
          </div>


          {result.error && <p className="error-text">{result.error}</p>}
        </>
      )}
    </section>
  );
}