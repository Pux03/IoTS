import { useEffect } from "react";

function normalizeLog(log) {
  return String(log)
    .split("\n")
    .map((line) => line.trimEnd())
    .filter(Boolean);
}

export default function LiveLogs({ logs }) {
  const lines = logs.flatMap(normalizeLog);


  return (
    <section className="panel logs-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">stream</p>
          <h2>Live logs</h2>
        </div>
        <span className="badge">{lines.length} lines</span>
      </div>

      <div className="log-window">
        {lines.length === 0 ? (
          <p className="empty-state">Logs will appear here after the benchmark starts.</p>
        ) : (
          lines.map((line, index) => (
            <div className={line.toLowerCase().includes("error") ? "log-line error" : "log-line"} key={`${index}-${line}`}>
              {line}
            </div>
          ))
        )}
      </div>
    </section>
  );
}
