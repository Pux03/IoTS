import { useState } from "react";
import { startBenchmark } from "../api/benchmark_api";

const protocolOptions = [
  { value: "rest", label: "REST" },
  { value: "grpc", label: "gRPC" },
  { value: "graphql", label: "GraphQL" },
];

const scenarioOptions = [
  { value: "high-frequency", label: "High-frequency ingestion" },
  { value: "selective", label: "Selective monitoring" },
  { value: "heavy", label: "Heavy querying" },
];

export default function ControlPanel({ onRunStart, onRunStartFailed, isRunning }) {
  const [protocol, setProtocol] = useState("rest");
  const [scenario, setScenario] = useState("high-frequency");
  const [vus, setVus] = useState(10);
  const [duration, setDuration] = useState("30s");
  const [error, setError] = useState("");

  const handleStart = async () => {
    setError("");
    onRunStart({
      protocol,
      scenario,
      vus,
      duration,
    });

    try {
      await startBenchmark({
        protocol,
        scenario,
        vus: Number(vus),
        duration,
      });
    } catch (err) {
      setError(err.response?.data?.error || err.message || "Benchmark could not be started.");
      onRunStartFailed();
    }
  };

  return (
    <section className="panel control-panel">
      <div>
        <p className="eyebrow">k6 benchmark runner</p>
        <h1>IoT protocol benchmark</h1>
      </div>

      <div className="control-grid">
        <label>
          Protocol
          <select value={protocol} onChange={(e) => setProtocol(e.target.value)} disabled={isRunning}>
            {protocolOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label>
          Scenario
          <select value={scenario} onChange={(e) => setScenario(e.target.value)} disabled={isRunning}>
            {scenarioOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label>
          Virtual users
          <input
            type="number"
            min="1"
            value={vus}
            onChange={(e) => setVus(e.target.value)}
            disabled={isRunning}
          />
        </label>

        <label>
          Duration
          <input value={duration} onChange={(e) => setDuration(e.target.value)} disabled={isRunning} />
        </label>
      </div>

      <div className="actions-row">
        <button type="button" onClick={handleStart} disabled={isRunning}>
          {isRunning ? "Running..." : "Start test"}
        </button>
        {error && <span className="error-text">{error}</span>}
      </div>
    </section>
  );
}
