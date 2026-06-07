import { useState } from "react";
import { startBenchmark } from "../api/benchmark_api";

export default function ControlPanel({ setLogs, setResult }) {
    const [protocol, setProtocol] = useState("rest");
    const [scenario, setScenario] = useState("high-frequency");
    const [vus, setVus] = useState(10);
    const [duration, setDuration] = useState("30s");

    const handleStart = async () => {
        setLogs([]);
        setResult(null);

        await startBenchmark({
            protocol,
            scenario,
            vus,
            duration,
        });
    };

    return (
        <div style={{ padding: 20, border: "1px solid #ccc" }}>
            <h2>Benchmark Control</h2>

            <select onChange={(e) => setProtocol(e.target.value)}>
                <option value="rest">REST</option>
                <option value="grpc">gRPC</option>
                <option value="graphql">GraphQL</option>
            </select>

            <select onChange={(e) => setScenario(e.target.value)}>
                <option value="high-frequency">High Frequency</option>
                <option value="selective">Selective Monitoring</option>
                <option value="heavy">Heavy Querying</option>
            </select>

            <input
                type="number"
                value={vus}
                onChange={(e) => setVus(e.target.value)}
                placeholder="VUs"
            />

            <input
                value={duration}
                onChange={(e) => setDuration(e.target.value)}
                placeholder="Duration"
            />

            <button onClick={handleStart}>START TEST</button>
        </div>
    );
}