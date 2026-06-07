export default function ResultsPanel({ result }) {
    if (!result) return null;

    return (
        <div style={{ padding: 20, border: "1px solid green" }}>
            <h3>Final Results</h3>
            <p>Avg latency: {result.avg} ms</p>
            <p>P95 latency: {result.p95} ms</p>
            <p>RPS: {result.rps}</p>
        </div>
    );
}