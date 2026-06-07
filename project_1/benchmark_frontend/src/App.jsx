import { useEffect, useState } from "react";
import socket from "./socket/socket";

import ControlPanel from "./components/ControlPanel"
import LiveLogs from "./components/LiveLogs";
import ResultsPanel from "./components/ResultsPanel";

export default function Dashboard() {
  const [logs, setLogs] = useState([]);
  const [result, setResult] = useState(null);

  useEffect(() => {
    socket.on("benchmark-log", (msg) => {
      setLogs((prev) => [...prev, msg]);
    });

    socket.on("benchmark-result", (data) => {
      setResult(data);
    });

    return () => {
      socket.off("benchmark-log");
      socket.off("benchmark-result");
    };
  }, []);

  return (
    <div style={{ display: "grid", gap: 20 }}>
      <ControlPanel setLogs={setLogs} setResult={setResult} />
      <LiveLogs logs={logs} />
      <ResultsPanel result={result} />
    </div>
  );
}