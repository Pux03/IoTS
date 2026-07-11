import { useEffect, useRef, useState } from "react";
import socket from "../socket/socket";
import { parseK6Logs } from "../utils/k6LogParser";

import ControlPanel from "./ControlPanel";
import LiveLogs from "./LiveLogs";
import ResultsPanel from "./ResultsPanel";

export default function Dashboard() {
  const [logs, setLogs] = useState([]);
  const [summary, setSummary] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const runConfigRef = useRef(null);
  const rawTextRef = useRef("");

  const startRun = (config) => {
    const nextConfig = {
      ...config,
      requestedVus: config.vus,
      state: "running",
      source: "live_logs",
      startedAt: new Date().toISOString(),
    };

    runConfigRef.current = nextConfig;
    rawTextRef.current = "";
    setLogs([]);
    setSummary(nextConfig);
    setIsRunning(true);
  };

  const failRunStart = () => {
    runConfigRef.current = null;
    rawTextRef.current = "";
    setSummary(null);
    setIsRunning(false);
  };

  useEffect(() => {
    const onLog = (msg) => {
      rawTextRef.current += String(msg);
      setLogs((previousLogs) => [...previousLogs.slice(-300), msg]);

      const parsed = parseK6Logs(rawTextRef.current, runConfigRef.current || {});

      if (parsed && isRunning) {
        setSummary(parsed);

        // safety net: ako benchmark-result event ne stigne iz bilo kog
        // razloga, ipak oslobodi dugme cim live parsing vidi kraj testa
        if (parsed.state === "completed" || parsed.state === "failed") {
          setIsRunning(false);
        }
      }
    };

    const onResult = (result) => {
      setSummary(result);
      setIsRunning(false);
    };

    socket.on("benchmark-log", onLog);
    socket.on("benchmark-result", onResult);

    return () => {
      socket.off("benchmark-log", onLog);
      socket.off("benchmark-result", onResult);
    };
  }, [isRunning]);

  return (
    <main className="dashboard">
      <ControlPanel onRunStart={startRun} onRunStartFailed={failRunStart} isRunning={isRunning} />
      <ResultsPanel result={summary} />
      <LiveLogs logs={logs} />
    </main>
  );
}