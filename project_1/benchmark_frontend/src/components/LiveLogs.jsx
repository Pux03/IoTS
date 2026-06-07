const cardStyle = {
    background: "#0b1220",
    border: "1px solid #1f2a44",
    borderRadius: 12,
    padding: 16,
    boxShadow: "0 10px 30px rgba(0,0,0,0.3)",
    color: "#e6edf3",
    fontFamily: "Inter, sans-serif",
};

const titleStyle = {
    fontSize: 14,
    opacity: 0.7,
    marginBottom: 10,
};

const gridStyle = {
    display: "grid",
    gridTemplateColumns: "1fr 1fr 1fr",
    gap: 16,
};

export default function LiveLogs({ logs }) {
    const formatLog = (log) => {
        return log
            .replace(/\s+/g, " ")
            .replace(/running/g, "⚡ running")
            .replace(/TEST FINISHED/g, "🏁 TEST FINISHED");
    };

    return (
        <div
            style={{
                ...cardStyle,
                height: 360,
                overflowY: "auto",
                fontFamily: "monospace",
                fontSize: 12,
            }}
        >
            <div style={titleStyle}>Live Execution Logs</div>

            {logs.map((log, i) => (
                <div
                    key={i}
                    style={{
                        padding: "4px 0",
                        borderBottom: "1px solid rgba(255,255,255,0.05)",
                        color: log.includes("ERROR") ? "#ff6b6b" : "#0f0",
                    }}
                >
                    {formatLog(log)}
                </div>
            ))}
        </div>
    );
}