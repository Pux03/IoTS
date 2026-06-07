const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { spawn } = require("child_process");

const app = express();
app.use(express.json());

app.use(
  cors({
    origin: "http://localhost:5173",
  }),
);

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "http://localhost:5173",
  },
});

io.on("connection", (socket) => {
  console.log("Frontend connected");
});

app.post("/benchmark/start", (req, res) => {
  const { protocol, scenario, vus, duration } = req.body;

  console.log("Starting k6:", req.body);

  const k6 = spawn("k6", [
    "run",
    `scripts/${protocol}/${scenario}.js`,
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
  });

  res.json({ status: "started" });
});

server.listen(3000, () => {
  console.log("Benchmark service running on port 3000");
});
