import axios from "axios";

const API = import.meta.env.VITE_BENCHMARK_API_URL || "http://localhost:3000";

export const startBenchmark = (data) => {
  return axios.post(`${API}/benchmark/start`, data);
};
