import axios from "axios";

const API = "http://localhost:3000";

export const startBenchmark = (data) => {
  return axios.post(`${API}/benchmark/start`, data);
};
