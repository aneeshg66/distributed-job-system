import { useEffect, useRef, useState } from "react";
import axios from "axios";
import "./App.css";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
const API_KEY = import.meta.env.VITE_API_KEY || "";
const api = axios.create({
  baseURL: API_BASE,
  headers: API_KEY ? { "X-API-Key": API_KEY } : {},
});

export default function App() {
  const [jobs, setJobs] = useState([]);
  const [steps, setSteps] = useState("");
  const [stats, setStats] = useState(null);
  const [message, setMessage] = useState(null);
  const [workers, setWorkers] = useState([]);
  const [workerCounts, setWorkerCounts] = useState({ active: 0, idle: 0, stale: 0 });
  const [workerCountInput, setWorkerCountInput] = useState("1");
  const [throughput, setThroughput] = useState(0);
  const [confirmUntil, setConfirmUntil] = useState({ clearJobs: 0, clearWorkers: 0 });
  const autoStartedRef = useRef(false);
  const prevJobsRef = useRef({});
  const completionsRef = useRef([]);

  const showMessage = (text, type = "info") => {
    setMessage({ text, type });
    setTimeout(() => setMessage(null), 4000);
  };

  const loadJobs = async () => {
    try {
      const res = await api.get("/jobs/progress");
      setJobs(res.data);
      const now = Date.now();
      const prev = prevJobsRef.current;
      const next = {};
      res.data.forEach((j) => {
        next[j.id] = j.status;
        if (prev[j.id] && prev[j.id] !== "completed" && j.status === "completed") {
          completionsRef.current.push(now);
        }
      });
      prevJobsRef.current = next;
      completionsRef.current = completionsRef.current.filter((t) => now - t <= 60000);
      setThroughput(completionsRef.current.length);
    } catch (err) {
      console.error("Failed to load jobs:", err);
      setJobs([]);
    }
  };

  const loadStats = async () => {
    try {
      const res = await api.get("/stats");
      setStats(res.data);
      if (res.data?.total_workers === 0 && !autoStartedRef.current) {
        autoStartedRef.current = true;
        try {
          await startWorkers(1, true);
        } catch (err) {
          console.error("Failed to auto-start worker:", err);
          showMessage("Failed to auto-start worker. Is the backend running?", "error");
        }
      }
    } catch (err) {
      console.error("Failed to load stats:", err);
      setStats(null);
    }
  };

  const loadWorkers = async () => {
    try {
      const res = await api.get("/workers/status");
      setWorkers(res.data.workers || []);
      setWorkerCounts(res.data.counts || { active: 0, idle: 0, stale: 0 });
    } catch (err) {
      console.error("Failed to load worker status:", err);
      setWorkers([]);
      setWorkerCounts({ active: 0, idle: 0, stale: 0 });
    }
  };

  const createJob = async () => {
    if (!steps) return;
    try {
      await api.post("/jobs", {
        total_steps: Number(steps),
      });
      setSteps("");
      loadJobs();
      showMessage("Job created");
    } catch (err) {
      console.error("Failed to create job:", err);
      showMessage("Failed to create job. Is the backend running?", "error");
    }
  };

  const startWorkers = async (count, silent = false) => {
    try {
      const parsed = parseInt(count, 10);
      const n = Math.max(1, Number.isFinite(parsed) ? parsed : 1);
      const requests = Array.from({ length: n }, () => api.post("/workers/start"));
      await Promise.all(requests);
      if (!silent) {
        showMessage(`Started ${n} worker${n === 1 ? "" : "s"}`);
      }
      await loadStats();
      await loadWorkers();
    } catch (err) {
      console.error("Failed to start worker:", err);
      if (!silent) {
        showMessage("Failed to start worker. Is the backend running?", "error");
      }
    }
  };

  const needsConfirm = (key, label) => {
    const now = Date.now();
    if (confirmUntil[key] && confirmUntil[key] > now) {
      return false;
    }
    setConfirmUntil((prev) => ({ ...prev, [key]: now + 5000 }));
    showMessage(`Click ${label} again to confirm`, "info");
    return true;
  };

  const clearJobs = async () => {
    if (needsConfirm("clearJobs", "Clear Jobs")) return;
    setConfirmUntil((prev) => ({ ...prev, clearJobs: 0 }));
    await api.post("/jobs/clear");
    await loadJobs();
    await loadStats();
    completionsRef.current = [];
    setThroughput(0);
    showMessage("Jobs cleared");
  };

  const clearWorkers = async () => {
    if (needsConfirm("clearWorkers", "Clear Workers")) return;
    setConfirmUntil((prev) => ({ ...prev, clearWorkers: 0 }));
    await api.post("/workers/clear");
    await loadStats();
    await loadWorkers();
    await loadJobs();
    showMessage("Workers cleared");
  };

  const stopSystem = async () => {
    await api.post("/system/stop");
    await loadStats();
    showMessage("System paused");
  };

  const continueSystem = async () => {
    await api.post("/system/continue");
    await loadStats();
    showMessage("System resumed");
  };

  useEffect(() => {
    loadJobs();
    loadStats();
    loadWorkers();
    const timer = setInterval(() => {
      loadJobs();
      loadStats();
      loadWorkers();
    }, 2000);
    return () => clearInterval(timer);
  }, []);

  const formatHeartbeat = (iso) => {
    if (!iso) return "-";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "-";
    const ageSec = Math.max(0, Math.floor((Date.now() - d.getTime()) / 1000));
    return `${d.toLocaleTimeString()} (${ageSec}s ago)`;
  };

  return (
    <div className="container">
      <div className="dashboard">
        <h2>Distributed Job Dashboard</h2>

        <div className={`system-banner ${stats?.system_paused ? "paused" : "running"}`}>
          {stats?.system_paused ? "System Paused" : "System Running"}
        </div>

        {message && (
          <div className={`status-line status-${message.type}`}>
            {message.text}
          </div>
        )}

        <div className="controls">
          <div className="job-controls">
            <input
              type="number"
              placeholder="Total steps"
              value={steps}
              onChange={(e) => setSteps(e.target.value)}
            />
            <button onClick={createJob}>Create Job</button>
            <button onClick={clearJobs}>Clear Jobs</button>
          </div>

          <div className="worker-controls">
            <input
              type="number"
              min="1"
              placeholder="Workers"
              value={workerCountInput}
              onChange={(e) => setWorkerCountInput(e.target.value)}
            />
            <button onClick={() => startWorkers(workerCountInput)}>Start N Workers</button>
            <button onClick={() => startWorkers(1)}>Start Worker</button>
            <button onClick={clearWorkers}>Clear Workers</button>
            <button onClick={stopSystem} disabled={stats?.system_paused}>
              Stop System
            </button>
            <button onClick={continueSystem} disabled={!stats?.system_paused}>
              Continue System
            </button>
          </div>
        </div>

        <div className="summary">
          <div className="counts">
            <strong>Total Jobs:</strong> {stats ? stats.total_jobs : "-"} &nbsp; 
            <strong>Workers:</strong> {stats ? stats.total_workers : "-"} &nbsp; 
            <strong>Queue:</strong> {stats ? stats.pending_jobs : "-"} &nbsp; 
            <strong>Throughput:</strong> {throughput} jobs/min
            {stats?.system_paused && (
              <span className="paused-badge">PAUSED</span>
            )}
          </div>

          <div className="overall-progress">
            <div className="bar">
              <div
                className="bar-fill"
                style={{ width: `${stats ? stats.overall_pct : 0}%` }}
              />
            </div>
            <span className="pct">{stats ? stats.overall_pct : 0}%</span>
          </div>
        </div>

        <div className="workers-summary">
          <div><strong>Active:</strong> {workerCounts.active}</div>
          <div><strong>Idle:</strong> {workerCounts.idle}</div>
          <div><strong>Stale:</strong> {workerCounts.stale}</div>
        </div>

        <table className="workers-table">
          <thead>
            <tr>
              <th>Worker ID</th>
              <th>State</th>
              <th>Current Job</th>
              <th>Last Heartbeat</th>
            </tr>
          </thead>
          <tbody>
            {workers.length === 0 && (
              <tr>
                <td colSpan={4} className="center">No workers</td>
              </tr>
            )}
            {workers.map((w) => (
              <tr key={w.id}>
                <td className="center">{w.id}</td>
                <td className={`center state-${w.state}`}>{w.state}</td>
                <td className="center">{w.current_job_id ?? "-"}</td>
                <td className="center">{formatHeartbeat(w.last_heartbeat)}</td>
              </tr>
            ))}
          </tbody>
        </table>

        <table>
          <thead>
            <tr>
              <th>Job ID</th>
              <th>Status</th>
              <th>Worker</th>
              <th>Total Steps</th>
              <th>Progress</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => {
              const pct =
                j.total_steps > 0
                  ? Math.floor((j.step / j.total_steps) * 100)
                  : 0;

              return (
                <tr key={j.id}>
                  <td className="center">{j.id}</td>
                  <td className={`center ${j.status}`}>{j.status}</td>
                  <td className="center">{j.worker_id ?? "-"}</td>
                  <td className="center">{j.total_steps}</td>
                  <td>
                    <div className="progress-cell">
                      <div className="bar">
                        <div
                          className="bar-fill"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="pct">{pct}%</span>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
