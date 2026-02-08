# worker/worker.py
import time
import requests
import psycopg2
import os
import json
from pathlib import Path
from dotenv import load_dotenv
import sys
import traceback

ROOT = Path(__file__).resolve().parents[1]   # project root
load_dotenv(ROOT / ".env", override=True)

DEBUG_LOG_PATH = os.getenv("DEBUG_LOG_PATH", "")

# small local debug logger into optional file
def _dbg(msg, data=None, hyp=None):
    try:
        if not DEBUG_LOG_PATH:
            return
        p = Path(DEBUG_LOG_PATH)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[1] / p
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a") as f:
            f.write(json.dumps({
                "location":"worker/worker.py",
                "message":msg,
                "data": data or {},
                "timestamp": time.time()*1000,
                "hypothesisId": hyp
            }) + "\n")
    except Exception:
        pass

_dbg("worker script starting", {"argv": list(sys.argv)}, "H1")

API = os.getenv("MANAGER_API", "http://127.0.0.1:8000")
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")
REQUEST_HEADERS = {"X-API-Key": WORKER_API_KEY} if WORKER_API_KEY else {}
WAIT_MS = int(os.getenv("WORKER_WAIT_MS", "5000"))
IDLE_BACKOFF_S = float(os.getenv("WORKER_IDLE_BACKOFF_S", "0.2"))
PAUSE_POLL_S = float(os.getenv("WORKER_PAUSE_POLL_S", "0.5"))
HEARTBEAT_INTERVAL_S = float(os.getenv("WORKER_HEARTBEAT_S", "2"))
_last_heartbeat = 0.0

def get_db(retries: int = 3, delay: float = 1.0):
    last_exc = None
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "127.0.0.1"),
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME", "job_system"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", ""),
                connect_timeout=5
            )
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc

# Determine worker_id: if provided as arg, use it; otherwise register
worker_id = None
if len(sys.argv) > 1:
    try:
        worker_id = int(sys.argv[1])
    except Exception:
        worker_id = None

if worker_id is None:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO workers DEFAULT VALUES RETURNING id")
        worker_id = cur.fetchone()[0]
        conn.commit()
    finally:
        cur.close()
        conn.close()

print(f"Worker {worker_id} started (talking to {API})")
_dbg("worker process started, entering loop", {"worker_id": worker_id, "API": API}, "H1")

def send_log(level, message):
    try:
        requests.post(
            f"{API}/workers/log",
            json={"worker_id": worker_id, "level": level, "message": message},
            headers=REQUEST_HEADERS,
            timeout=5,
        )
    except Exception:
        pass

def send_heartbeat(force=False):
    global _last_heartbeat
    now = time.monotonic()
    if not force and (now - _last_heartbeat) < HEARTBEAT_INTERVAL_S:
        return
    _last_heartbeat = now
    try:
        requests.post(
            f"{API}/workers/heartbeat",
            json={"worker_id": worker_id},
            headers=REQUEST_HEADERS,
            timeout=5,
        )
    except Exception:
        pass

def is_system_paused():
    try:
        res = requests.get(f"{API}/system/state", headers=REQUEST_HEADERS, timeout=5)
        if res.ok:
            return bool(res.json().get("system_paused"))
    except Exception:
        pass
    return False

def wait_if_paused():
    if not is_system_paused():
        return
    _dbg("system paused, waiting", {"worker_id": worker_id}, "H3")
    send_log("info", "system_paused")
    while True:
        send_heartbeat()
        time.sleep(PAUSE_POLL_S)
        if not is_system_paused():
            _dbg("system resumed", {"worker_id": worker_id}, "H3")
            send_log("info", "system_resumed")
            return

send_log("info", "worker_started")
send_heartbeat(force=True)

while True:
    send_heartbeat()
    wait_if_paused()
    _dbg("worker requesting job", {"worker_id": worker_id}, "H1")
    send_log("debug", "requesting_job")
    try:
        payload = {"worker_id": worker_id}
        if WAIT_MS > 0:
            payload["wait_ms"] = WAIT_MS
        response = requests.post(
            f"{API}/next-job",
            json=payload,
            headers=REQUEST_HEADERS,
            timeout=max(5, (WAIT_MS / 1000.0) + 5)
        )
        if not response.ok:
            _dbg("worker got HTTP error from /next-job", {"status": response.status_code, "body": response.text[:400]}, "H2")
            time.sleep(1)
            continue
        response = response.json()
    except Exception as e:
        _dbg("worker request failed", {"worker_id": worker_id, "error": str(e), "trace": traceback.format_exc()}, "H1")
        send_log("error", f"request_failed:{str(e)}")
        time.sleep(2)
        continue

    if not response.get("job_id"):
        _dbg("worker got no job, sleeping", {"worker_id": worker_id, "response": response}, "H3")
        if WAIT_MS <= 0:
            time.sleep(IDLE_BACKOFF_S)
        continue

    job_id = response["job_id"]
    total_steps = int(response["total_steps"])
    step = int(response.get("start_step", 0))

    _dbg("worker got job, starting execution", {"worker_id": worker_id, "job_id": job_id}, "H1")
    send_log("info", f"assigned_job:{job_id}")

    try:
        for s in range(step + 1, total_steps + 1):
            send_heartbeat()
            wait_if_paused()
            # simulate work
            time.sleep(1)

            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE checkpoints
                    SET step = %s, updated_at = NOW()
                    WHERE job_id = %s
                    """,
                    (s, job_id)
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()

            send_log("debug", f"job:{job_id}:step:{s}")

        # mark job completed
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE jobs SET status = 'completed' WHERE id = %s",
                (job_id,)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        _dbg("worker completed job, looping for next", {"worker_id": worker_id, "job_id": job_id}, "H1")
        send_log("info", f"completed_job:{job_id}")

    except Exception as e:
        _dbg("worker job failed", {"worker_id": worker_id, "job_id": job_id, "error": str(e), "trace": traceback.format_exc()}, "H5")
        send_log("error", f"job:{job_id}:failed:{str(e)}")
        # try to reset job back to pending so others can pick it up
        try:
            _conn = get_db()
            try:
                _cur = _conn.cursor()
                _cur.execute(
                    "UPDATE jobs SET status = 'pending', worker_id = NULL WHERE id = %s",
                    (job_id,)
                )
                _conn.commit()
            finally:
                _cur.close()
                _conn.close()
        except Exception:
            pass
        time.sleep(2)
