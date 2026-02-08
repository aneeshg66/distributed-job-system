# manager/main.py
from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import subprocess
import sys
from pathlib import Path
import os
from dotenv import load_dotenv
import json
import time
import logging

# load .env as early as possible so os.getenv() returns values from .env
ROOT = Path(__file__).resolve().parents[1]   # project root (two levels above manager/main.py)
dotenv_path = ROOT / ".env"
load_dotenv(dotenv_path, override=True)

# set up a small logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manager")

ENV = os.getenv("ENV", "development").lower()
REQUIRE_API_KEYS = os.getenv("REQUIRE_API_KEYS", "1" if ENV == "production" else "0").lower() in ("1", "true", "yes")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")
ALLOW_SPAWN_WORKERS = os.getenv("ALLOW_SPAWN_WORKERS", "0" if ENV == "production" else "1").lower() in ("1", "true", "yes")
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://127.0.0.1:5173,http://localhost:5173").split(",") if o.strip()]
DEBUG_LOG_PATH = os.getenv("DEBUG_LOG_PATH", "")

NEXT_JOB_POLL_S = float(os.getenv("NEXT_JOB_POLL_S", "0.2"))
NEXT_JOB_MAX_WAIT_MS = int(os.getenv("NEXT_JOB_MAX_WAIT_MS", "10000"))
WORKER_STALE_S = int(os.getenv("WORKER_STALE_S", "10"))
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
SYSTEM_PAUSED_KEY = "system_paused"

_db_pool = None

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
                "location":"manager/main.py",
                "message":msg,
                "data": data or {},
                "timestamp": time.time()*1000,
                "hypothesisId": hyp
            }) + "\n")
    except Exception:
        pass

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS else [],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    init_db_pool()
    ensure_system_settings()
    if REQUIRE_API_KEYS and (not ADMIN_API_KEY or not WORKER_API_KEY):
        raise RuntimeError("API keys required but not configured")


@app.on_event("shutdown")
def on_shutdown():
    close_db_pool()

def _db_params():
    return dict(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "job_system"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        connect_timeout=5,
    )


def init_db_pool():
    global _db_pool
    if _db_pool is not None:
        return
    try:
        _db_pool = ThreadedConnectionPool(DB_POOL_MIN, DB_POOL_MAX, **_db_params())
        logger.info("DB pool initialized")
    except Exception as e:
        _db_pool = None
        logger.error("DB pool init failed: %s", e)


def close_db_pool():
    global _db_pool
    if _db_pool is not None:
        _db_pool.closeall()
        _db_pool = None


def get_db(retries: int = 3, delay: float = 1.0):
    """Return a psycopg2 connection (with retries)."""
    last_exc = None
    for attempt in range(retries):
        try:
            if _db_pool is not None:
                return _db_pool.getconn()
            return psycopg2.connect(**_db_params())
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc


def close_db(conn):
    if conn is None:
        return
    if _db_pool is not None:
        _db_pool.putconn(conn)
    else:
        close_db(conn)


def ensure_system_settings():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO system_settings (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO NOTHING
            """,
            (SYSTEM_PAUSED_KEY, "false"),
        )
        conn.commit()
    finally:
        cur.close()
        close_db(conn)


def fetch_system_paused(cur):
    cur.execute("SELECT value FROM system_settings WHERE key = %s", (SYSTEM_PAUSED_KEY,))
    row = cur.fetchone()
    return bool(row and row[0] == "true")


def _check_key(expected, provided):
    if not REQUIRE_API_KEYS:
        return
    if not expected:
        raise HTTPException(status_code=500, detail="Server misconfigured")
    if provided != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin_key(x_api_key: str = Header(None)):
    _check_key(ADMIN_API_KEY, x_api_key)


def require_worker_key(x_api_key: str = Header(None)):
    _check_key(WORKER_API_KEY, x_api_key)


# ---------------- JOB APIs ----------------

@app.post("/jobs")
def create_job(payload: dict, _: None = Depends(require_admin_key)):
    """Create a new job"""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (status, total_steps)
            VALUES ('pending', %s)
            RETURNING id
            """,
            (int(payload["total_steps"]),)
        )
        job_id = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO checkpoints (job_id, step) VALUES (%s, 0)",
            (job_id,)
        )

        conn.commit()
    finally:
        cur.close()
        close_db(conn)

    return {"job_id": job_id}


@app.get("/jobs/progress")
def get_job_progress(_: None = Depends(require_admin_key)):
    """Dashboard API"""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT j.id, j.status, j.worker_id, j.total_steps, c.step
            FROM jobs j
            JOIN checkpoints c ON j.id = c.job_id
            ORDER BY j.id
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        close_db(conn)

    return [
        {
            "id": r[0],
            "status": r[1],
            "worker_id": r[2],
            "total_steps": r[3],
            "step": r[4],
        }
        for r in rows
    ]


@app.post("/next-job")
def assign_next_job(payload: dict, _: None = Depends(require_worker_key)):
    """
    Atomically assign the next available job to a worker.
    Uses FOR UPDATE SKIP LOCKED and verifies UPDATE succeeded to avoid races.
    """
    worker_id = payload.get("worker_id")
    try:
        wait_ms = int(payload.get("wait_ms") or 0)
    except Exception:
        wait_ms = 0
    if wait_ms < 0:
        wait_ms = 0
    if wait_ms > NEXT_JOB_MAX_WAIT_MS:
        wait_ms = NEXT_JOB_MAX_WAIT_MS
    deadline = time.monotonic() + (wait_ms / 1000.0) if wait_ms > 0 else None
    _dbg("next-job entry", {"worker_id": worker_id}, "H2")

    conn = get_db()
    try:
        cur = conn.cursor()
        while True:
            if fetch_system_paused(cur):
                conn.commit()
                return {}

            # Start transaction (psycopg2 default is a transaction until commit/rollback)
            # Lock one pending job row (skip locked prevents blocking other workers)
            cur.execute(
                """
                SELECT j.id, j.total_steps, c.step
                FROM jobs j
                JOIN checkpoints c ON j.id = c.job_id
                WHERE j.status = 'pending'
                ORDER BY j.id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()

            # debug snapshot of jobs
            try:
                cur.execute("SELECT id, status FROM jobs ORDER BY id")
                all_jobs = cur.fetchall()
            except Exception:
                all_jobs = []

            _dbg("next-job after SELECT", {
                "row": list(row) if row else None,
                "all_jobs": [{"id": r[0], "status": r[1]} for r in all_jobs]
            }, "H2")

            if row:
                job_id, total_steps, current_step = row

                # Update only if still pending (safety) and check rowcount
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'running', worker_id = %s
                    WHERE id = %s AND status = 'pending'
                    """,
                    (worker_id, job_id)
                )

                if cur.rowcount != 1:
                    # someone else claimed it; rollback/return empty
                    conn.rollback()
                    _dbg("next-job: UPDATE failed rowcount != 1", {"job_id": job_id}, "H4")
                    return {}

                conn.commit()
                _dbg("next-job returning job", {"job_id": job_id, "total_steps": total_steps, "start_step": current_step}, "H2")
                return {
                    "job_id": job_id,
                    "total_steps": total_steps,
                    "start_step": current_step,
                }

            conn.commit()  # nothing taken; end txn before waiting
            if not deadline or time.monotonic() >= deadline:
                return {}
            time.sleep(NEXT_JOB_POLL_S)

    except Exception as e:
        conn.rollback()
        _dbg("next-job exception", {"error": str(e)}, "H1")
        return {}
    finally:
        cur.close()
        close_db(conn)


# ---------------- WORKER CONTROL ----------------

@app.post("/workers/start")
def start_worker(_: None = Depends(require_admin_key)):
    """
    Start a real worker process.
    Worker self-registers in DB.
    """
    if not ALLOW_SPAWN_WORKERS:
        raise HTTPException(status_code=403, detail="Worker spawning disabled")

    cwd = Path(__file__).resolve().parents[1]  # project root
    _dbg("start_worker: about to spawn", {"cwd": str(cwd), "worker_script": "worker/worker.py"}, "H1")

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO workers DEFAULT VALUES RETURNING id")
        worker_id = cur.fetchone()[0]
        conn.commit()
    finally:
        cur.close()
        close_db(conn)

    worker_script = cwd / "worker" / "worker.py"
    _dbg("start_worker: spawning", {"worker_script_abs": str(worker_script), "exists": worker_script.exists()}, "H1")

    try:
        err_log = cwd / ".cursor" / f"worker_{worker_id}_stderr.log"
        err_log.parent.mkdir(parents=True, exist_ok=True)
        err_f = open(err_log, "w")
        process = subprocess.Popen(
            [str(sys.executable), str(worker_script), str(worker_id)],
            cwd=str(cwd),
            stderr=err_f,
            stdout=err_f,
        )
        _dbg("start_worker: spawned", {"worker_id": worker_id, "pid": process.pid, "err_log": str(err_log)}, "H1")
    except Exception as e:
        _dbg("start_worker: spawn FAILED", {"worker_id": worker_id, "error": str(e)}, "H1")
        raise

    return {"message": "worker created", "worker_id": worker_id}


@app.post("/workers/log")
def worker_log(payload: dict, _: None = Depends(require_worker_key)):
    """Receive logs from workers and persist to DB"""
    worker_id = payload.get("worker_id")
    conn = get_db()
    cur = conn.cursor()
    try:
        try:
            worker_id = int(worker_id)
        except Exception:
            conn.commit()
            return {"ok": False}

        # Ensure the worker exists to avoid FK errors if workers table was cleared.
        cur.execute(
            "INSERT INTO workers (id, status) VALUES (%s, 'running') ON CONFLICT (id) DO NOTHING",
            (worker_id,)
        )
        cur.execute(
            """
            INSERT INTO worker_logs (worker_id, level, message)
            SELECT %s, %s, %s
            WHERE EXISTS (SELECT 1 FROM workers WHERE id = %s)
            """,
            (worker_id, payload.get("level"), payload.get("message"), worker_id)
        )
        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        _dbg("worker_log exception", {"error": str(e), "worker_id": worker_id}, "H6")
        return {"ok": False}
    finally:
        cur.close()
        close_db(conn)


@app.post("/workers/heartbeat")
def worker_heartbeat(payload: dict, _: None = Depends(require_worker_key)):
    """Update worker heartbeat (and upsert worker row)."""
    worker_id = payload.get("worker_id")
    if worker_id is None:
        return {"ok": False}

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO workers (id, status, last_heartbeat)
            VALUES (%s, 'running', NOW())
            ON CONFLICT (id)
            DO UPDATE SET status = 'running', last_heartbeat = NOW()
            """,
            (worker_id,)
        )
        conn.commit()
    finally:
        cur.close()
        close_db(conn)
    return {"ok": True}


@app.get("/workers/status")
def get_worker_status(_: None = Depends(require_admin_key)):
    """Return worker status list and active/idle/stale counts."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT w.id, w.status, w.last_heartbeat, j.id
            FROM workers w
            LEFT JOIN jobs j
                ON j.worker_id = w.id AND j.status = 'running'
            ORDER BY w.id
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        close_db(conn)

    now = time.time()
    workers = []
    active = idle = stale = 0
    for worker_id, status, last_heartbeat, job_id in rows:
        last_ts = last_heartbeat.timestamp() if last_heartbeat else None
        is_stale = (last_ts is None) or ((now - last_ts) > WORKER_STALE_S)
        if is_stale:
            state = "stale"
            stale += 1
        elif job_id is not None:
            state = "active"
            active += 1
        else:
            state = "idle"
            idle += 1
        workers.append(
            {
                "id": worker_id,
                "state": state,
                "status": status,
                "last_heartbeat": last_heartbeat.isoformat() if last_heartbeat else None,
                "current_job_id": job_id,
            }
        )

    return {
        "counts": {"active": active, "idle": idle, "stale": stale},
        "workers": workers,
    }


@app.get("/workers/logs")
def get_worker_logs(limit: int = 50, _: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, worker_id, level, message, created_at FROM worker_logs ORDER BY id DESC LIMIT %s",
            (limit,)
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        close_db(conn)

    return [
        {"id": r[0], "worker_id": r[1], "level": r[2], "message": r[3], "created_at": r[4].isoformat()}
        for r in rows
    ]


# ---------------- ADMIN / STATS APIs ----------------

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/health/db")
def health_db():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    finally:
        cur.close()
        close_db(conn)
    return {"ok": True}


@app.get("/system/state")
def system_state(_: None = Depends(require_worker_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        system_paused = fetch_system_paused(cur)
    finally:
        cur.close()
        close_db(conn)
    return {"system_paused": system_paused}


@app.get("/stats")
def get_stats(_: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending'")
        pending_jobs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'running'")
        running_jobs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'completed'")
        completed_jobs = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(total_steps),0) FROM jobs")
        total_steps = cur.fetchone()[0] or 0

        cur.execute("SELECT COALESCE(SUM(c.step),0) FROM checkpoints c")
        completed_steps = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM workers")
        total_workers = cur.fetchone()[0]

        system_paused = fetch_system_paused(cur)
    finally:
        cur.close()
        close_db(conn)

    overall_pct = 0
    if total_steps > 0:
        overall_pct = int((completed_steps / total_steps) * 100)

    return {
        "total_jobs": total_jobs,
        "pending_jobs": pending_jobs,
        "running_jobs": running_jobs,
        "completed_jobs": completed_jobs,
        "total_workers": total_workers,
        "total_steps": total_steps,
        "completed_steps": completed_steps,
        "overall_pct": overall_pct,
        "system_paused": system_paused,
    }


@app.post("/jobs/clear")
def clear_jobs(_: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE checkpoints, jobs RESTART IDENTITY CASCADE")
        conn.commit()
    finally:
        cur.close()
        close_db(conn)
    return {"ok": True}


@app.post("/workers/clear")
def clear_workers(_: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status = 'pending' WHERE status = 'running'")
        cur.execute("UPDATE jobs SET worker_id = NULL WHERE worker_id IS NOT NULL")
        cur.execute("DELETE FROM workers")
        try:
            cur.execute("ALTER SEQUENCE workers_id_seq RESTART WITH 1")
        except Exception:
            pass
        conn.commit()
    finally:
        cur.close()
        close_db(conn)
    return {"ok": True}


@app.post("/system/stop")
def stop_system(_: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE system_settings SET value = %s, updated_at = NOW() WHERE key = %s",
            ("true", SYSTEM_PAUSED_KEY),
        )
        conn.commit()
    finally:
        cur.close()
        close_db(conn)
    return {"paused": True}


@app.post("/system/continue")
def continue_system(_: None = Depends(require_admin_key)):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE system_settings SET value = %s, updated_at = NOW() WHERE key = %s",
            ("false", SYSTEM_PAUSED_KEY),
        )
        conn.commit()
    finally:
        cur.close()
        close_db(conn)
    return {"paused": False}
