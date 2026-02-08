CREATE TABLE workers (
    id SERIAL PRIMARY KEY,
    status TEXT CHECK (status IN ('running','stopped')) DEFAULT 'running',
    created_at TIMESTAMP DEFAULT NOW(),
    last_heartbeat TIMESTAMP DEFAULT NOW()
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    status TEXT CHECK (status IN ('pending','running','paused','completed')) NOT NULL,
    total_steps INTEGER NOT NULL,
    worker_id INTEGER REFERENCES workers(id),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE checkpoints (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    step INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE worker_logs (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES workers(id) ON DELETE CASCADE,
    level TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO system_settings (key, value)
VALUES ('system_paused', 'false')
ON CONFLICT (key) DO NOTHING;
