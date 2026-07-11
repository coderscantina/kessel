-- Bookmarks / connection profiles. No secrets here — secret_handle references
-- an entry in the OS keychain (see src/secrets.rs).
CREATE TABLE sites (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL,
    scheme        TEXT    NOT NULL,             -- OpenDAL service scheme: "s3", "sftp", "webdav", ...
    options       TEXT    NOT NULL DEFAULT '{}', -- JSON map of non-secret Operator options (endpoint, region, root, ...)
    layers        TEXT    NOT NULL DEFAULT '{}', -- JSON LayerConfig: retry / timeout / throttle / concurrency
    secret_handle TEXT,                          -- keychain account handle, NULL if backend needs no secret
    ui_prefs      TEXT    NOT NULL DEFAULT '{}',
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Transfer queue, persisted so it survives crash/quit and resumes mid-transfer.
-- A job is the user-level action (e.g. "upload this directory").
CREATE TABLE transfer_jobs (
    id          INTEGER PRIMARY KEY,
    site_id     INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    direction   TEXT    NOT NULL CHECK (direction IN ('upload', 'download', 'remote_copy')),
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'active', 'paused', 'failed', 'done', 'cancelled')),
    error       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Sub-tasks: a recursive directory job fans out into one row per file.
-- byte_offset is the resume point for partial transfers.
CREATE TABLE transfer_tasks (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'active', 'paused', 'failed', 'done', 'cancelled')),
    total_bytes INTEGER,
    byte_offset INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_transfer_tasks_job ON transfer_tasks(job_id, state);

-- Completed-job audit trail (throughput, errors) for history and re-queue.
CREATE TABLE transfer_history (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    site_id     INTEGER NOT NULL,
    direction   TEXT    NOT NULL,
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    outcome     TEXT    NOT NULL,
    bytes       INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER,
    error       TEXT,
    finished_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- SFTP known-hosts and per-site trust decisions.
CREATE TABLE known_hosts (
    id         INTEGER PRIMARY KEY,
    host       TEXT NOT NULL,
    port       INTEGER NOT NULL DEFAULT 22,
    key_type   TEXT NOT NULL,
    public_key TEXT NOT NULL,
    trusted    INTEGER NOT NULL DEFAULT 1,
    added_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (host, port, key_type)
);
