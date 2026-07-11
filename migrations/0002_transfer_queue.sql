-- Transfer queue v2: src/dst are both sites (the local filesystem is the
-- reserved site 0), so uploads, downloads and site-to-site copies are one
-- code path. Tables are recreated — no released build ever wrote to v1.

-- Reserved "Local" site backing the local-filesystem side of transfers.
-- Hidden from the bookmark list and protected from deletion in code.
INSERT OR IGNORE INTO sites (id, name, scheme, options)
VALUES (0, 'Local', 'fs', '{"root":"/"}');

DROP TABLE transfer_tasks;
DROP TABLE transfer_jobs;
DROP TABLE transfer_history;

-- A job is the user-level action ("copy this entry there"). `src` is a file
-- or directory browse path (dirs end '/'); `dst` is the destination
-- directory prefix ('' = site root).
CREATE TABLE transfer_jobs (
    id          INTEGER PRIMARY KEY,
    src_site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    dst_site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'active', 'paused', 'failed', 'done', 'cancelled')),
    priority    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    started_at  TEXT,
    finished_at TEXT
);
CREATE INDEX idx_transfer_jobs_state ON transfer_jobs(state);

-- Sub-tasks: a directory job fans out into one row per file. byte_offset is
-- the resume point; src_size/src_etag detect a source that changed since the
-- offset was recorded (a mismatch restarts the file from zero).
CREATE TABLE transfer_tasks (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'active', 'paused', 'failed', 'done', 'cancelled')),
    total_bytes INTEGER,
    byte_offset INTEGER NOT NULL DEFAULT 0,
    src_etag    TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_transfer_tasks_job ON transfer_tasks(job_id, state);

-- Audit trail. Site names are denormalized so history survives site deletion;
-- no foreign keys on purpose.
CREATE TABLE transfer_history (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    src_site    TEXT    NOT NULL,
    dst_site    TEXT    NOT NULL,
    src         TEXT    NOT NULL,
    dst         TEXT    NOT NULL,
    outcome     TEXT    NOT NULL,
    files       INTEGER NOT NULL DEFAULT 0,
    bytes       INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER,
    error       TEXT,
    finished_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
