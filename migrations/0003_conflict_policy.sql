-- Conflict handling: what to do when a destination file already exists.
-- The job-level policy is applied per file at transfer time; 'ask' parks the
-- task in a 'conflict' state until the user resolves it from the UI.
ALTER TABLE transfer_jobs ADD COLUMN conflict_policy TEXT NOT NULL DEFAULT 'ask';

-- transfer_tasks gains two states ('skipped', 'conflict') and the observed
-- destination metadata for the conflict prompt. SQLite can't extend a CHECK
-- constraint in place, so the table is rebuilt.
CREATE TABLE transfer_tasks_new (
    id           INTEGER PRIMARY KEY,
    job_id       INTEGER NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
    src          TEXT    NOT NULL,
    dst          TEXT    NOT NULL,
    state        TEXT    NOT NULL DEFAULT 'queued'
                 CHECK (state IN ('queued', 'active', 'paused', 'failed', 'done',
                                  'cancelled', 'skipped', 'conflict')),
    total_bytes  INTEGER,
    byte_offset  INTEGER NOT NULL DEFAULT 0,
    src_etag     TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    -- What the destination held when the conflict was detected.
    dst_size     INTEGER,
    dst_modified TEXT,
    updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO transfer_tasks_new
    (id, job_id, src, dst, state, total_bytes, byte_offset, src_etag, attempts, error, updated_at)
SELECT id, job_id, src, dst, state, total_bytes, byte_offset, src_etag, attempts, error, updated_at
FROM transfer_tasks;
DROP TABLE transfer_tasks;
ALTER TABLE transfer_tasks_new RENAME TO transfer_tasks;
CREATE INDEX idx_transfer_tasks_job ON transfer_tasks(job_id, state);
