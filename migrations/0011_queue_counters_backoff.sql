-- Retry backoff: a task re-queued after a transient error can't be claimed
-- again before `not_before`.
ALTER TABLE transfer_tasks ADD COLUMN not_before TEXT;

-- Per-job counters, kept current by the triggers below, so queue snapshots
-- read one row per job instead of aggregating every task.
ALTER TABLE transfer_jobs ADD COLUMN files_total    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN files_done     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN files_skipped  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN files_conflict INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN files_failed   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN bytes_total    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transfer_jobs ADD COLUMN bytes_done     INTEGER NOT NULL DEFAULT 0;

-- A job can finish with some files failed; history keeps that count.
ALTER TABLE transfer_history ADD COLUMN files_failed INTEGER NOT NULL DEFAULT 0;

UPDATE transfer_jobs SET
    files_total    = (SELECT COUNT(*) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id),
    files_done     = (SELECT COUNT(*) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id AND t.state = 'done'),
    files_skipped  = (SELECT COUNT(*) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id AND t.state = 'skipped'),
    files_conflict = (SELECT COUNT(*) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id AND t.state = 'conflict'),
    files_failed   = (SELECT COUNT(*) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id AND t.state = 'failed'),
    bytes_total    = (SELECT COALESCE(SUM(CASE WHEN t.state = 'skipped' THEN 0 ELSE COALESCE(t.total_bytes, 0) END), 0)
                      FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id),
    bytes_done     = (SELECT COALESCE(SUM(t.byte_offset), 0) FROM transfer_tasks t WHERE t.job_id = transfer_jobs.id);

CREATE TRIGGER transfer_tasks_count_insert AFTER INSERT ON transfer_tasks BEGIN
    UPDATE transfer_jobs SET
        files_total    = files_total + 1,
        files_done     = files_done + (NEW.state = 'done'),
        files_skipped  = files_skipped + (NEW.state = 'skipped'),
        files_conflict = files_conflict + (NEW.state = 'conflict'),
        files_failed   = files_failed + (NEW.state = 'failed'),
        bytes_total    = bytes_total + CASE WHEN NEW.state = 'skipped' THEN 0 ELSE COALESCE(NEW.total_bytes, 0) END,
        bytes_done     = bytes_done + NEW.byte_offset
    WHERE id = NEW.job_id;
END;

CREATE TRIGGER transfer_tasks_count_delete AFTER DELETE ON transfer_tasks BEGIN
    UPDATE transfer_jobs SET
        files_total    = files_total - 1,
        files_done     = files_done - (OLD.state = 'done'),
        files_skipped  = files_skipped - (OLD.state = 'skipped'),
        files_conflict = files_conflict - (OLD.state = 'conflict'),
        files_failed   = files_failed - (OLD.state = 'failed'),
        bytes_total    = bytes_total - CASE WHEN OLD.state = 'skipped' THEN 0 ELSE COALESCE(OLD.total_bytes, 0) END,
        bytes_done     = bytes_done - OLD.byte_offset
    WHERE id = OLD.job_id;
END;

CREATE TRIGGER transfer_tasks_count_update
AFTER UPDATE OF state, total_bytes, byte_offset ON transfer_tasks BEGIN
    UPDATE transfer_jobs SET
        files_done     = files_done + (NEW.state = 'done') - (OLD.state = 'done'),
        files_skipped  = files_skipped + (NEW.state = 'skipped') - (OLD.state = 'skipped'),
        files_conflict = files_conflict + (NEW.state = 'conflict') - (OLD.state = 'conflict'),
        files_failed   = files_failed + (NEW.state = 'failed') - (OLD.state = 'failed'),
        bytes_total    = bytes_total
                         + CASE WHEN NEW.state = 'skipped' THEN 0 ELSE COALESCE(NEW.total_bytes, 0) END
                         - CASE WHEN OLD.state = 'skipped' THEN 0 ELSE COALESCE(OLD.total_bytes, 0) END,
        bytes_done     = bytes_done + NEW.byte_offset - OLD.byte_offset
    WHERE id = NEW.job_id;
END;
