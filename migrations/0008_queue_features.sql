-- Move semantics: delete the source after a successful transfer.
ALTER TABLE transfer_jobs ADD COLUMN move_source INTEGER NOT NULL DEFAULT 0;

-- Streaming expansion: tasks are now inserted in batches while the source
-- tree is being listed, so "tasks exist" no longer implies "expansion
-- finished". A job whose expansion was interrupted re-expands from scratch.
ALTER TABLE transfer_jobs ADD COLUMN expanded INTEGER NOT NULL DEFAULT 0;

-- Jobs created before this migration used all-or-nothing expansion, so any
-- existing tasks imply a completed fan-out.
UPDATE transfer_jobs SET expanded = 1
  WHERE EXISTS (SELECT 1 FROM transfer_tasks WHERE job_id = transfer_jobs.id);
