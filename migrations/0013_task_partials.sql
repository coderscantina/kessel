-- Partial ownership: the browse path of the `.kessel-part` file a task
-- opened, so cancel, removal and finalization can delete it whatever the
-- resume offset says (non-append backends checkpoint 0).
ALTER TABLE transfer_tasks ADD COLUMN partial TEXT;

-- Write mode: the task appends onto the existing destination in place
-- (conflict policy `resume`), so pause, retry and relaunch keep doing that
-- instead of starting a staged copy from zero.
ALTER TABLE transfer_tasks ADD COLUMN in_place INTEGER NOT NULL DEFAULT 0;

-- "Keep both" reserves a unique name by checking which destinations other
-- unfinished tasks already target.
CREATE INDEX idx_transfer_tasks_dst ON transfer_tasks(dst);
