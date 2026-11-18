-- A move sets this once its destination is complete, before the source goes
-- away. Only then does a missing source mean the move already finished.
ALTER TABLE transfer_tasks ADD COLUMN dst_committed INTEGER NOT NULL DEFAULT 0;
