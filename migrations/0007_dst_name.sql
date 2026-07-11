-- "Download As…" / "save under a different name": an optional destination
-- name that overrides the source file name during job expansion.
ALTER TABLE transfer_jobs ADD COLUMN dst_name TEXT;
