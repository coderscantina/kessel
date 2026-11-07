-- Tasks saved before 0013 never recorded their partial file. Those that ran
-- and didn't finish may own one at the deterministic path
-- `<parent>/.<name>.<task id>.kessel-part` (see `transfer::partial_path`), so
-- record it: cancel, removal and finalization then delete it like any other.
-- Nothing here decides how a task resumes; the transfer stats the partial
-- and restarts from zero when it is missing. `rtrim(dst, <dst without '/'>)`
-- strips the file name and keeps the parent with its trailing slash.
UPDATE transfer_tasks
SET partial = rtrim(dst, replace(dst, '/', ''))
    || '.' || substr(dst, length(rtrim(dst, replace(dst, '/', ''))) + 1)
    || '.' || id || '.kessel-part'
WHERE partial IS NULL
  AND in_place = 0
  AND attempts > 0
  AND state IN ('queued', 'active', 'paused', 'failed', 'cancelled');
