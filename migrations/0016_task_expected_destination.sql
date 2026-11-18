-- Conditional overwrite: what the destination must still hold when the worker
-- writes (JSON of `queue::ExpectedDest`). A mismatch parks the task as a
-- conflict; deciding that conflict clears the expectation.
ALTER TABLE transfer_tasks ADD COLUMN expected_dest TEXT;
