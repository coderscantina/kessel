-- Bookmarks and their folders belong to a space, like tabs: each space is a
-- self-contained workspace with its own pinned sites.
ALTER TABLE sites ADD COLUMN space_id INTEGER NOT NULL DEFAULT 1;
ALTER TABLE bookmark_folders ADD COLUMN space_id INTEGER NOT NULL DEFAULT 1;

-- Adopt existing rows into the first space (id 1 may have been deleted).
UPDATE sites SET space_id = COALESCE((SELECT MIN(id) FROM spaces), 1);
UPDATE bookmark_folders SET space_id = COALESCE((SELECT MIN(id) FROM spaces), 1);
