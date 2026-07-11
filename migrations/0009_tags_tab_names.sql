-- Connection tags become first-class rows: normalized to lowercase, with an
-- optional accent color. Sites keep referencing tags by name in their JSON
-- `tags` column (the existing model); this table adds identity + color.
CREATE TABLE tags (
    id         INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL UNIQUE,       -- always lowercase
    color      TEXT,                          -- named color from the app's palette
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Normalize existing site tags to lowercase …
UPDATE sites
SET tags = (SELECT json_group_array(lower(value)) FROM json_each(sites.tags))
WHERE tags != '[]';

-- … and seed the tags table from them.
INSERT OR IGNORE INTO tags (name)
SELECT DISTINCT lower(value) FROM sites, json_each(sites.tags);

-- Optional user-given tab title (inline rename); NULL derives the title
-- from the connection.
ALTER TABLE open_tabs ADD COLUMN name TEXT;
