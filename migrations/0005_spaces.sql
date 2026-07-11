-- Arc-style spaces: named workspaces, each with its own set of open tabs.
CREATE TABLE spaces (
    id         INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    icon       TEXT,                          -- named icon from the app's palette
    color      TEXT,                          -- named color from the app's palette
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO spaces (id, name, icon) VALUES (1, 'Default', 'globe');

ALTER TABLE open_tabs ADD COLUMN space_id INTEGER NOT NULL DEFAULT 1;
-- Set when the tab is a bookmark ("pinned") tab living on its bookmark row
-- in the sidebar instead of the ephemeral tab list.
ALTER TABLE open_tabs ADD COLUMN bookmark_site_id INTEGER;
