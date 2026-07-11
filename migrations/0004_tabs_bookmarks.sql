-- Sidebar bookmarks get visual identity (icon, color, tags), optional
-- grouping into folders, and a manual sort order.
CREATE TABLE bookmark_folders (
    id         INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    icon       TEXT,                          -- named icon from the app's palette
    color      TEXT,                          -- named color from the app's palette
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

ALTER TABLE sites ADD COLUMN icon       TEXT;
ALTER TABLE sites ADD COLUMN color      TEXT;
ALTER TABLE sites ADD COLUMN tags       TEXT NOT NULL DEFAULT '[]'; -- JSON array of strings
ALTER TABLE sites ADD COLUMN folder_id  INTEGER REFERENCES bookmark_folders(id) ON DELETE SET NULL;
ALTER TABLE sites ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0;

-- Open tab session, restored on relaunch. Each tab holds up to two panes
-- (split view); a NULL site id means the reserved local filesystem. No FK to
-- sites: a deleted bookmark just makes the tab fall back to local.
CREATE TABLE open_tabs (
    id         INTEGER PRIMARY KEY,
    sort_order INTEGER NOT NULL DEFAULT 0,
    layout     TEXT    NOT NULL DEFAULT 'single'
               CHECK (layout IN ('single', 'split_h', 'split_v')),
    a_site_id  INTEGER,
    a_path     TEXT    NOT NULL DEFAULT '',
    b_site_id  INTEGER,
    b_path     TEXT    NOT NULL DEFAULT '',
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
