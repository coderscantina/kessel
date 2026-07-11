//! SQLite is the app's source of truth for everything except secrets:
//! bookmarks, the persistent transfer queue, history, and known hosts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context as _, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::Row as _;

use crate::provider::{LayerConfig, SiteProfile};

pub async fn connect(data_dir: &Path) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(data_dir.join("kessel.db"))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .context("opening kessel.db")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("running migrations")?;

    Ok(pool)
}

/// Non-connection bookmark metadata: sidebar presentation and grouping.
#[derive(Debug, Clone, Default)]
pub struct BookmarkMeta {
    pub icon: Option<String>,
    pub color: Option<String>,
    pub tags: Vec<String>,
    pub folder_id: Option<i64>,
    /// Space a new bookmark is created in (updates never move spaces).
    pub space_id: i64,
}

/// A sidebar folder grouping bookmarks (rows come back in display order).
#[derive(Debug, Clone)]
pub struct BookmarkFolder {
    pub id: i64,
    pub name: String,
    pub icon: Option<String>,
    pub color: Option<String>,
    pub space_id: i64,
}

/// A connection tag: lowercase name plus an optional accent color. Sites
/// reference tags by name (their JSON `tags` column); this row adds color.
#[derive(Debug, Clone)]
pub struct Tag {
    pub id: i64,
    pub name: String,
    pub color: Option<String>,
}

/// An Arc-style space: a named workspace holding its own set of open tabs.
#[derive(Debug, Clone)]
pub struct Space {
    pub id: i64,
    pub name: String,
    pub icon: Option<String>,
    pub color: Option<String>,
}

/// One persisted open tab (up to two panes for split view). A `None` site id
/// is the reserved local filesystem; `bookmark_site_id` marks a bookmark
/// ("pinned") tab that lives on its bookmark row instead of the tab list.
#[derive(Debug, Clone, Default)]
pub struct OpenTabRow {
    pub space_id: i64,
    pub bookmark_site_id: Option<i64>,
    /// User-given tab title (inline rename); `None` derives it live.
    pub name: Option<String>,
    pub layout: String,
    pub a_site_id: Option<i64>,
    pub a_path: String,
    pub b_site_id: Option<i64>,
    pub b_path: String,
}

pub async fn list_sites(pool: &SqlitePool) -> Result<Vec<SiteProfile>> {
    let rows = sqlx::query(
        // id 0 is the reserved "Local" site backing transfers; not a bookmark.
        "SELECT id, name, scheme, options, layers, secret_handle, icon, color, tags, \
                folder_id, space_id, sort_order \
         FROM sites WHERE id != 0 ORDER BY sort_order, name",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let options: HashMap<String, String> =
                serde_json::from_str(row.get::<String, _>("options").as_str())?;
            let layers: LayerConfig =
                serde_json::from_str(row.get::<String, _>("layers").as_str())?;
            let tags: Vec<String> =
                serde_json::from_str(row.get::<String, _>("tags").as_str()).unwrap_or_default();
            Ok(SiteProfile {
                id: row.get("id"),
                name: row.get("name"),
                scheme: row.get("scheme"),
                options,
                layers,
                secret_handle: row.get("secret_handle"),
                icon: row.get("icon"),
                color: row.get("color"),
                tags,
                folder_id: row.get("folder_id"),
                space_id: row.get("space_id"),
            })
        })
        .collect()
}

pub async fn insert_site(
    pool: &SqlitePool,
    name: &str,
    scheme: &str,
    options: &HashMap<String, String>,
    layers: &LayerConfig,
    meta: &BookmarkMeta,
) -> Result<i64> {
    let id = sqlx::query(
        "INSERT INTO sites (name, scheme, options, layers, icon, color, tags, folder_id, space_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(name)
    .bind(scheme)
    .bind(serde_json::to_string(options)?)
    .bind(serde_json::to_string(layers)?)
    .bind(&meta.icon)
    .bind(&meta.color)
    .bind(serde_json::to_string(&meta.tags)?)
    .bind(meta.folder_id)
    .bind(meta.space_id)
    .execute(pool)
    .await?
    .last_insert_rowid();
    Ok(id)
}

/// Layers are deliberately untouched — the bookmark form doesn't edit them.
pub async fn update_site(
    pool: &SqlitePool,
    site_id: i64,
    name: &str,
    scheme: &str,
    options: &HashMap<String, String>,
    meta: &BookmarkMeta,
) -> Result<()> {
    sqlx::query(
        "UPDATE sites SET name = ?, scheme = ?, options = ?, icon = ?, color = ?, tags = ?, \
         folder_id = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(name)
    .bind(scheme)
    .bind(serde_json::to_string(options)?)
    .bind(&meta.icon)
    .bind(&meta.color)
    .bind(serde_json::to_string(&meta.tags)?)
    .bind(meta.folder_id)
    .bind(site_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Move a bookmark into a folder (or out of all folders) without touching
/// the rest of the row.
pub async fn set_site_folder(
    pool: &SqlitePool,
    site_id: i64,
    folder_id: Option<i64>,
) -> Result<()> {
    sqlx::query("UPDATE sites SET folder_id = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(folder_id)
        .bind(site_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Persist a full manual ordering of bookmarks (ids in display order).
pub async fn set_site_order(pool: &SqlitePool, ids: &[i64]) -> Result<()> {
    let mut tx = pool.begin().await?;
    for (ix, id) in ids.iter().enumerate() {
        sqlx::query("UPDATE sites SET sort_order = ? WHERE id = ?")
            .bind(ix as i64)
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

// --- bookmark folders ---

pub async fn list_folders(pool: &SqlitePool) -> Result<Vec<BookmarkFolder>> {
    let rows = sqlx::query(
        "SELECT id, name, icon, color, space_id FROM bookmark_folders ORDER BY sort_order, name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| BookmarkFolder {
            id: row.get("id"),
            name: row.get("name"),
            icon: row.get("icon"),
            color: row.get("color"),
            space_id: row.get("space_id"),
        })
        .collect())
}

pub async fn insert_folder(
    pool: &SqlitePool,
    name: &str,
    icon: Option<&str>,
    color: Option<&str>,
    space_id: i64,
) -> Result<i64> {
    let id = sqlx::query(
        "INSERT INTO bookmark_folders (name, icon, color, space_id) VALUES (?, ?, ?, ?)",
    )
    .bind(name)
    .bind(icon)
    .bind(color)
    .bind(space_id)
    .execute(pool)
    .await?
    .last_insert_rowid();
    Ok(id)
}

/// Deleting a folder keeps its bookmarks (folder_id is set NULL by the FK).
pub async fn delete_folder(pool: &SqlitePool, folder_id: i64) -> Result<()> {
    sqlx::query("DELETE FROM bookmark_folders WHERE id = ?")
        .bind(folder_id)
        .execute(pool)
        .await?;
    Ok(())
}

// --- spaces ---

pub async fn list_spaces(pool: &SqlitePool) -> Result<Vec<Space>> {
    let rows =
        sqlx::query("SELECT id, name, icon, color FROM spaces ORDER BY sort_order, id")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|row| Space {
            id: row.get("id"),
            name: row.get("name"),
            icon: row.get("icon"),
            color: row.get("color"),
        })
        .collect())
}

pub async fn insert_space(
    pool: &SqlitePool,
    name: &str,
    icon: Option<&str>,
    color: Option<&str>,
) -> Result<i64> {
    let id = sqlx::query("INSERT INTO spaces (name, icon, color) VALUES (?, ?, ?)")
        .bind(name)
        .bind(icon)
        .bind(color)
        .execute(pool)
        .await?
        .last_insert_rowid();
    Ok(id)
}

/// The space's tabs are cleaned up by the next session rewrite. Its
/// bookmarks and folders are adopted by the first surviving space rather
/// than deleted — saved connections outlive workspace shuffling.
pub async fn delete_space(pool: &SqlitePool, space_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    let fallback = "(SELECT MIN(id) FROM spaces WHERE id != ?)";
    for table in ["sites", "bookmark_folders"] {
        // `table`/`fallback` are compile-time constants, not user input.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET space_id = {fallback} WHERE space_id = ?"
        )))
        .bind(space_id)
        .bind(space_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM spaces WHERE id = ?")
        .bind(space_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

// --- open tab session ---

pub async fn list_open_tabs(pool: &SqlitePool) -> Result<Vec<OpenTabRow>> {
    let rows = sqlx::query(
        "SELECT space_id, bookmark_site_id, name, layout, a_site_id, a_path, b_site_id, b_path \
         FROM open_tabs ORDER BY sort_order",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| OpenTabRow {
            space_id: row.get("space_id"),
            bookmark_site_id: row.get("bookmark_site_id"),
            name: row.get("name"),
            layout: row.get("layout"),
            a_site_id: row.get("a_site_id"),
            a_path: row.get("a_path"),
            b_site_id: row.get("b_site_id"),
            b_path: row.get("b_path"),
        })
        .collect())
}

/// Replace the whole persisted tab session. Called on every tab mutation —
/// the session is small, so rewrite-all beats tracking row identities.
pub async fn save_open_tabs(pool: &SqlitePool, tabs: &[OpenTabRow]) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM open_tabs").execute(&mut *tx).await?;
    for (ix, tab) in tabs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO open_tabs (sort_order, space_id, bookmark_site_id, name, layout, \
             a_site_id, a_path, b_site_id, b_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(ix as i64)
        .bind(tab.space_id)
        .bind(tab.bookmark_site_id)
        .bind(&tab.name)
        .bind(&tab.layout)
        .bind(tab.a_site_id)
        .bind(&tab.a_path)
        .bind(tab.b_site_id)
        .bind(&tab.b_path)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Inline rename: update only the bookmark's display name.
pub async fn set_site_name(pool: &SqlitePool, site_id: i64, name: &str) -> Result<()> {
    sqlx::query("UPDATE sites SET name = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(name)
        .bind(site_id)
        .execute(pool)
        .await?;
    Ok(())
}

// --- tags ---

pub async fn list_tags(pool: &SqlitePool) -> Result<Vec<Tag>> {
    let rows = sqlx::query("SELECT id, name, color FROM tags ORDER BY name")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| Tag {
            id: row.get("id"),
            name: row.get("name"),
            color: row.get("color"),
        })
        .collect())
}

/// Ensure a row exists for every (lowercase) tag name.
pub async fn upsert_tags(pool: &SqlitePool, names: &[String]) -> Result<()> {
    for name in names {
        sqlx::query("INSERT OR IGNORE INTO tags (name) VALUES (?)")
            .bind(name.to_lowercase())
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn set_tag_color(pool: &SqlitePool, name: &str, color: Option<&str>) -> Result<()> {
    sqlx::query("INSERT INTO tags (name, color) VALUES (?, ?) \
                 ON CONFLICT(name) DO UPDATE SET color = excluded.color")
        .bind(name.to_lowercase())
        .bind(color)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_site(pool: &SqlitePool, site_id: i64) -> Result<()> {
    sqlx::query("DELETE FROM sites WHERE id = ? AND id != 0")
        .bind(site_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_site_secret_handle(
    pool: &SqlitePool,
    site_id: i64,
    handle: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE sites SET secret_handle = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(handle)
        .bind(site_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// On launch, anything left `active` by a crash goes back to `queued` so the
/// queue resumes where it stopped — tasks keep their byte_offset.
pub async fn requeue_interrupted(pool: &SqlitePool) -> Result<u64> {
    let jobs = sqlx::query("UPDATE transfer_jobs SET state = 'queued' WHERE state = 'active'")
        .execute(pool)
        .await?
        .rows_affected();
    sqlx::query("UPDATE transfer_tasks SET state = 'queued' WHERE state = 'active'")
        .execute(pool)
        .await?;
    Ok(jobs)
}
