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

    let migrator = sqlx::migrate!("./migrations");
    backup_before_migrating(&pool, &migrator, &data_dir.join("kessel.db.bak")).await?;
    migrator.run(&pool).await.context("running migrations")?;

    Ok(pool)
}

/// Copy the database to `backup` when it already has applied migrations and
/// the binary carries newer ones, replacing the previous backup. A fresh
/// database has nothing worth keeping. `VACUUM INTO` gives a consistent copy
/// of a WAL database, which a plain file copy would not.
async fn backup_before_migrating(
    pool: &SqlitePool,
    migrator: &sqlx::migrate::Migrator,
    backup: &Path,
) -> Result<()> {
    // The table doesn't exist before the first run.
    let applied: Vec<i64> = sqlx::query_scalar("SELECT version FROM _sqlx_migrations")
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    let pending = migrator
        .iter()
        .any(|migration| !applied.contains(&migration.version));
    if applied.is_empty() || !pending {
        return Ok(());
    }
    // VACUUM INTO refuses to overwrite.
    std::fs::remove_file(backup).ok();
    sqlx::query("VACUUM INTO ?")
        .bind(backup.to_string_lossy().into_owned())
        .execute(pool)
        .await
        .context("backing up kessel.db before migrating")?;
    tracing::info!(backup = %backup.display(), "database backed up before migrating");
    Ok(())
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

/// Saving through the form also settles an imported site's "needs
/// credentials" flag: the user has been through the form.
pub async fn update_site(
    pool: &SqlitePool,
    site_id: i64,
    name: &str,
    scheme: &str,
    options: &HashMap<String, String>,
    layers: &LayerConfig,
    meta: &BookmarkMeta,
) -> Result<()> {
    sqlx::query(
        "UPDATE sites SET name = ?, scheme = ?, options = ?, layers = ?, icon = ?, color = ?, \
         tags = ?, folder_id = ?, needs_credentials = 0, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(name)
    .bind(scheme)
    .bind(serde_json::to_string(options)?)
    .bind(serde_json::to_string(layers)?)
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

/// Transfer jobs the user can still act on: everything except a clean `done`
/// job. Deleting a site cascades to its jobs, and a job resolves its site by
/// id whenever it runs, so these are the jobs a site change could hurt.
const OPEN_JOB: &str = "NOT (state = 'done' AND files_failed = 0)";

/// Why a site can't be deleted or retargeted right now: `jobs` open transfer
/// jobs still reference it.
#[derive(Debug, thiserror::Error)]
pub enum SiteBusy {
    #[error("{}", crate::i18n::tf("site.busy.delete", &[("jobs", &.0.to_string())]))]
    Delete(i64),
    #[error("{}", crate::i18n::tf("site.busy.edit", &[("jobs", &.0.to_string())]))]
    Edit(i64),
}

pub async fn open_job_count(pool: &SqlitePool, site_id: i64) -> Result<i64> {
    let sql = format!(
        "SELECT COUNT(*) FROM transfer_jobs \
         WHERE (src_site_id = ?1 OR dst_site_id = ?1) AND {OPEN_JOB}"
    );
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(site_id)
        .fetch_one(pool)
        .await?)
}

/// Delete a site unless open transfer jobs reference it (the check and the
/// delete are one statement, so a job can't slip in between).
pub async fn delete_site(pool: &SqlitePool, site_id: i64) -> Result<()> {
    let sql = format!(
        "DELETE FROM sites WHERE id = ?1 AND id != 0 AND NOT EXISTS ( \
           SELECT 1 FROM transfer_jobs \
           WHERE (src_site_id = ?1 OR dst_site_id = ?1) AND {OPEN_JOB})"
    );
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(site_id)
        .execute(pool)
        .await?
        .rows_affected();
    if deleted == 0 {
        let jobs = open_job_count(pool, site_id).await?;
        if jobs > 0 {
            return Err(SiteBusy::Delete(jobs).into());
        }
    }
    Ok(())
}

/// What a job aims at when it resolves the site: protocol and where on the
/// server. Credentials, names and limits don't redirect anything.
fn connection_target<'a>(scheme: &'a str, options: &'a HashMap<String, String>) -> Vec<&'a str> {
    std::iter::once(scheme)
        .chain(
            ["endpoint", "root", "bucket", "container"]
                .map(|key| options.get(key).map_or("", String::as_str)),
        )
        .collect()
}

/// Refuse a change of protocol, endpoint, root, bucket or container while
/// open transfer jobs reference the site: a paused job would otherwise resume
/// against the new location.
pub async fn ensure_site_retargetable(
    pool: &SqlitePool,
    site_id: i64,
    scheme: &str,
    options: &HashMap<String, String>,
) -> Result<()> {
    let row = sqlx::query("SELECT scheme, options FROM sites WHERE id = ?")
        .bind(site_id)
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else { return Ok(()) };
    let old_scheme: String = row.get("scheme");
    let old_options: HashMap<String, String> =
        serde_json::from_str(row.get::<String, _>("options").as_str())?;
    if connection_target(&old_scheme, &old_options) == connection_target(scheme, options) {
        return Ok(());
    }
    let jobs = open_job_count(pool, site_id).await?;
    if jobs > 0 {
        return Err(SiteBusy::Edit(jobs).into());
    }
    Ok(())
}

/// Ids of imported sites still waiting for their first credentials review.
pub async fn sites_needing_credentials(pool: &SqlitePool) -> Result<std::collections::HashSet<i64>> {
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM sites WHERE needs_credentials = 1")
        .fetch_all(pool)
        .await?;
    Ok(ids.into_iter().collect())
}

/// A site parsed from another client's export, ready to be saved.
pub struct ImportSite {
    pub name: String,
    pub scheme: String,
    pub options: HashMap<String, String>,
    /// Folder the source kept it in; created in the space when missing.
    pub folder: Option<String>,
    pub needs_credentials: bool,
}

/// Save imported sites into `space_id` in one transaction, reusing folders
/// of the same name. Nothing is stored if any row fails.
pub async fn import_sites(pool: &SqlitePool, space_id: i64, sites: &[ImportSite]) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut folders: HashMap<String, i64> =
        sqlx::query_as::<_, (String, i64)>("SELECT name, id FROM bookmark_folders WHERE space_id = ?")
            .bind(space_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
    for site in sites {
        let folder_id = match &site.folder {
            Some(name) => match folders.get(name) {
                Some(id) => Some(*id),
                None => {
                    let id = sqlx::query("INSERT INTO bookmark_folders (name, space_id) VALUES (?, ?)")
                        .bind(name)
                        .bind(space_id)
                        .execute(&mut *tx)
                        .await?
                        .last_insert_rowid();
                    folders.insert(name.clone(), id);
                    Some(id)
                }
            },
            None => None,
        };
        sqlx::query(
            "INSERT INTO sites (name, scheme, options, folder_id, space_id, needs_credentials) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&site.name)
        .bind(&site.scheme)
        .bind(serde_json::to_string(&site.options)?)
        .bind(folder_id)
        .bind(space_id)
        .bind(site.needs_credentials)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
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
/// queue resumes where it stopped — tasks keep their byte_offset. A task is
/// only `paused` while its job is paused by the user, so a paused task under
/// a running job was stranded mid-pause and is re-queued too. A task caught
/// running under a cancel that hadn't settled is cancelled instead; the
/// engine finishes that cancel on start.
pub async fn requeue_interrupted(pool: &SqlitePool) -> Result<u64> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE transfer_tasks SET state = CASE WHEN \
           (SELECT state FROM transfer_jobs WHERE id = job_id) = 'cancelled' \
           THEN 'cancelled' ELSE 'queued' END \
         WHERE state = 'active' OR (state = 'paused' AND job_id IN \
           (SELECT id FROM transfer_jobs WHERE state IN ('queued', 'active')))",
    )
    .execute(&mut *tx)
    .await?;
    let jobs = sqlx::query("UPDATE transfer_jobs SET state = 'queued' WHERE state = 'active'")
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backs_up_only_when_migrations_are_pending() {
        let dir = std::env::temp_dir().join(format!("kessel-db-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let backup = dir.join("kessel.db.bak");
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let pool = connect(&dir).await.unwrap();
            assert!(!backup.exists(), "a fresh database needs no backup");

            let migrator = sqlx::migrate!("./migrations");
            backup_before_migrating(&pool, &migrator, &backup).await.unwrap();
            assert!(!backup.exists(), "nothing pending");

            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)")
                .execute(&pool)
                .await
                .unwrap();
            backup_before_migrating(&pool, &migrator, &backup).await.unwrap();
            assert!(backup.exists());
            pool.close().await;
        });
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A scratch database in its own temp dir, with one site and (optionally)
    /// one transfer job from the local site to it in `state`.
    async fn site_with_job(name: &str, state: Option<&str>) -> (SqlitePool, i64, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("kessel-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = connect(&dir).await.unwrap();
        let options = HashMap::from([("endpoint".to_string(), "ssh://old.example.com".to_string())]);
        let site = insert_site(&pool, "x", "sftp", &options, &LayerConfig::default(), &BookmarkMeta::default())
            .await
            .unwrap();
        if let Some(state) = state {
            sqlx::query(
                "INSERT INTO transfer_jobs (src_site_id, dst_site_id, src, dst, state) VALUES (0, ?, 'a', '', ?)",
            )
            .bind(site)
            .bind(state)
            .execute(&pool)
            .await
            .unwrap();
        }
        (pool, site, dir)
    }

    #[test]
    fn delete_is_refused_while_open_jobs_reference_the_site() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (pool, site, dir) = site_with_job("delete-busy", Some("paused")).await;
            let error = delete_site(&pool, site).await.unwrap_err();
            assert!(matches!(error.downcast_ref::<SiteBusy>(), Some(SiteBusy::Delete(1))));
            assert_eq!(list_sites(&pool).await.unwrap().len(), 1);

            sqlx::query("UPDATE transfer_jobs SET state = 'done', files_failed = 0")
                .execute(&pool)
                .await
                .unwrap();
            delete_site(&pool, site).await.unwrap();
            assert!(list_sites(&pool).await.unwrap().is_empty());
            pool.close().await;
            std::fs::remove_dir_all(&dir).unwrap();
        });
    }

    #[test]
    fn retargeting_is_refused_while_open_jobs_reference_the_site() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (pool, site, dir) = site_with_job("retarget-busy", Some("queued")).await;
            let mut options = HashMap::from([("endpoint".to_string(), "ssh://old.example.com".to_string())]);
            options.insert("user".to_string(), "anna".to_string());
            ensure_site_retargetable(&pool, site, "sftp", &options).await.unwrap();

            options.insert("endpoint".to_string(), "ssh://new.example.com".to_string());
            let error = ensure_site_retargetable(&pool, site, "sftp", &options).await.unwrap_err();
            assert!(matches!(error.downcast_ref::<SiteBusy>(), Some(SiteBusy::Edit(1))));

            sqlx::query("DELETE FROM transfer_jobs").execute(&pool).await.unwrap();
            ensure_site_retargetable(&pool, site, "sftp", &options).await.unwrap();
            pool.close().await;
            std::fs::remove_dir_all(&dir).unwrap();
        });
    }
}
