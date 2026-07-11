//! Embedded Model Context Protocol server.
//!
//! Exposes a read-only view of kessel — bookmarked sites, directory listings,
//! object metadata, and the live transfer queue / history — to MCP clients
//! over a local streamable-HTTP endpoint (`http://<bind>:<port>/mcp`). When
//! `expose_write` is set, a second set of mutating tools (enqueue / pause /
//! resume / cancel transfers) is registered too.
//!
//! The server runs on the app-owned tokio runtime, sharing the same SQLite
//! pool and [`QueueHandle`] as the GUI, so everything it reports is the real
//! app state. It reuses [`crate::provider::Provider`] to talk to backends,
//! which means every OpenDAL service and per-site layer stack works here for
//! free.
//!
//! SECURITY: there is no authentication in v1. The transport is meant to sit
//! on loopback; a non-loopback bind is allowed but logged prominently. Write
//! tools stay off unless `expose_write` is explicitly enabled.

use std::net::{IpAddr, SocketAddr};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler};
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;

use crate::config::McpConfig;
use crate::provider::{LayerConfig, Provider};
use crate::queue::{store, ConflictPolicy, NewJob, QueueHandle};

/// Cap on entries returned by a single `list_dir` call.
const MAX_ENTRIES: usize = 1000;
/// Cap on rows returned by `transfer_history`.
const MAX_HISTORY: i64 = 200;

/// Start the MCP server on the app runtime. No-op when disabled; otherwise the
/// bound address is logged and the server serves until process exit.
pub fn start(
    runtime: &tokio::runtime::Runtime,
    cfg: &McpConfig,
    db: SqlitePool,
    queue: QueueHandle,
    layers: LayerConfig,
) {
    if !cfg.enabled {
        return;
    }

    let ip: IpAddr = match cfg.bind.parse() {
        Ok(ip) => ip,
        Err(err) => {
            tracing::error!(bind = %cfg.bind, %err, "invalid mcp.bind address; MCP server not started");
            return;
        }
    };
    let addr = SocketAddr::new(ip, cfg.port);
    if !ip.is_loopback() {
        tracing::warn!(
            %addr,
            "MCP server bound to a NON-LOOPBACK address with NO authentication — anyone who can reach this port can browse your sites"
        );
    }

    let server = KesselMcp::new(db, queue, layers, cfg.expose_write);
    let expose_write = cfg.expose_write;

    runtime.spawn(async move {
        let service = StreamableHttpService::new(
            move || Ok(server.clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );
        let router = axum::Router::new().nest_service("/mcp", service);
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(%addr, %err, "MCP server failed to bind; not started");
                return;
            }
        };
        tracing::warn!(
            url = %format!("http://{addr}/mcp"),
            expose_write,
            "MCP server enabled (no authentication — v1)"
        );
        if let Err(err) = axum::serve(listener, router).await {
            tracing::error!(%err, "MCP server stopped");
        }
    });
}

/// The MCP tool surface over kessel state. Cloned per client session by the
/// streamable-HTTP transport; all fields are cheap handles.
#[derive(Clone)]
struct KesselMcp {
    db: SqlitePool,
    queue: QueueHandle,
    /// Default OpenDAL layer stack, merged over each site's own config when
    /// connecting a provider for `list_dir` / `stat`.
    layers: LayerConfig,
    tool_router: ToolRouter<KesselMcp>,
}

// --- tool argument schemas ---

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SitePath {
    /// Bookmarked site id (from `list_sites`). Site 0 is the local filesystem.
    site_id: i64,
    /// Browse path within the site; "" is the site root, directories end "/".
    #[serde(default)]
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct HistoryArgs {
    /// Maximum rows to return (newest first); capped at 200.
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EnqueueArgs {
    /// Source site id (0 = local filesystem).
    src_site_id: i64,
    /// Source file or directory browse path (directories end with "/").
    src: String,
    /// Destination site id (0 = local filesystem).
    dst_site_id: i64,
    /// Destination directory prefix ("" = destination root).
    #[serde(default)]
    dst_dir: String,
    /// Optional name to write under at the destination; keeps the source name
    /// when omitted.
    #[serde(default)]
    dst_name: Option<String>,
    /// Overwrite an existing destination file. When false, conflicting files
    /// are parked for a decision (`ask`).
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct JobId {
    /// Transfer job id (from `queue_status`).
    job_id: i64,
}

fn internal(err: impl std::fmt::Display) -> McpError {
    McpError::internal_error(err.to_string(), None)
}

fn ok_json(value: serde_json::Value) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(&value).map_err(internal)?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

// --- read-only tools (always registered) ---

#[tool_router(router = read_router)]
impl KesselMcp {
    /// Connect a provider for a site off the async worker: `Provider::connect`
    /// is blocking (keychain access), so it runs on the blocking pool.
    async fn provider_for(&self, site_id: i64) -> Result<Provider, McpError> {
        let profile = store::get_site(&self.db, site_id)
            .await
            .map_err(internal)?;
        let layers = self.layers.clone();
        tokio::task::spawn_blocking(move || Provider::connect(&profile, &layers))
            .await
            .map_err(internal)?
            .map_err(internal)
    }

    #[tool(description = "List kessel's bookmarked sites (id, name, scheme, root). \
        No secrets or endpoints are exposed. Site 0 is the local filesystem.")]
    async fn list_sites(&self) -> Result<CallToolResult, McpError> {
        let sites = crate::db::list_sites(&self.db).await.map_err(internal)?;
        let items: Vec<_> = sites
            .into_iter()
            .map(|site| {
                json!({
                    "id": site.id,
                    "name": site.name,
                    "scheme": site.scheme,
                    // `root` is a non-secret path prefix; endpoints/creds are omitted.
                    "root": site.options.get("root").cloned().unwrap_or_default(),
                })
            })
            .collect();
        ok_json(json!({ "sites": items }))
    }

    #[tool(description = "List a directory on a site. Args: site_id, path (\"\" = root, \
        directories end with \"/\"). Returns up to 1000 entries with name, is_dir, size, mtime.")]
    async fn list_dir(
        &self,
        Parameters(args): Parameters<SitePath>,
    ) -> Result<CallToolResult, McpError> {
        let provider = self.provider_for(args.site_id).await?;
        let mut entries = provider.list(&args.path).await.map_err(internal)?;
        let truncated = entries.len() > MAX_ENTRIES;
        entries.truncate(MAX_ENTRIES);
        let items: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                json!({
                    "name": entry.name,
                    "path": entry.path,
                    "is_dir": entry.is_dir,
                    "size": entry.size,
                    "mtime": entry.modified.map(|dt| dt.to_rfc3339()),
                })
            })
            .collect();
        ok_json(json!({
            "site_id": args.site_id,
            "path": args.path,
            "truncated": truncated,
            "entries": items,
        }))
    }

    #[tool(description = "Stat one path on a site. Args: site_id, path. Returns \
        is_dir, size, mtime, etag and content_type where the backend reports them.")]
    async fn stat(
        &self,
        Parameters(args): Parameters<SitePath>,
    ) -> Result<CallToolResult, McpError> {
        let provider = self.provider_for(args.site_id).await?;
        let (op, rel) = provider.op_for(&args.path).map_err(internal)?;
        let meta = op.stat(&rel).await.map_err(internal)?;
        ok_json(json!({
            "site_id": args.site_id,
            "path": args.path,
            "is_dir": meta.is_dir(),
            "size": meta.content_length(),
            "mtime": meta.last_modified().and_then(crate::provider::to_datetime).map(|dt| dt.to_rfc3339()),
            "etag": meta.etag(),
            "content_type": meta.content_type(),
        }))
    }

    #[tool(description = "Summarize the transfer queue: counts by state plus per-job \
        progress (id, direction, src, dst, state, files, bytes, bytes/sec, error).")]
    async fn queue_status(&self) -> Result<CallToolResult, McpError> {
        let snapshot = self.queue.latest();
        let mut active = 0;
        let mut queued = 0;
        let mut failed = 0;
        let mut done = 0;
        let jobs: Vec<_> = snapshot
            .jobs
            .iter()
            .map(|job| {
                use crate::queue::JobState::*;
                match job.state {
                    Active => active += 1,
                    Queued => queued += 1,
                    Failed => failed += 1,
                    Done => done += 1,
                    _ => {}
                }
                json!({
                    "id": job.id,
                    "direction": job.direction(),
                    "src_site": job.src_site,
                    "dst_site": job.dst_site,
                    "src": job.src,
                    "dst": job.dst,
                    "state": job.state.as_str(),
                    "files_total": job.files_total,
                    "files_done": job.files_done,
                    "bytes_total": job.bytes_total,
                    "bytes_done": job.bytes_done,
                    "bps": job.bps,
                    "error": job.error,
                })
            })
            .collect();
        ok_json(json!({
            "active": active,
            "queued": queued,
            "failed": failed,
            "done": done,
            "total_bps": snapshot.total_bps,
            "jobs": jobs,
        }))
    }

    #[tool(description = "Recent completed transfers from the audit trail, newest first. \
        Args: limit (optional, default 50, capped at 200).")]
    async fn transfer_history(
        &self,
        Parameters(args): Parameters<HistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(50).clamp(1, MAX_HISTORY);
        let history = store::history(&self.db, limit).await.map_err(internal)?;
        let items: Vec<_> = history
            .into_iter()
            .map(|entry| {
                json!({
                    "src_site": entry.src_site,
                    "dst_site": entry.dst_site,
                    "src": entry.src,
                    "dst": entry.dst,
                    "outcome": entry.outcome,
                    "files": entry.files,
                    "bytes": entry.bytes,
                    "duration_ms": entry.duration_ms,
                    "error": entry.error,
                    "finished_at": entry.finished_at,
                })
            })
            .collect();
        ok_json(json!({ "history": items }))
    }
}

// --- write tools (registered only when expose_write) ---

#[tool_router(router = write_router)]
impl KesselMcp {
    #[tool(description = "Enqueue a transfer. Args: src_site_id, src, dst_site_id, dst_dir, \
        optional dst_name, overwrite. Directory sources fan out into per-file tasks. \
        Returns the queued job's id.")]
    async fn enqueue_transfer(
        &self,
        Parameters(args): Parameters<EnqueueArgs>,
    ) -> Result<CallToolResult, McpError> {
        let job = NewJob {
            src_site_id: args.src_site_id,
            dst_site_id: args.dst_site_id,
            src: args.src,
            dst_dir: args.dst_dir,
            dst_name: args.dst_name,
            conflict_policy: if args.overwrite {
                ConflictPolicy::Overwrite
            } else {
                ConflictPolicy::Ask
            },
            move_source: false,
        };
        // Same path as the UI: the engine persists the job and starts
        // scheduling it. Fire-and-forget, so the new id isn't returned here —
        // call `queue_status` to see it land.
        self.queue.enqueue(job);
        ok_json(json!({ "state": "queued" }))
    }

    #[tool(description = "Pause a transfer job by id.")]
    async fn pause_job(
        &self,
        Parameters(args): Parameters<JobId>,
    ) -> Result<CallToolResult, McpError> {
        self.queue.pause(args.job_id);
        ok_json(json!({ "job_id": args.job_id, "requested": "pause" }))
    }

    #[tool(description = "Resume a paused transfer job by id.")]
    async fn resume_job(
        &self,
        Parameters(args): Parameters<JobId>,
    ) -> Result<CallToolResult, McpError> {
        self.queue.resume(args.job_id);
        ok_json(json!({ "job_id": args.job_id, "requested": "resume" }))
    }

    #[tool(description = "Cancel a transfer job by id.")]
    async fn cancel_job(
        &self,
        Parameters(args): Parameters<JobId>,
    ) -> Result<CallToolResult, McpError> {
        self.queue.cancel(args.job_id);
        ok_json(json!({ "job_id": args.job_id, "requested": "cancel" }))
    }
}

impl KesselMcp {
    fn new(db: SqlitePool, queue: QueueHandle, layers: LayerConfig, expose_write: bool) -> Self {
        // Read-only tools are always present; mutating tools are merged in only
        // when the operator opted into `expose_write`.
        let mut tool_router = Self::read_router();
        if expose_write {
            tool_router = tool_router + Self::write_router();
        }
        Self {
            db,
            queue,
            layers,
            tool_router,
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for KesselMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(
                "kessel file manager. Read-only tools: list_sites, list_dir, stat, \
                 queue_status, transfer_history. When write access is enabled: \
                 enqueue_transfer, pause_job, resume_job, cancel_job. Site 0 is the \
                 local filesystem; other site ids come from list_sites."
                    .to_string(),
            )
    }
}
