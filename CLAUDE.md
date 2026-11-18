# `kessel` — product brief (v2)

**One-liner:** A fast, native desktop file manager for remote filesystems, built in Rust on top of Apache OpenDAL.

**Name rationale:** The Kessel Run is the smuggling route between distant systems; `kessel` hauls your files across them. Fits the _Coder's Cantina_ world without shouting it.

**What it is:** A Cyberduck/FileZilla-class client that unifies object storage and classic transfer protocols behind one interface. Because the storage layer is OpenDAL, `kessel` isn't limited to S3/FTP/SFTP — it inherits OpenDAL's entire service catalog (GCS, Azure Blob, OSS, COS, B2, WebDAV, HDFS, local fs, and dozens more) essentially for free. Add a backend, and it just shows up as another site.

## Architecture

**Provider layer = OpenDAL `Operator`.** Every backend is an `Operator`, so browsing, statting, listing, reading, and writing are identical across all protocols. `kessel`'s own provider trait sits just above OpenDAL to carry per-backend capability flags (server-side rename? recursive delete? presigned URLs?) and path-model quirks (S3 has no real directories).

**Layer stack — the reason to build on OpenDAL.** Every `Operator` is wrapped in a composable middleware stack, configured per-site:

- `RetryLayer` — exponential backoff on transient failures, so dropped FTP/S3 connections self-heal.
- `TimeoutLayer` — bounded operations; no hung transfers on a dead socket.
- `ThrottleLayer` — bandwidth / traffic control, per-site and global (your "don't saturate my uplink" knob).
- `ConcurrentLimitLayer` — caps in-flight operations per backend, complementing the queue's own parallelism.
- `LoggingLayer` + `TracingLayer` — structured, per-operation logs feeding an in-app diagnostics/console panel.

This is the core value proposition: retry, timeout, throttling and logging are _library-provided and battle-tested_, not something `kessel` reimplements. The app composes them; it doesn't own that risk.

## Local state — SQLite

A single embedded SQLite database is the app's source of truth for everything except secrets:

- **Bookmarks / connection profiles** — backend type, endpoint, region, root path, per-site layer config (throttle, retry, timeout), and UI preferences. No secrets stored here.
- **Transfer queue — persisted.** Every job and sub-task (recursive dir → per-file tasks) lives in SQLite with its state (queued / active / paused / failed / done) and byte offset. This is what lets the queue survive a crash or quit and **resume mid-transfer** on relaunch — the "walk away and trust it" promise made durable.
- **Transfer history & logs** — completed jobs, throughput, errors, for auditing and re-queue.
- **SFTP known-hosts** and per-site trust decisions.

**Secrets never touch SQLite** — passwords, S3 keys, and SSH passphrases go to the OS keychain (`keyring`), referenced from the DB by handle only.

## Configuration — JSON5

App-level and advanced config is **JSON5** (via the `config` crate with live reload), chosen deliberately over plain JSON/TOML: comments, trailing commas, and unquoted keys make hand-edited config pleasant, which matters for the power users who'll tune layer stacks and default throttles by hand. SQLite holds runtime state; JSON5 holds human-authored defaults and overrides. Follow the principle of defaults in json5 with the app and one in app_data_dir

## Tech stack

| Concern       | Choice                                             | Notes                                                                                                                                                       |
| ------------- | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Language      | **Rust** (stable, latest)                          | GPUI tracks latest stable                                                                                                                                   |
| GUI           | **GPUI** + **gpui-component**                      | Both from crates.io (`gpui-component = "0.6"` on `gpui-pre = "0.3"`, imported as `gpui`; native backends via `gpui-pre-platform`); GPU-accelerated; virtualized Table/List for huge dir listings; Dock layout for one/two-pane |
| Async runtime | **tokio** — app-owned `Runtime` held in `AppState` | gpui has its own executor; no `gpui_tokio` on crates.io. All OpenDAL/sqlx work is spawned on the app runtime, results handed back to the UI executor        |
| Storage layer | **Apache OpenDAL**                                 | Unified `Operator` + full layer stack; every OpenDAL service supported                                                                                      |
| Local DB      | **SQLite** via **sqlx** (async)                    | `rusqlite` is the simpler sync alternative if you'd rather run it on the background executor                                                                |
| Config        | **config** crate (serde)                           | Human-friendly config with comments                                                                                                                         |
| Secrets       | **keyring**                                        | OS keychain / Secret Service / Credential Manager                                                                                                           |
| Errors        | **thiserror** (libs) + **anyhow** (app)            |                                                                                                                                                             |
| Observability | **tracing** ecosystem                              | Pairs with OpenDAL's Logging/Tracing/Metrics layers                                                                                                         |
| Serialization | **serde**                                          |                                                                                                                                                             |

## What OpenDAL buys you, restated

The headline is de-risking. The two historically hardest parts of a transfer client — **broad protocol support** and a **resilient transfer core** (retry, timeout, throttle, observability) — are handled by a mature Apache project. That leaves `kessel`'s _own_ engineering focused where it should be: the persistent SQLite-backed queue, the provider trait's capability handling and the GPUI experience.

**Positioning:** The smuggler's ship for your data — one cockpit for every system, a transfer engine you can walk away from, and a manifest (SQLite) that survives the jump.
