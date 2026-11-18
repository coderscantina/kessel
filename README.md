# kessel

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](https://www.rust-lang.org)
[![GUI: GPUI](https://img.shields.io/badge/GUI-GPUI-8A2BE2.svg)](https://www.gpui.rs)
[![Storage: Apache OpenDAL](https://img.shields.io/badge/Storage-Apache%20OpenDAL-green.svg)](https://opendal.apache.org)

> The smuggler's ship for your data — one cockpit for every system, a transfer
> engine you can walk away from, and a manifest that survives the jump.

**kessel** is a fast, native desktop file manager for remote filesystems,
built in Rust on [GPUI](https://www.gpui.rs) and
[Apache OpenDAL](https://opendal.apache.org). It is a Cyberduck/FileZilla-class
client that unifies object storage and classic transfer protocols behind one
interface: S3, SFTP, FTP, WebDAV, Google Cloud Storage, Azure Blob, Google
Drive, Dropbox, OneDrive, and the local filesystem today — and because the
storage layer is OpenDAL, adding a backend mostly means turning a feature
flag on.

## Features

### Browsing

- One or two panes per tab (horizontal / vertical split) with drag & drop
  between panes, to directory rows, and from Finder/Explorer
- Linked browsing: choose **Link panes** in split view to follow matching
  subfolders from the two current directories. Leaving those roots or reaching
  an unavailable folder stops the link
- **Upload…** accepts files and full folder trees, including empty folders
  on backends that support directories
- Virtualized file list built for huge directory listings, with per-backend
  columns (size, modified, unix permissions, S3 storage class, bucket region)
- Finder-grade selection: click, cmd-click, shift-click ranges, rubber-band,
  full keyboard navigation
- Per-pane folder tree, quick look, hidden-file toggle
- Filter the current folder as you type, or search every subfolder by name
  (`cmd-shift-f`): results stream in, can be stopped, and work like any other
  rows. A search stops at 5,000 matches
- A lost connection keeps the last loaded rows on screen, marked as out of
  date, with **Reconnect** to rebuild the connection and renew credentials
- Remote programs and scripts never launch on a double-click: kessel names the
  file and offers to download it instead
- Account-root S3 browsing: save a site without a bucket and the root lists
  every bucket in the account

### Transfers

- **Persistent queue** — every job and per-file task lives in SQLite with its
  state and byte offset, so the queue survives a crash or quit and picks up
  again on relaunch. A file continues from its last byte when the destination
  can append and the source is provably unchanged. Otherwise, and always on
  object stores like S3, that file restarts from zero
- Upload, download and site-to-site copy are the same code path: a layered
  source Operator streamed into a layered destination Operator
- Resolve conflicts per file or for the entire transfer: overwrite, overwrite
  if newer, skip, resume a known partial copy, or keep both with a unique name
- Priorities, pause/resume (per job and global), retries, post-transfer
  verification, timestamp preservation, transfer history
- Global bandwidth and parallel-transfer limits, adjustable while running.
  The `layers` section of `config.json5` sets the per-connection throttle and
  concurrency cap for all sites. Per-site overrides are honored when stored on
  a site, but the site editor has no fields for them yet

### Synchronization

- **Directory sync** between any two sites: a diff pass (size + modified
  time) produces a reviewable plan — copy new, update changed, and in mirror
  mode delete extraneous — then hands the copies to the persistent queue

### Cloud drives

- Google Drive, Dropbox and OneDrive connect through a built-in OAuth flow
  (browser sign-in with PKCE and a loopback redirect); only the long-lived
  refresh token is kept, in the OS keychain, and access tokens renew
  automatically

### AWS, properly

- S3 sites authenticate with access keys (with optional **STS session
  token**), or straight from `~/.aws` profiles — including **SSO** and
  **assume-role** profiles, resolved to short-lived credentials and
  refreshed before they expire

### Organization

- Arc-style sidebar: bookmarks act as pinned tabs, ephemeral tabs below,
  **spaces** switch whole tab sets — the session persists across restarts
- Bookmark folders, tags, icons and accent colors
- Secrets never touch SQLite: passwords, keys and passphrases go to the OS
  keychain, referenced by handle only

### Resilience, by construction

The historically hardest parts of a transfer client — broad protocol support
and a resilient transfer core — are OpenDAL's, not ours. Every connection is
wrapped in a composable, per-site layer stack:

| Layer                           | What it buys you                                          |
| ------------------------------- | --------------------------------------------------------- |
| `RetryLayer`                    | exponential backoff; dropped FTP/S3 connections self-heal |
| `TimeoutLayer`                  | bounded operations; no hung transfers on a dead socket    |
| `ThrottleLayer`                 | bandwidth control, per-site and global                    |
| `ConcurrentLimitLayer`          | caps in-flight operations per backend                     |
| `LoggingLayer` + `TracingLayer` | structured logs feeding the in-app console                |

### Customization

- **Command bar** (`cmd-k`): fuzzy palette over every command, bookmark and
  space, with keystroke hints
- **Settings editor** (`cmd-,`): searchable, Zed-style — categories, inline
  descriptions — writing through to the same hand-editable config file;
  comments survive, values hot-reload
- Hand-editable **JSON5 config** (comments, trailing commas) with live reload —
  every value applies while the app is running
- Configurable keybindings, same JSON5 dialect
- **Zed theme & icon-theme browser**: search the Zed extension gallery,
  install with one click, hot-applied — plus anything you drop into
  `<data_dir>/themes/` or `<data_dir>/extensions/` yourself
- Native menu bar with the standard app/File/Edit/View/Go/Help menus

### Automation

- **Built-in MCP server** (off by default): point Claude Code or any MCP
  client at `http://127.0.0.1:39100/mcp` and browse sites, inspect the queue
  and transfer history, or — explicitly opt-in — enqueue and control
  transfers. Configure under `mcp` in `config.json5`
  (`claude mcp add --transport http kessel http://127.0.0.1:39100/mcp`)

### Accessibility & localization

- Keyboard-first: navigation, selection, search, upload, download, rename,
  delete, pane copy and move, and the transfer queue (`cmd-shift-j`) all work
  from configurable shortcuts and the command bar. A few per-file actions are
  in the context menu only: Download As, Duplicate, Calculate Folder Size,
  permissions, S3 storage class and ACL, and presigned URLs
- Localized UI (English and German built in); add a locale by dropping a
  `<locale>.json5` string table into `<data_dir>/i18n/` — see
  [CONTRIBUTING.md](CONTRIBUTING.md)

## Technical architecture

| Concern       | Choice                                                                                       |
| ------------- | -------------------------------------------------------------------------------------------- |
| Language      | Rust (stable)                                                                                |
| GUI           | [GPUI](https://www.gpui.rs) + [gpui-component](https://github.com/longbridge/gpui-component) |
| Storage layer | [Apache OpenDAL](https://opendal.apache.org) — unified `Operator` + layer stack              |
| Async runtime | tokio (app-owned runtime; gpui keeps its own executor)                                       |
| Local state   | SQLite via sqlx (bookmarks, queue, history, session)                                         |
| Secrets       | OS keychain via `keyring`                                                                    |
| Config        | JSON5 via the `config` crate, live-reloaded                                                  |
| Observability | `tracing` ecosystem                                                                          |

## Getting started

### Prerequisites

- Rust stable (latest; GPUI tracks latest stable)
- macOS 12+ or Windows 10+ — both are built and tested in CI

SFTP is macOS/Linux only: its transport drives the system `ssh` binary through
a unix control socket, so the Windows build ships without that backend. Every
other backend — S3, FTP, WebDAV, GCS, Azure Blob, Drive, Dropbox, OneDrive and
the local filesystem — is available on both.

One other difference: dragging entries out to Finder or Explorer promises
remote *files* on both platforms, but remote *folders* only on macOS — the
Windows protocol needs every file named before the drag starts. Drag a folder
through the transfer queue instead.

### Build & run

```sh
git clone https://github.com/coderscantina/kessel.git
cd kessel
cargo run
```

### Package a macOS app

```sh
scripts/bundle.sh          # release build → target/bundle/release/Kessel.app
scripts/bundle.sh dev      # debug build with the dev icon and bundle id
scripts/package.sh         # sign + notarize + staple → target/dist/Kessel-<version>.dmg
```

`package.sh` reads `KESSEL_SIGN_IDENTITY` (a Developer ID Application
identity) and notarizes via `KESSEL_NOTARY_PROFILE` or
`APPLE_ID`/`APPLE_TEAM_ID`/`APPLE_APP_PASSWORD`; without an identity it falls
back to an ad-hoc-signed, non-notarized DMG for local use.

### Package a Windows build

```powershell
scripts\package-windows.ps1     # → target\dist\kessel-<version>-x86_64.exe (+ .zip, + .sha256)
```

kessel is a single self-contained executable on Windows — assets, icons,
migrations and config defaults are all compiled in — so there is no installer:
put the `.exe` where you want it and run it. The script signs it when
`KESSEL_WIN_CERT` (a `.pfx`) or `KESSEL_WIN_CERT_THUMBPRINT` is set, and warns
that SmartScreen will flag the download when neither is.

The app icon and version metadata are compiled into the executable by
`build.rs`; regenerate the `.ico` files from the SVG sources with
`scripts/gen-icons.sh` after changing the artwork.

## Releases & versioning

- **CalVer**: releases are tagged `vYY.M.D` (e.g. `v26.7.10`) — the version
  tells you exactly how old your build is. At most one release per day.
- **Gitmoji**: commits follow the [Gitmoji](https://gitmoji.dev) convention
  (`✨` feature, `🐛` fix, `♻️` refactor, `🔖` release, …); the
  [CHANGELOG](CHANGELOG.md) is generated from commit subjects, so they're
  written to be read.
- `scripts/release.sh` cuts a release: bumps `Cargo.toml`, creates the
  `🔖 Release` commit, folds the generated changelog entry into it, tags and
  pushes — and the tag triggers the GitHub workflow, which builds every
  platform in parallel (a signed, notarized universal DMG on macOS; a signed
  executable on Windows) and publishes them in one release alongside a
  per-platform update manifest.
- **Auto-update**: kessel checks the latest release on startup (daily
  thereafter, `update.check = false` to opt out) and offers a one-click,
  checksum-verified install-and-relaunch — swapping the `.app` bundle on
  macOS, the `.exe` on Windows.

### Configuration

App defaults ship compiled in; your overrides live in
`<data_dir>/kessel/config.json5` (macOS:
`~/Library/Application Support/kessel/config.json5`; Windows:
`%APPDATA%\kessel\config.json5`) — same shape, comments
welcome, hot-reloaded on save. Keybindings follow the same pattern in
`keybindings.json5`. See [`assets/config/`](assets/config) for the annotated
defaults.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the
workflow, commit conventions, and how to get a development build running.

## Security

If you discover a security vulnerability, please email
[security@coderscantina.com](mailto:security@coderscantina.com) instead of opening a
public issue.

## License

kessel is open-source software licensed under the [MIT license](LICENSE).

## Acknowledgements

- [Apache OpenDAL](https://opendal.apache.org) — the storage layer that makes
  broad protocol support tractable
- [GPUI](https://www.gpui.rs) and [gpui-component](https://github.com/longbridge/gpui-component) — the GPU-accelerated UI stack
- [Lucide](https://lucide.dev) — icons
