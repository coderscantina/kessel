# Contributing to kessel

Thanks for your interest in improving kessel! This document covers the
workflow and conventions for contributions.

## Getting set up

```sh
git clone https://github.com/coderscantina/kessel.git
cd kessel
cargo run            # debug build straight from the checkout
scripts/bundle.sh dev  # or a signed dev .app bundle
```

Requirements: latest stable Rust; macOS 12+ for the app bundle. Tests run
with `cargo test`, lints with `cargo clippy --all-targets`.

## Project layout

| Path | What lives there |
| ---- | ---------------- |
| `src/app.rs` | Root view: sidebar, tabs, spaces, panels |
| `src/ui/` | File pane, bookmarks, transfers, logs, pickers |
| `src/provider.rs` | OpenDAL Operator construction + layer stack |
| `src/queue/` | Persistent transfer queue (engine, store, transfer) |
| `src/config.rs`, `src/keymap.rs` | JSON5 config and keybindings |
| `src/db.rs`, `migrations/` | SQLite state (never secrets) |
| `src/secrets.rs` | OS keychain access |
| `assets/config/` | Compiled-in defaults (config, keybindings) |

Architecture ground rules:

- **Secrets never touch SQLite.** They go to the OS keychain via
  `secrets.rs`, referenced by handle only.
- **All I/O runs on the app-owned tokio runtime** (`AppState::runtime`);
  results are handed back to the UI executor. Don't block the UI thread.
- **Resilience comes from OpenDAL layers** (retry/timeout/throttle), not
  hand-rolled loops. Compose layers; don't reimplement them.
- **User-facing state is durable.** Queue state, sessions and bookmarks
  persist to SQLite; config and keybindings are hand-editable JSON5 whose
  comments must survive programmatic writes.

## Workflow

1. **Fork** the repository and create a feature branch from `main`:
   `git checkout -b feature/my-improvement`
2. **Make your changes.** Keep PRs focused — one feature or fix per PR.
3. **Add tests** where the change is testable (see the inline `#[cfg(test)]`
   modules for the style used).
4. **Verify** before pushing:
   ```sh
   cargo test
   cargo clippy --all-targets
   cargo fmt --check
   ```
5. **Open a pull request** against `main` with a clear description of the
   problem and the approach.

## Commit conventions

Use [Gitmoji](https://gitmoji.dev) — an emoji prefix declaring the intent of
the commit:

```
✨ add WebDAV lock support
🐛 fix resume offset drift on flaky SFTP connections
📝 clarify bucket-root browsing
♻️ extract conflict resolution from the transfer worker
🔖 Release v26.7.10
```

Keep the subject line imperative and under ~72 characters; explain the *why*
in the body when it isn't obvious. The changelog is generated from these
subjects, so write them for release notes.

## Releases

kessel versions by [CalVer](https://calver.org): `vYY.M.D` (e.g. `v26.7.10`),
one release per day at most. `scripts/release.sh` bumps `Cargo.toml`, creates
the `🔖 Release` commit, folds the generated `CHANGELOG.md` entry into it, and
pushes the tag — which triggers the GitHub release workflow (build, sign,
notarize, package, publish).

## Code style

- `rustfmt` formatting, `clippy`-clean.
- Errors: `thiserror` for library-ish modules, `anyhow` at the app edge.
- Comments explain constraints and non-obvious decisions, not what the next
  line does. Match the density of the surrounding code.
- UI strings go through the localization table (`src/i18n.rs`) — see
  *Localization* below.

## Localization

User-visible strings in menus and dialogs resolve through `src/i18n.rs`.
To contribute a translation, add a locale file under `assets/i18n/`
(e.g. `de.json5`) mirroring the keys in `en.json5`, and list it in the
module's locale table. Missing keys fall back to English, so partial
translations are fine to ship.

## Reporting bugs

Open a GitHub issue with:

- What you did, what you expected, what happened instead
- The backend/protocol involved (S3, SFTP, …) and platform
- Relevant output from the in-app log panel (`cmd-alt-j`) — set
  `logging.filter` to `"kessel=debug,opendal=debug"` in `config.json5` for
  verbose logs

For security vulnerabilities, email
[hello@coderscantina.com](mailto:hello@coderscantina.com) instead of opening
a public issue.

## License

By contributing, you agree that your contributions are licensed under the
[MIT license](LICENSE).
