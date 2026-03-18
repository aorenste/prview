# PRView

A self-hosted Rust web dashboard for monitoring your GitHub PRs and review requests. Runs on a devserver with HTTPS, polls GitHub via GraphQL, and pushes live updates to the browser via Server-Sent Events.

## Architecture

```
main.rs          Entry point, CLI args, TLS setup, server startup
  |
  +-- db.rs      SQLite storage with versioned migrations (PRAGMA user_version)
  +-- github.rs  GitHub GraphQL queries via `gh api graphql` CLI
  +-- worker.rs  Background fetch loop, diff computation, broadcast
  +-- web.rs     HTTP endpoints, SSE streaming, HTML/JS frontend (inline)
```

### main.rs
CLI argument parsing (clap), database init, broadcast channel creation, worker spawn, TLS/OpenSSL setup, actix-web server binding. Uses SO_REUSEADDR via socket2 for clean restarts. Prints the `.fbinfra.net` URL for VPNless devserver access (ports 44200-44209).

### db.rs
SQLite via rusqlite (bundled). Two tables: `prs` (my authored PRs) and `review_prs` (PRs requesting my review). Schema versioned with `PRAGMA user_version` — migrations run sequentially on startup. Handles legacy unversioned databases by detecting existing tables. Key types: `PrRow`, `ReviewPrRow`, `PrInsert`.

### github.rs
Two parallel GraphQL queries via `gh api graphql`:
- **My PRs** (`author:@me`): Full query with check contexts (first 100), comments (for DrCI parsing), reviews.
- **Review PRs** (`review-requested:@me`): Lighter query with just `statusCheckRollup { state }` (no individual check contexts or comments — those cause GitHub 504 timeouts on 50+ PRs).

Returns `FetchResult { my_prs, review_prs }`. Extracts: review status, reviewer list, check counts, DrCI status (from pytorch-bot comments), comment count, overall CI rollup state.

### worker.rs
Background loop on a configurable interval. Snapshots old DB state, fetches from GitHub, stores new data, computes granular diffs (Changed/Removed per PR), and broadcasts via `tokio::sync::broadcast`. Only broadcasts when something actually changed.

### web.rs
Three endpoints:
- `GET /` — Static HTML+JS single-page app (inline in `PAGE_HTML` const)
- `GET /api/events` — SSE endpoint. Sends `event: init` with full state on connect, `event: update` with diffs on changes.
- `POST /api/toggle-hidden` — JSON API to hide/unhide PRs (persisted in DB)

The frontend has two tabs:
- **My Open PRs**: Review status, CI checks (detailed counts), DrCI, comments, updated time, hide/unhide checkbox
- **Needs Attention**: Review-requested PRs with draft/approved/rejected filters, author, overall CI status

Toggle states and active tab persist in `localStorage`. A build hash (compile-time hash of web.rs source) is included in SSE events — if the server restarts with new code, the browser auto-reloads.

## Building and Running

```sh
cargo build
cargo run -- --port 44200 --interval 5m
```

CLI options:
- `--port` (default 44200)
- `--cert` / `--key` (TLS cert/key paths, default to machine certs)
- `--db` (SQLite path, default `prview.db`)
- `--interval` (fetch interval, human-readable like `5m`, `30s`, `1h`)

**For local debugging, use `--port 8443`** (or any non-44200 port). Port 44200 is reserved for the real running instance.

Requires `gh` CLI authenticated with GitHub.

## Dependencies

actix-web (openssl), async-stream, clap (derive), hostname, humantime, openssl, rusqlite (bundled), serde/serde_json, socket2, tokio
