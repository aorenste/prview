use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use actix_web::{HttpResponse, get, post, web};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::db;
use crate::worker::UpdateBatch;

pub type Db = web::Data<Arc<Mutex<Connection>>>;
pub type Tx = web::Data<broadcast::Sender<UpdateBatch>>;
pub type Nudge = web::Data<Arc<std::sync::atomic::AtomicBool>>;
pub type ActiveUsers = web::Data<Arc<Mutex<HashMap<String, usize>>>>;

pub fn build_hash() -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    include_str!("web.rs").hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub type BuildHash = web::Data<String>;

#[derive(Serialize)]
struct InitPayload {
    build_hash: String,
    prs: Vec<db::PrRow>,
    review_prs: Vec<db::ReviewPrRow>,
    merged_prs: Vec<db::MergedPrRow>,
    hidden_count: i64,
}

#[derive(Deserialize)]
struct ToggleRequest {
    user: Option<String>,
    repo: String,
    number: i64,
    hidden: bool,
}

#[derive(Deserialize)]
struct ToggleReadRequest {
    user: Option<String>,
    repo: String,
    number: i64,
    read: bool,
}

#[derive(Deserialize)]
struct UserQuery {
    user: Option<String>,
}

#[get("/")]
pub async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(PAGE_HTML)
}

/// Drop guard that unregisters the user from active_users when the SSE stream is dropped.
struct SseDropGuard {
    user: String,
    active_users: Arc<Mutex<HashMap<String, usize>>>,
}

impl Drop for SseDropGuard {
    fn drop(&mut self) {
        let mut map = self.active_users.lock().unwrap();
        let label = if self.user.is_empty() { "@me" } else { &self.user };
        if let Some(count) = map.get_mut(&self.user) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.user);
                eprintln!("[{}] SSE disconnected (0 conns)", label);
            } else {
                eprintln!("[{}] SSE disconnected ({} conn{})",
                    label, count, if *count == 1 { "" } else { "s" });
            }
        }
    }
}

#[get("/api/events")]
pub async fn events(
    db: Db,
    tx: Tx,
    hash: BuildHash,
    active_users: ActiveUsers,
    query: web::Query<UserQuery>,
) -> HttpResponse {
    let user = query.user.clone().unwrap_or_default();

    // Register this user so the worker fetches for them
    {
        let mut map = active_users.lock().unwrap();
        let count = map.entry(user.clone()).or_insert(0);
        *count += 1;
        eprintln!("[{}] SSE connected ({} conn{})",
            if user.is_empty() { "@me" } else { &user }, count, if *count == 1 { "" } else { "s" });
    }

    let guard = Arc::new(SseDropGuard {
        user: user.clone(),
        active_users: active_users.clone().into_inner().as_ref().clone(),
    });

    let (init_data, mut rx) = {
        let conn = db.lock().unwrap();
        let prs = db::list_prs(&conn, true, &user);
        let review_prs = db::list_review_prs(&conn, &user);
        let merged_prs = db::list_merged_prs(&conn, &user);
        let hidden_count = db::hidden_count(&conn, &user);
        let init = InitPayload { build_hash: hash.as_ref().clone(), prs, review_prs, merged_prs, hidden_count };
        (serde_json::to_string(&init).unwrap(), tx.subscribe())
    };

    let stream_hash = hash.as_ref().clone();
    let stream = async_stream::stream! {
        let _guard = guard; // prevent drop until stream ends

        // Send initial full state
        yield Ok::<_, actix_web::Error>(
            web::Bytes::from(format!("event: init\ndata: {}\n\n", init_data))
        );

        // Stream updates with periodic heartbeat
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(15));
        heartbeat.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(batch) => {
                            if batch.target_user != user {
                                continue;
                            }
                            if let Ok(mut json) = serde_json::to_value(&batch) {
                                json["build_hash"] = serde_json::Value::String(stream_hash.clone());
                                if let Ok(s) = serde_json::to_string(&json) {
                                    yield Ok(web::Bytes::from(format!("event: update\ndata: {}\n\n", s)));
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            eprintln!("SSE client lagged, missed {} messages", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok(web::Bytes::from(": heartbeat\n\n"));
                }
            }
        }
    };

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(stream)
}

#[post("/api/toggle-hidden")]
pub async fn api_toggle_hidden(db: Db, tx: Tx, body: web::Json<ToggleRequest>) -> HttpResponse {
    let user = body.user.clone().unwrap_or_default();
    let hidden_val: i64 = if body.hidden { 1 } else { 0 };
    let pr = {
        let conn = db.lock().unwrap();
        db::set_hidden(&conn, &body.repo, body.number, hidden_val, &user);
        db::get_pr(&conn, &body.repo, body.number, &user)
    };

    if let Some(pr) = pr {
        let hidden_count = {
            let conn = db.lock().unwrap();
            db::hidden_count(&conn, &user)
        };
        let batch = UpdateBatch {
            target_user: user,
            pr_updates: vec![crate::worker::PrUpdate::Changed(pr)],
            review_updates: vec![],
            merged_prs: vec![],
            hidden_count,
            error: None,
        };
        let _ = tx.send(batch);
    }

    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

#[post("/api/toggle-review-read")]
pub async fn api_toggle_review_read(db: Db, tx: Tx, body: web::Json<ToggleReadRequest>) -> HttpResponse {
    let user = body.user.clone().unwrap_or_default();
    let review_pr = {
        let conn = db.lock().unwrap();
        db::set_review_read(&conn, &body.repo, body.number, body.read, &user);
        db::get_review_pr(&conn, &body.repo, body.number, &user)
    };

    if let Some(pr) = review_pr {
        let hidden_count = {
            let conn = db.lock().unwrap();
            db::hidden_count(&conn, &user)
        };
        let batch = UpdateBatch {
            target_user: user,
            pr_updates: vec![],
            review_updates: vec![crate::worker::ReviewPrUpdate::Changed(pr)],
            merged_prs: vec![],
            hidden_count,
            error: None,
        };
        let _ = tx.send(batch);
    }

    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

#[post("/api/refresh")]
pub async fn api_refresh(nudge: Nudge) -> HttpResponse {
    nudge.store(true, std::sync::atomic::Ordering::Relaxed);
    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

#[post("/api/set-user")]
pub async fn api_set_user(
    nudge: Nudge,
) -> HttpResponse {
    // Just nudge — the SSE reconnect will register the user
    nudge.store(true, std::sync::atomic::Ordering::Relaxed);
    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

const PAGE_HTML: &str = r##"<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>PRView</title>
<style>
  :root {
    --bg-body: #0f172a;
    --bg-card: #1e293b;
    --bg-header: #0f172a;
    --bg-hover: rgba(99, 102, 241, 0.08);
    --border: #334155;
    --text: #e2e8f0;
    --text-bright: #f8fafc;
    --text-muted: #64748b;
    --text-dim: #475569;
    --accent: #818cf8;
    --accent-bg: rgba(99, 102, 241, 0.15);
    --green: #4ade80;
    --green-bg: rgba(74, 222, 128, 0.12);
    --red: #f87171;
    --red-bg: rgba(248, 113, 113, 0.12);
    --yellow: #facc15;
    --yellow-bg: rgba(250, 204, 21, 0.12);
    --pill-muted-bg: rgba(100, 116, 139, 0.15);
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg-body);
    color: var(--text);
    padding: 24px 32px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }

  /* Connection error banner */
  #conn-error, #fetch-error {
    display: none;
    background: var(--red-bg);
    border: 1px solid var(--red);
    color: var(--red);
    padding: 10px 16px;
    text-align: center;
    font-weight: 500;
    position: sticky;
    top: 0;
    z-index: 100;
    border-radius: 8px;
    margin-bottom: 16px;
  }
  #fetch-error {
    background: var(--yellow-bg);
    border-color: var(--yellow);
    color: var(--yellow);
  }

  /* Header */
  .header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 24px;
  }
  .header h1 {
    font-size: 1.5em;
    font-weight: 700;
    color: var(--text-bright);
    letter-spacing: -0.02em;
  }
  .header .badge {
    font-size: 0.7em;
    color: var(--accent);
    background: var(--accent-bg);
    padding: 2px 8px;
    border-radius: 6px;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }

  /* Segmented tabs */
  .tabs {
    display: inline-flex;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 3px;
    margin-bottom: 20px;
    gap: 2px;
  }
  .tab {
    padding: 8px 18px;
    cursor: pointer;
    border: none;
    background: none;
    font-size: 0.9em;
    color: var(--text-muted);
    border-radius: 8px;
    transition: all 0.15s ease;
    font-family: inherit;
    font-weight: 500;
    white-space: nowrap;
  }
  .tab:hover { color: var(--text); background: rgba(255,255,255,0.04); }
  .tab.active {
    color: var(--text-bright);
    background: var(--accent-bg);
    box-shadow: 0 1px 3px rgba(0,0,0,0.2);
  }
  .tab-count {
    display: inline-block;
    background: rgba(255,255,255,0.1);
    border-radius: 8px;
    padding: 1px 7px;
    font-size: 0.85em;
    margin-left: 6px;
    font-weight: 600;
    min-width: 22px;
    text-align: center;
  }
  .tab.active .tab-count {
    background: rgba(255,255,255,0.15);
  }
  .tab-count-dim {
    opacity: 0.5;
  }

  /* Card container */
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
  }

  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* Filter chips */
  .filter-bar {
    display: flex;
    gap: 8px;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    align-items: center;
  }
  .filter-bar:empty { display: none; }
  .chip {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 4px 12px;
    border-radius: 16px;
    font-size: 0.8em;
    font-weight: 500;
    cursor: pointer;
    border: 1px solid var(--border);
    background: none;
    color: var(--text-muted);
    transition: all 0.15s ease;
    font-family: inherit;
  }
  .chip:hover { border-color: var(--text-dim); color: var(--text); }
  .chip.active { border-color: var(--accent); color: var(--accent); background: var(--accent-bg); }
  .chip .chip-count {
    font-size: 0.9em;
    opacity: 0.7;
  }

  /* Table */
  table { border-collapse: collapse; width: 100%; }
  th {
    text-align: left;
    padding: 10px 14px;
    font-size: 0.75em;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    background: var(--bg-header);
    border-bottom: 1px solid var(--border);
  }
  td {
    padding: 10px 14px;
    border-bottom: 1px solid rgba(51, 65, 85, 0.5);
    vertical-align: middle;
  }
  td.icon { text-align: center; font-size: 1em; }
  td.mono { font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace; font-size: 0.85em; }
  tr:hover { background: var(--bg-hover); }
  tr.hidden-row { opacity: 0.4; }
  tr.draft-row { opacity: 0.45; }
  tr.read-row > td:not(.menu-cell) { opacity: 0.4; }
  tbody tr:last-child td { border-bottom: none; }

  /* Links */
  a { color: var(--accent); text-decoration: none; transition: color 0.1s; }
  a:hover { color: #a5b4fc; }

  /* PR title link styling */
  td.title-cell a { color: var(--text-bright); font-weight: 500; }
  td.title-cell a:hover { color: var(--accent); }

  /* Repo text */
  .repo-text { color: var(--text-dim); font-size: 0.85em; }

  /* Status pills */
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.8em;
    font-weight: 500;
    white-space: nowrap;
    cursor: default;
    position: relative;
  }
  .pill[data-tip]:hover::after {
    content: attr(data-tip);
    position: absolute;
    bottom: calc(100% + 6px);
    left: 50%;
    transform: translateX(-50%);
    background: #1a1a2e;
    color: var(--text);
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 0.85em;
    white-space: nowrap;
    pointer-events: none;
    z-index: 50;
    border: 1px solid var(--border);
    animation: tooltip-fade 0s 0.3s forwards;
    opacity: 0;
  }
  @keyframes tooltip-fade { to { opacity: 1; } }
  .pill-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }
  .pill-green { background: var(--green-bg); color: var(--green); }
  .pill-green .pill-dot { background: var(--green); }
  .pill-red { background: var(--red-bg); color: var(--red); }
  .pill-red .pill-dot { background: var(--red); }
  .pill-yellow { background: var(--yellow-bg); color: var(--yellow); }
  .pill-yellow .pill-dot { background: var(--yellow); }
  .pill-muted { background: var(--pill-muted-bg); color: var(--text-muted); }
  .pill-muted .pill-dot { background: var(--text-muted); }
  .pill-accent { background: var(--accent-bg); color: var(--accent); }

  /* Comment count */
  .comment-count {
    color: var(--text-muted);
    font-size: 0.85em;
    font-variant-numeric: tabular-nums;
  }

  /* Updated time */
  .time-text {
    color: var(--text-muted);
    font-size: 0.85em;
    white-space: nowrap;
  }

  /* Author */
  .author-text {
    color: var(--text);
    font-size: 0.9em;
  }

  /* Checkbox */
  input[type="checkbox"] {
    accent-color: var(--accent);
    cursor: pointer;
    width: 14px;
    height: 14px;
  }

  /* Sortable headers */
  th.sortable { cursor: pointer; user-select: none; white-space: nowrap; }
  th.sortable:hover { color: var(--text); }
  th.sortable .sort-arrow { margin-left: 4px; font-size: 0.8em; opacity: 0.5; }
  th.sortable.sort-active .sort-arrow { opacity: 1; color: var(--accent); }

  /* Menu */
  .menu-cell { position: relative; z-index: 1; }
  .menu-cell:has(.dropdown.open) { z-index: 20; }
  .menu-btn {
    cursor: pointer;
    border: none;
    background: none;
    font-size: 1.1em;
    padding: 4px 8px;
    border-radius: 6px;
    color: var(--text-muted);
    transition: all 0.1s;
  }
  .menu-btn:hover { background: rgba(255,255,255,0.08); color: var(--text); }
  .dropdown {
    display: none;
    position: absolute;
    right: 0;
    bottom: 100%;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    z-index: 10;
    white-space: nowrap;
  }
  .dropdown.open { display: block; }
  .dropdown a {
    display: block;
    padding: 8px 16px;
    color: var(--text);
    font-size: 0.9em;
  }
  .dropdown a:hover { background: rgba(255,255,255,0.06); color: var(--text-bright); }

  /* Empty state */
  .empty-state {
    text-align: center;
    padding: 48px 16px;
    color: var(--text-muted);
  }

  /* Spinning loader for pending */
  @keyframes spin { to { transform: rotate(360deg); } }
  .spinner {
    display: inline-block;
    width: 6px;
    height: 6px;
    border: 1.5px solid var(--yellow);
    border-top-color: transparent;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    flex-shrink: 0;
  }

  /* Pulse for attention count */
  @keyframes pulse-glow {
    0%, 100% { box-shadow: 0 0 0 0 rgba(248, 113, 113, 0); }
    50% { box-shadow: 0 0 8px 2px rgba(248, 113, 113, 0.3); }
  }
  .tab-count.attention {
    background: var(--red-bg);
    color: var(--red);
    animation: pulse-glow 2s ease-in-out infinite;
  }

  /* Refresh button */
  .refresh-btn {
    border: 1px solid var(--border);
    background: none;
    color: var(--text-muted);
    font-size: 1.2em;
    cursor: pointer;
    padding: 4px 10px;
    border-radius: 8px;
    transition: all 0.15s ease;
    font-family: inherit;
    line-height: 1;
  }
  .refresh-btn:hover { color: var(--text); border-color: var(--text-dim); }
  .refresh-btn.spinning {
    animation: spin 0.8s linear infinite;
    pointer-events: none;
    color: var(--accent);
    border-color: var(--accent);
  }

  /* User toggle */
  .user-toggle {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-left: auto;
  }
  .user-input {
    background: var(--bg-card);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: inherit;
    font-size: 0.85em;
    padding: 5px 10px;
    border-radius: 8px;
    width: 130px;
    outline: none;
    transition: border-color 0.15s;
  }
  .user-input:focus { border-color: var(--accent); }
  .user-input.active-user { border-color: var(--accent); color: var(--accent); }

  /* Recently Landed section */
  .landed-section {
    margin-top: 16px;
  }
  .landed-toggle {
    background: none;
    border: none;
    color: var(--text-muted);
    font-size: 13px;
    cursor: pointer;
    padding: 8px 4px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .landed-toggle:hover { color: var(--text); }
  #landed-arrow {
    font-size: 10px;
    transition: transform 0.15s;
    display: inline-block;
  }
  #landed-arrow.open { transform: rotate(90deg); }
  .landed-count {
    background: var(--bg-header);
    color: var(--text-muted);
    font-size: 11px;
    padding: 1px 6px;
    border-radius: 9999px;
  }
  .landed-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  .landed-table td {
    padding: 6px 10px;
    border-top: 1px solid var(--border);
    color: var(--text-muted);
  }
  .landed-table a { color: var(--text-muted); text-decoration: none; }
  .landed-table a:hover { color: var(--text); text-decoration: underline; }
  .landed-table .title-cell { max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
</head>
<body>
<div id="conn-error">Unable to reach server</div>
<div id="fetch-error"></div>
<div class="header">
  <h1>PRView</h1>
  <span class="badge">Live</span>
  <button id="refresh-btn" class="refresh-btn" onclick="refreshNow()" title="Refresh now">&#x21bb;</button>
  <div class="user-toggle">
    <button class="chip active" id="user-me-btn" onclick="setUser('')">me</button>
    <input type="text" id="user-input" class="user-input" placeholder="username" spellcheck="false" autocomplete="off"
           onfocus="this.select()"
           onkeydown="if(event.key==='Enter'){setUser(this.value);this.blur();}">
  </div>
</div>
<div class="tabs">
  <button class="tab active" data-tab="my-prs">My Open PRs <span class="tab-count" id="my-prs-count">0</span><span class="tab-count tab-count-dim" id="my-prs-draft-count" style="display:none"></span></button>
  <button class="tab" data-tab="reviews">Needs Attention <span class="tab-count" id="reviews-count">0</span><span class="tab-count tab-count-dim" id="reviews-read-count" style="display:none"></span></button>
</div>

<div id="my-prs-panel" class="tab-panel active">
  <div class="card">
    <div class="filter-bar" id="my-prs-filter-bar"></div>
    <table>
      <thead id="my-prs-thead">
      </thead>
      <tbody id="my-prs-body">
        <tr><td colspan="9" class="empty-state">Connecting...</td></tr>
      </tbody>
    </table>
  </div>
  <div id="landed-section" class="landed-section" style="display:none">
    <button class="landed-toggle" id="landed-toggle" onclick="toggleLanded()">
      <span id="landed-arrow">&#9654;</span> Recently Landed <span class="landed-count" id="landed-count">0</span>
    </button>
    <div id="landed-body" style="display:none">
      <table class="landed-table">
        <tbody id="landed-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<div id="reviews-panel" class="tab-panel">
  <div class="card">
    <div class="filter-bar" id="reviews-filter-bar"></div>
    <table>
      <thead id="reviews-thead">
      </thead>
      <tbody id="reviews-body">
        <tr><td colspan="11" class="empty-state">Connecting...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
// --- State ---
let buildHash = null;
let allPrs = [];
let allReviewPrs = [];
let allMergedPrs = [];
let hiddenCount = 0;
let hasFetched = false;

// Persist toggle state in localStorage
function loadPref(key, fallback) {
  try { const v = localStorage.getItem('prview.' + key); return v === null ? fallback : v === 'true'; }
  catch { return fallback; }
}
function savePref(key, val) {
  try { localStorage.setItem('prview.' + key, val); } catch {}
}

let activeTab = loadPref('activeTab', false) ? 'reviews' : 'my-prs';
let showHidden = loadPref('showHidden', false);
let showDrafts = loadPref('showDrafts', false);
let showApproved = loadPref('showApproved', false);
let showRejected = loadPref('showRejected', false);

// --- Sort state ---
function loadSortPref(key, defaultCol, defaultDir) {
  try {
    const v = getCookie('prview_sort_' + key);
    if (v) { const [col, dir] = v.split(':'); return { col, dir }; }
  } catch {}
  return { col: defaultCol, dir: defaultDir };
}
function saveSortPref(key, col, dir) {
  setCookie('prview_sort_' + key, col + ':' + dir);
}

let myPrsSort = loadSortPref('my', 'updated_at', 'desc');
let reviewsSort = loadSortPref('reviews', 'updated_at', 'desc');

const myPrsCols = [
  { key: null, label: '', style: 'width:36px' },
  { key: 'repo', label: 'Repo' },
  { key: 'number', label: 'PR' },
  { key: 'title', label: 'Title' },
  { key: 'review_status', label: 'Review' },
  { key: 'checks_overall', label: 'CI' },
  { key: 'drci_emoji', label: 'DrCI' },
  { key: 'comment_count', label: 'Comments' },
  { key: 'updated_at', label: 'Updated' },
];
const reviewsCols = [
  { key: null, label: '', style: 'width:36px' },
  { key: 'repo', label: 'Repo' },
  { key: 'number', label: 'PR' },
  { key: 'title', label: 'Title' },
  { key: 'author', label: 'Author' },
  { key: 'review_status', label: 'Review' },
  { key: 'checks_overall', label: 'CI' },
  { key: 'drci_emoji', label: 'DrCI' },
  { key: 'comment_count', label: 'Comments' },
  { key: 'updated_at', label: 'Updated' },
  { key: null, label: '', style: 'width:40px' },
];

function renderHeaders(theadId, cols, sortState, onSort) {
  const thead = document.getElementById(theadId);
  thead.innerHTML = '<tr>' + cols.map(c => {
    if (!c.key) return `<th${c.style ? ' style="' + c.style + '"' : ''}></th>`;
    const active = sortState.col === c.key;
    const arrow = active ? (sortState.dir === 'asc' ? '\u25b2' : '\u25bc') : '\u25b4';
    const cls = 'sortable' + (active ? ' sort-active' : '');
    return `<th class="${cls}" data-sort="${c.key}"${c.style ? ' style="' + c.style + '"' : ''}>${c.label}<span class="sort-arrow">${arrow}</span></th>`;
  }).join('') + '</tr>';
  thead.querySelectorAll('th.sortable').forEach(th => {
    th.onclick = () => onSort(th.dataset.sort);
  });
}

function genericCompare(a, b, col) {
  let va = a[col], vb = b[col];
  if (va == null) va = '';
  if (vb == null) vb = '';
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  if (typeof va === 'boolean' && typeof vb === 'boolean') return (va ? 1 : 0) - (vb ? 1 : 0);
  return String(va).localeCompare(String(vb));
}

function sortList(list, sortState) {
  const dir = sortState.dir === 'asc' ? 1 : -1;
  list.sort((a, b) => dir * genericCompare(a, b, sortState.col));
}

// --- Tabs ---
function activateTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${id}"]`).classList.add('active');
  document.getElementById(id + '-panel').classList.add('active');
  activeTab = id;
  savePref('activeTab', id === 'reviews');
}
document.querySelectorAll('.tab').forEach(tab => {
  tab.onclick = () => activateTab(tab.dataset.tab);
});
activateTab(activeTab);

// --- Shared helpers ---
function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function relativeTime(iso) {
  if (!iso) return '';
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diff = Math.max(0, now - then);
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return mins + 'm ago';
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return hrs + 'h ago';
  const days = Math.floor(hrs / 24);
  if (days < 30) return days + 'd ago';
  const months = Math.floor(days / 30);
  return months + 'mo ago';
}

function reviewPill(pr) {
  let reviewers;
  try { reviewers = JSON.parse(pr.reviewers); } catch { reviewers = []; }
  const tip = reviewers.length === 0
    ? 'No reviews yet'
    : reviewers.map(r => r.login + ': ' + r.state).join('\n');
  if (pr.review_status === 'APPROVED')
    return `<span class="pill pill-green" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Approved</span>`;
  if (pr.review_status === 'CHANGES_REQUESTED')
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Changes</span>`;
  if (reviewers.length > 0)
    return `<span class="pill pill-yellow" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Pending</span>`;
  return `<span class="pill pill-muted" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>None</span>`;
}

function drciPill(pr) {
  if (!pr.drci_emoji) {
    if (pr.checks_pending > 0)
      return '<span class="pill pill-yellow"><span class="spinner"></span>Pending</span>';
    return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  }
  const tip = escapeHtml(pr.drci_status);
  const m = {
    'white_check_mark': ['pill-green', 'Passing'],
    'x': ['pill-red', pr.checks_pending > 0 ? 'Failing' : 'Failed'],
    'hourglass_flowing_sand': ['pill-yellow', 'Running'],
  };
  const [cls, label] = m[pr.drci_emoji] || ['pill-muted', 'Unknown'];
  const spinning = cls === 'pill-yellow' || (pr.checks_pending > 0);
  const dot = spinning
    ? `<span class="spinner"${cls === 'pill-red' ? ' style="border-color: var(--red); border-top-color: transparent;"' : ''}></span>`
    : '<span class="pill-dot"></span>';
  return `<span class="pill ${cls}" data-tip="${tip}">${dot}${label}</span>`;
}

function ciApprovalPill(pr) {
  if (!pr.ci_approval_needed) return '';
  return '<span class="pill pill-red"><span class="spinner" style="border-color: var(--red); border-top-color: transparent;"></span>CI Approval</span>';
}

function checksOverallPill(pr) {
  if (!pr.checks_overall) return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  if (pr.checks_overall === 'SUCCESS')
    return '<span class="pill pill-green" data-tip="Checks passed"><span class="pill-dot"></span>Passing</span>';
  if (pr.checks_overall === 'FAILURE' || pr.checks_overall === 'ERROR') {
    if (pr.checks_running)
      return '<span class="pill pill-red" data-tip="Checks failing, still running"><span class="spinner" style="border-color: var(--red); border-top-color: transparent;"></span>Failed</span>';
    return '<span class="pill pill-red" data-tip="Checks failed"><span class="pill-dot"></span>Failed</span>';
  }
  return '<span class="pill pill-yellow" data-tip="Checks pending"><span class="spinner"></span>Pending</span>';
}

function detailedCIPill(pr) {
  const total = (pr.checks_success || 0) + (pr.checks_fail || 0) + (pr.checks_pending || 0);
  if (total === 0) return checksOverallPill(pr);
  const tip = `${pr.checks_success} passed, ${pr.checks_fail} failed, ${pr.checks_pending} pending`;
  if (pr.checks_fail > 0) {
    const dot = pr.checks_running
      ? '<span class="spinner" style="border-color: var(--red); border-top-color: transparent;"></span>'
      : '<span class="pill-dot"></span>';
    return `<span class="pill pill-red" data-tip="${escapeHtml(tip)}">${dot}${pr.checks_fail} failed</span>`;
  }
  if (pr.checks_pending > 0)
    return `<span class="pill pill-yellow" data-tip="${escapeHtml(tip)}"><span class="spinner"></span>${pr.checks_pending} pending</span>`;
  return `<span class="pill pill-green" data-tip="${escapeHtml(tip)}"><span class="pill-dot"></span>Passing</span>`;
}

function ciOrLandingPill(pr) {
  const total = (pr.checks_success || 0) + (pr.checks_fail || 0) + (pr.checks_pending || 0);
  const tip = total > 0
    ? escapeHtml(`${pr.checks_success} passed, ${pr.checks_fail} failed, ${pr.checks_pending} pending`)
    : '';
  if (pr.landing_status === 'landing')
    return `<span class="pill pill-yellow" data-tip="${tip}"><span class="spinner"></span>Landing</span>`;
  if (pr.landing_status === 'reverted')
    return `<span class="pill pill-red" data-tip="${tip}"><span class="pill-dot"></span>Reverted</span>`;
  if (pr.landing_status === 'failed')
    return `<span class="pill pill-red" data-tip="${tip}"><span class="pill-dot"></span>Land Failed</span>`;
  return detailedCIPill(pr);
}

function reviewStatusPill(pr) {
  if (pr.is_draft)
    return '<span class="pill pill-muted">Draft</span>';
  if (pr.review_status === 'APPROVED')
    return '<span class="pill pill-green"><span class="pill-dot"></span>Approved</span>';
  if (pr.review_status === 'CHANGES_REQUESTED')
    return '<span class="pill pill-red"><span class="pill-dot"></span>Changes</span>';
  return '<span class="pill pill-yellow"><span class="pill-dot"></span>Awaiting</span>';
}

function commentCell(pr) {
  if (pr.comment_count > 0) return `<span class="comment-count">${pr.comment_count}</span>`;
  return '';
}

function prKey(pr) { return pr.repo + '#' + pr.number; }

// --- My PRs tab ---
function toggleMyPrsSort(col) {
  if (myPrsSort.col === col) myPrsSort.dir = myPrsSort.dir === 'asc' ? 'desc' : 'asc';
  else { myPrsSort.col = col; myPrsSort.dir = 'desc'; }
  saveSortPref('my', myPrsSort.col, myPrsSort.dir);
  sortList(allPrs, myPrsSort);
  renderMyPrs();
}

function renderMyPrs() {
  renderHeaders('my-prs-thead', myPrsCols, myPrsSort, toggleMyPrsSort);
  const visible = showHidden ? allPrs : allPrs.filter(p => !p.hidden);
  const tbody = document.getElementById('my-prs-body');
  const bar = document.getElementById('my-prs-filter-bar');

  if (hiddenCount > 0) {
    bar.innerHTML = `<button class="chip${showHidden ? ' active' : ''}" id="hide-toggle">
      ${showHidden ? 'Showing' : 'Show'} ${hiddenCount} hidden <span class="chip-count"></span>
    </button>`;
    document.getElementById('hide-toggle').onclick = () => {
      showHidden = !showHidden;
      savePref('showHidden', showHidden);
      renderMyPrs();
    };
  } else {
    bar.innerHTML = '';
  }

  const nonHidden = allPrs.filter(p => !p.hidden);
  const nonDraftCount = nonHidden.filter(p => !p.is_draft).length;
  const draftCount2 = nonHidden.filter(p => p.is_draft).length;
  document.getElementById('my-prs-count').textContent = nonDraftCount;
  const draftCountEl = document.getElementById('my-prs-draft-count');
  if (draftCount2 > 0) {
    draftCountEl.textContent = '+' + draftCount2;
    draftCountEl.style.display = '';
  } else {
    draftCountEl.style.display = 'none';
  }

  if (visible.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state">' +
      (hasFetched ? 'No open PRs' : 'Fetching...') + '</td></tr>';
    return;
  }

  tbody.innerHTML = visible.map(pr => {
    let cls = [];
    if (pr.hidden) cls.push('hidden-row');
    if (pr.is_draft) cls.push('draft-row');
    const rowClass = cls.length ? ` class="${cls.join(' ')}"` : '';
    const checked = pr.hidden ? ' checked' : '';
    return `<tr${rowClass} data-key="${escapeHtml(prKey(pr))}">
      <td><input type="checkbox"${checked} onchange="toggleHidden('${escapeHtml(pr.repo)}', ${pr.number}, this.checked)" title="Hide this PR"></td>
      <td><span class="repo-text">${escapeHtml(pr.repo)}</span></td>
      <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank">#${pr.number}</a></td>
      <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank">${escapeHtml(pr.title)}</a></td>
      <td>${pr.is_draft ? '<span class="pill pill-muted">Draft</span>' : reviewPill(pr)}</td>
      <td>${ciOrLandingPill(pr)}</td>
      <td>${drciPill(pr)}</td>
      <td>${commentCell(pr)}</td>
      <td><span class="time-text" title="${escapeHtml(pr.updated_at)}">${relativeTime(pr.updated_at)}</span></td>
    </tr>`;
  }).join('');
}

// --- Recently Landed ---
let landedOpen = loadPref('landedOpen', false);

function toggleLanded() {
  landedOpen = !landedOpen;
  savePref('landedOpen', landedOpen);
  renderLanded();
}

function renderLanded() {
  const section = document.getElementById('landed-section');
  const body = document.getElementById('landed-body');
  const arrow = document.getElementById('landed-arrow');
  const count = document.getElementById('landed-count');

  if (allMergedPrs.length === 0) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';
  count.textContent = allMergedPrs.length;

  if (landedOpen) {
    arrow.classList.add('open');
    body.style.display = '';
    const tbody = document.getElementById('landed-tbody');
    tbody.innerHTML = allMergedPrs.map(pr => {
      const repo = escapeHtml(pr.repo);
      const shortRepo = repo.split('/').pop();
      return `<tr>
        <td><span class="repo-text">${shortRepo}</span></td>
        <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank">#${pr.number}</a></td>
        <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank">${escapeHtml(pr.title)}</a></td>
        <td><span class="time-text" title="${escapeHtml(pr.landed_at)}">${relativeTime(pr.landed_at)}</span></td>
      </tr>`;
    }).join('');
  } else {
    arrow.classList.remove('open');
    body.style.display = 'none';
  }
}

// --- Reviews tab ---
function toggleReviewsSort(col) {
  if (reviewsSort.col === col) reviewsSort.dir = reviewsSort.dir === 'asc' ? 'desc' : 'asc';
  else { reviewsSort.col = col; reviewsSort.dir = 'desc'; }
  saveSortPref('reviews', reviewsSort.col, reviewsSort.dir);
  sortList(allReviewPrs, reviewsSort);
  renderReviews();
}

function renderReviews() {
  renderHeaders('reviews-thead', reviewsCols, reviewsSort, toggleReviewsSort);
  let visible = allReviewPrs;
  if (!showDrafts) visible = visible.filter(p => !p.is_draft);
  if (!showApproved) visible = visible.filter(p => p.review_status !== 'APPROVED');
  if (!showRejected) visible = visible.filter(p => p.review_status !== 'CHANGES_REQUESTED');

  const tbody = document.getElementById('reviews-body');
  const bar = document.getElementById('reviews-filter-bar');

  const draftCount = allReviewPrs.filter(p => p.is_draft).length;
  const approvedCount = allReviewPrs.filter(p => p.review_status === 'APPROVED').length;
  const rejectedCount = allReviewPrs.filter(p => p.review_status === 'CHANGES_REQUESTED').length;

  let chips = [];
  if (draftCount > 0) {
    chips.push(`<button class="chip${showDrafts ? ' active' : ''}" id="draft-toggle">
      ${showDrafts ? 'Showing' : 'Show'} ${draftCount} drafts</button>`);
  }
  if (approvedCount > 0) {
    chips.push(`<button class="chip${showApproved ? ' active' : ''}" id="approved-toggle">
      ${showApproved ? 'Showing' : 'Show'} ${approvedCount} approved</button>`);
  }
  if (rejectedCount > 0) {
    chips.push(`<button class="chip${showRejected ? ' active' : ''}" id="rejected-toggle">
      ${showRejected ? 'Showing' : 'Show'} ${rejectedCount} changes requested</button>`);
  }
  bar.innerHTML = chips.join('');

  if (draftCount > 0) {
    document.getElementById('draft-toggle').onclick = () => {
      showDrafts = !showDrafts;
      savePref('showDrafts', showDrafts);
      renderReviews();
    };
  }
  if (approvedCount > 0) {
    document.getElementById('approved-toggle').onclick = () => {
      showApproved = !showApproved;
      savePref('showApproved', showApproved);
      renderReviews();
    };
  }
  if (rejectedCount > 0) {
    document.getElementById('rejected-toggle').onclick = () => {
      showRejected = !showRejected;
      savePref('showRejected', showRejected);
      renderReviews();
    };
  }

  // Update count + attention pulse
  const countEl = document.getElementById('reviews-count');
  const readCountEl = document.getElementById('reviews-read-count');
  const unreadCount = visible.filter(p => !p.is_read).length;
  const readCount = visible.length - unreadCount;
  countEl.textContent = unreadCount;
  countEl.classList.toggle('attention', unreadCount > 5);
  if (readCount > 0) {
    readCountEl.textContent = '+' + readCount;
    readCountEl.style.display = '';
  } else {
    readCountEl.style.display = 'none';
  }

  if (visible.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="empty-state">' +
      (hasFetched ? 'No review requests' : 'Fetching...') + '</td></tr>';
    return;
  }

  tbody.innerHTML = visible.map(pr => {
    let cls = [];
    if (pr.is_draft) cls.push('draft-row');
    if (pr.is_read) cls.push('read-row');
    const rowClass = cls.length ? ` class="${cls.join(' ')}"` : '';
    const menuLabel = pr.is_read ? 'Mark unread' : 'Mark read';
    const menuRead = pr.is_read ? 'false' : 'true';
    return `<tr${rowClass} data-key="${escapeHtml(prKey(pr))}">
      <td>${reviewStatusPill(pr)}</td>
      <td><span class="repo-text">${escapeHtml(pr.repo)}</span></td>
      <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank" onclick="markRead('${escapeHtml(pr.repo)}', ${pr.number})">#${pr.number}</a></td>
      <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank" onclick="markRead('${escapeHtml(pr.repo)}', ${pr.number})">${escapeHtml(pr.title)}</a></td>
      <td><span class="author-text">${escapeHtml(pr.author)}</span></td>
      <td>${reviewPill(pr)}</td>
      <td>${detailedCIPill(pr)} ${ciApprovalPill(pr)}</td>
      <td>${drciPill(pr)}</td>
      <td>${commentCell(pr)}</td>
      <td><span class="time-text" title="${escapeHtml(pr.updated_at || '')}">${relativeTime(pr.updated_at)}</span></td>
      <td class="menu-cell">
        <button class="menu-btn" onclick="toggleMenu(event)">&#x22ef;</button>
        <div class="dropdown">
          <a href="#" onclick="setReviewRead(event, '${escapeHtml(pr.repo)}', ${pr.number}, ${menuRead})">${menuLabel}</a>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function renderAll() {
  renderMyPrs();
  renderLanded();
  renderReviews();
}

// --- Updates ---
function applyUpdate(batch) {
  const fetchErr = document.getElementById('fetch-error');
  if (batch.error) {
    fetchErr.textContent = 'Fetch error: ' + batch.error;
    fetchErr.style.display = 'block';
    return;
  }
  fetchErr.style.display = 'none';
  hasFetched = true;

  hiddenCount = batch.hidden_count;

  for (const u of batch.pr_updates) {
    if (u.type === 'changed') {
      const key = prKey(u);
      const idx = allPrs.findIndex(p => prKey(p) === key);
      if (idx >= 0) allPrs[idx] = u;
      else allPrs.push(u);
    } else if (u.type === 'removed') {
      const key = u.repo + '#' + u.number;
      allPrs = allPrs.filter(p => prKey(p) !== key);
    }
  }
  sortList(allPrs, myPrsSort);

  for (const u of batch.review_updates) {
    if (u.type === 'changed') {
      const key = prKey(u);
      const idx = allReviewPrs.findIndex(p => prKey(p) === key);
      if (idx >= 0) allReviewPrs[idx] = u;
      else allReviewPrs.push(u);
    } else if (u.type === 'removed') {
      const key = u.repo + '#' + u.number;
      allReviewPrs = allReviewPrs.filter(p => prKey(p) !== key);
    }
  }
  sortList(allReviewPrs, reviewsSort);

  if (batch.merged_prs && batch.merged_prs.length > 0) {
    allMergedPrs = batch.merged_prs;
  }

  renderAll();
}

function toggleHidden(repo, number, hidden) {
  fetch('/api/toggle-hidden', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: currentUser, repo, number, hidden}),
  });
  const key = repo + '#' + number;
  const pr = allPrs.find(p => prKey(p) === key);
  if (pr) {
    pr.hidden = hidden;
    hiddenCount += hidden ? 1 : -1;
    renderAll();
  }
}

function toggleMenu(e) {
  e.stopPropagation();
  const dd = e.currentTarget.nextElementSibling;
  document.querySelectorAll('.dropdown.open').forEach(d => { if (d !== dd) d.classList.remove('open'); });
  dd.classList.toggle('open');
}

document.addEventListener('click', () => {
  document.querySelectorAll('.dropdown.open').forEach(d => d.classList.remove('open'));
});

function setReviewRead(e, repo, number, read) {
  e.preventDefault();
  e.stopPropagation();
  document.querySelectorAll('.dropdown.open').forEach(d => d.classList.remove('open'));
  fetch('/api/toggle-review-read', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: currentUser, repo, number, read}),
  });
  const key = repo + '#' + number;
  const pr = allReviewPrs.find(p => prKey(p) === key);
  if (pr) {
    pr.is_read = read;
    renderReviews();
  }
}

function markRead(repo, number) {
  const key = repo + '#' + number;
  const pr = allReviewPrs.find(p => prKey(p) === key);
  if (pr && !pr.is_read) {
    fetch('/api/toggle-review-read', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({user: currentUser, repo, number, read: true}),
    });
    pr.is_read = true;
    renderReviews();
  }
}

let currentUser = '';

// Cookie helpers
function setCookie(name, val) {
  document.cookie = name + '=' + encodeURIComponent(val) + ';path=/;max-age=31536000;SameSite=Lax';
}
function getCookie(name) {
  const m = document.cookie.match('(?:^|; )' + name + '=([^;]*)');
  return m ? decodeURIComponent(m[1]) : '';
}

function setUser(user) {
  user = user.trim();
  currentUser = user;
  setCookie('prview_user', user);
  const meBtn = document.getElementById('user-me-btn');
  const input = document.getElementById('user-input');
  if (user === '') {
    meBtn.classList.add('active');
    input.classList.remove('active-user');
    input.value = '';
  } else {
    meBtn.classList.remove('active');
    input.classList.add('active-user');
    input.value = user;
  }
  allPrs = [];
  allReviewPrs = [];
  allMergedPrs = [];
  hiddenCount = 0;
  hasFetched = false;
  renderAll();
  fetch('/api/set-user', { method: 'POST' });
  connectSSE();
}

function refreshNow() {
  const btn = document.getElementById('refresh-btn');
  btn.classList.add('spinning');
  fetch('/api/refresh', { method: 'POST' })
    .finally(() => setTimeout(() => btn.classList.remove('spinning'), 1000));
}

// --- SSE ---
let evtSource = null;

function checkBuildHash(hash) {
  if (buildHash === null) {
    buildHash = hash;
  } else if (hash !== buildHash) {
    location.reload();
  }
}

function connectSSE() {
  if (evtSource) evtSource.close();
  const params = currentUser ? '?user=' + encodeURIComponent(currentUser) : '';
  evtSource = new EventSource('/api/events' + params);

  evtSource.addEventListener('init', (e) => {
    document.getElementById('conn-error').style.display = 'none';
    const data = JSON.parse(e.data);
    checkBuildHash(data.build_hash);
    allPrs = data.prs;
    sortList(allPrs, myPrsSort);
    allReviewPrs = data.review_prs;
    sortList(allReviewPrs, reviewsSort);
    allMergedPrs = data.merged_prs || [];
    hiddenCount = data.hidden_count;
    hasFetched = (allPrs.length > 0 || allReviewPrs.length > 0);
    renderAll();
  });

  evtSource.addEventListener('update', (e) => {
    document.getElementById('conn-error').style.display = 'none';
    const batch = JSON.parse(e.data);
    checkBuildHash(batch.build_hash);
    applyUpdate(batch);
  });

  evtSource.onerror = () => {
    document.getElementById('conn-error').style.display = 'block';
  };
}

// Restore user from cookie
const savedUser = getCookie('prview_user');
if (savedUser) {
  currentUser = savedUser;
  const meBtn = document.getElementById('user-me-btn');
  const input = document.getElementById('user-input');
  meBtn.classList.remove('active');
  input.classList.add('active-user');
  input.value = savedUser;
}

connectSSE();
</script>
</body>
</html>
"##;
