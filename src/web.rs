use std::sync::{Arc, Mutex};

use actix_web::{HttpResponse, get, post, web};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::db;
use crate::worker::UpdateBatch;

pub type Db = web::Data<Arc<Mutex<Connection>>>;
pub type Tx = web::Data<broadcast::Sender<UpdateBatch>>;

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
    hidden_count: i64,
}

#[derive(Deserialize)]
struct ToggleRequest {
    repo: String,
    number: i64,
    hidden: bool,
}

#[derive(Deserialize)]
struct ToggleReadRequest {
    repo: String,
    number: i64,
    read: bool,
}

#[get("/")]
pub async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(PAGE_HTML)
}

#[get("/api/events")]
pub async fn events(db: Db, tx: Tx, hash: BuildHash) -> HttpResponse {
    let (init_data, mut rx) = {
        let conn = db.lock().unwrap();
        let prs = db::list_prs(&conn, true);
        let review_prs = db::list_review_prs(&conn);
        let hidden_count = db::hidden_count(&conn);
        let init = InitPayload { build_hash: hash.as_ref().clone(), prs, review_prs, hidden_count };
        (serde_json::to_string(&init).unwrap(), tx.subscribe())
    };

    let stream_hash = hash.as_ref().clone();
    let stream = async_stream::stream! {
        // Send initial full state
        yield Ok::<_, actix_web::Error>(
            web::Bytes::from(format!("event: init\ndata: {}\n\n", init_data))
        );

        // Stream updates as they arrive
        loop {
            match rx.recv().await {
                Ok(batch) => {
                    // Inject build_hash into the update JSON
                    if let Ok(mut json) = serde_json::to_value(&batch) {
                        json["build_hash"] = serde_json::Value::String(stream_hash.clone());
                        if let Ok(s) = serde_json::to_string(&json) {
                            yield Ok(web::Bytes::from(format!("event: update\ndata: {}\n\n", s)));
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("SSE client lagged, missed {} messages", n);
                    // Send a full refresh
                    // Client should reconnect to get full state
                }
                Err(broadcast::error::RecvError::Closed) => break,
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
    let hidden_val: i64 = if body.hidden { 1 } else { 0 };
    let pr = {
        let conn = db.lock().unwrap();
        db::set_hidden(&conn, &body.repo, body.number, hidden_val);
        db::get_pr(&conn, &body.repo, body.number)
    };

    // Broadcast the change to all SSE clients
    if let Some(pr) = pr {
        let hidden_count = {
            let conn = db.lock().unwrap();
            db::hidden_count(&conn)
        };
        let batch = UpdateBatch {
            pr_updates: vec![crate::worker::PrUpdate::Changed(pr)],
            review_updates: vec![],
            hidden_count,
        };
        let _ = tx.send(batch);
    }

    HttpResponse::Ok().json(serde_json::json!({"ok": true}))
}

#[post("/api/toggle-review-read")]
pub async fn api_toggle_review_read(db: Db, tx: Tx, body: web::Json<ToggleReadRequest>) -> HttpResponse {
    let review_pr = {
        let conn = db.lock().unwrap();
        db::set_review_read(&conn, &body.repo, body.number, body.read);
        db::get_review_pr(&conn, &body.repo, body.number)
    };

    if let Some(pr) = review_pr {
        let hidden_count = {
            let conn = db.lock().unwrap();
            db::hidden_count(&conn)
        };
        let batch = UpdateBatch {
            pr_updates: vec![],
            review_updates: vec![crate::worker::ReviewPrUpdate::Changed(pr)],
            hidden_count,
        };
        let _ = tx.send(batch);
    }

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
  #conn-error {
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
  tr.read-row { opacity: 0.4; }
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
  }
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

  /* Menu */
  .menu-cell { position: relative; }
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
    top: 100%;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    z-index: 10;
    white-space: nowrap;
    overflow: hidden;
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
</style>
</head>
<body>
<div id="conn-error">Unable to reach server</div>
<div class="header">
  <h1>PRView</h1>
  <span class="badge">Live</span>
</div>
<div class="tabs">
  <button class="tab active" data-tab="my-prs">My Open PRs <span class="tab-count" id="my-prs-count">0</span></button>
  <button class="tab" data-tab="reviews">Needs Attention <span class="tab-count" id="reviews-count">0</span></button>
</div>

<div id="my-prs-panel" class="tab-panel active">
  <div class="card">
    <div class="filter-bar" id="my-prs-filter-bar"></div>
    <table>
      <thead>
        <tr>
          <th style="width:36px"></th><th>Repo</th><th>PR</th><th>Title</th>
          <th>Review</th>
          <th>CI</th>
          <th>DrCI</th>
          <th>Comments</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody id="my-prs-body">
        <tr><td colspan="9" class="empty-state">Connecting...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<div id="reviews-panel" class="tab-panel">
  <div class="card">
    <div class="filter-bar" id="reviews-filter-bar"></div>
    <table>
      <thead>
        <tr>
          <th style="width:36px"></th><th>Repo</th><th>PR</th><th>Title</th><th>Author</th>
          <th>Review</th>
          <th>CI</th>
          <th>Comments</th>
          <th style="width:40px"></th>
        </tr>
      </thead>
      <tbody id="reviews-body">
        <tr><td colspan="9" class="empty-state">Connecting...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
// --- State ---
let buildHash = null;
let allPrs = [];
let allReviewPrs = [];
let hiddenCount = 0;

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
    return `<span class="pill pill-green" title="${escapeHtml(tip)}"><span class="pill-dot"></span>Approved</span>`;
  if (pr.review_status === 'CHANGES_REQUESTED')
    return `<span class="pill pill-red" title="${escapeHtml(tip)}"><span class="pill-dot"></span>Changes</span>`;
  if (reviewers.length > 0)
    return `<span class="pill pill-yellow" title="${escapeHtml(tip)}"><span class="pill-dot"></span>Pending</span>`;
  return `<span class="pill pill-muted" title="${escapeHtml(tip)}"><span class="pill-dot"></span>None</span>`;
}

function testsPill(pr) {
  const total = pr.checks_success + pr.checks_fail + pr.checks_pending;
  if (total === 0) return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  const tip = `${pr.checks_success} passed, ${pr.checks_fail} failed, ${pr.checks_pending} pending`;
  if (pr.checks_fail > 0)
    return `<span class="pill pill-red" title="${tip}"><span class="pill-dot"></span>${pr.checks_fail} failed</span>`;
  if (pr.checks_pending > 0)
    return `<span class="pill pill-yellow" title="${tip}"><span class="spinner"></span>${pr.checks_pending} pending</span>`;
  return `<span class="pill pill-green" title="${tip}"><span class="pill-dot"></span>Passing</span>`;
}

function drciPill(pr) {
  if (!pr.drci_emoji) return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  const tip = escapeHtml(pr.drci_status);
  const m = {
    'white_check_mark': ['pill-green', 'Passing'],
    'x': ['pill-red', 'Failing'],
    'hourglass_flowing_sand': ['pill-yellow', 'Running'],
  };
  const [cls, label] = m[pr.drci_emoji] || ['pill-muted', 'Unknown'];
  const dot = cls === 'pill-yellow' ? '<span class="spinner"></span>' : '<span class="pill-dot"></span>';
  return `<span class="pill ${cls}" title="${tip}">${dot}${label}</span>`;
}

function checksOverallPill(pr) {
  if (!pr.checks_overall) return '<span class="pill pill-muted"><span class="pill-dot"></span>None</span>';
  if (pr.checks_overall === 'SUCCESS')
    return '<span class="pill pill-green" title="Checks passed"><span class="pill-dot"></span>Passing</span>';
  if (pr.checks_overall === 'FAILURE' || pr.checks_overall === 'ERROR')
    return '<span class="pill pill-red" title="Checks failed"><span class="pill-dot"></span>Failed</span>';
  return '<span class="pill pill-yellow" title="Checks pending"><span class="spinner"></span>Pending</span>';
}

function reviewStatusPill(pr) {
  if (pr.is_draft)
    return '<span class="pill pill-muted">Draft</span>';
  if (pr.review_status === 'APPROVED')
    return '<span class="pill pill-green"><span class="pill-dot"></span>Approved</span>';
  if (pr.review_status === 'CHANGES_REQUESTED')
    return '<span class="pill pill-red"><span class="pill-dot"></span>Changes</span>';
  return '<span class="pill pill-yellow"><span class="spinner"></span>Awaiting</span>';
}

function commentCell(pr) {
  if (pr.comment_count > 0) return `<span class="comment-count">${pr.comment_count}</span>`;
  return '';
}

function prKey(pr) { return pr.repo + '#' + pr.number; }

// --- My PRs tab ---
function renderMyPrs() {
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

  document.getElementById('my-prs-count').textContent = allPrs.filter(p => !p.hidden).length;

  if (visible.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No PRs found yet (fetching in background...)</td></tr>';
    return;
  }

  tbody.innerHTML = visible.map(pr => {
    const rowClass = pr.hidden ? ' class="hidden-row"' : '';
    const checked = pr.hidden ? ' checked' : '';
    return `<tr${rowClass} data-key="${escapeHtml(prKey(pr))}">
      <td><input type="checkbox"${checked} onchange="toggleHidden('${escapeHtml(pr.repo)}', ${pr.number}, this.checked)" title="Hide this PR"></td>
      <td><span class="repo-text">${escapeHtml(pr.repo)}</span></td>
      <td class="mono"><a href="${escapeHtml(pr.url)}" target="_blank">#${pr.number}</a></td>
      <td class="title-cell"><a href="${escapeHtml(pr.url)}" target="_blank">${escapeHtml(pr.title)}</a></td>
      <td>${reviewPill(pr)}</td>
      <td>${testsPill(pr)}</td>
      <td>${drciPill(pr)}</td>
      <td>${commentCell(pr)}</td>
      <td><span class="time-text" title="${escapeHtml(pr.updated_at)}">${relativeTime(pr.updated_at)}</span></td>
    </tr>`;
  }).join('');
}

// --- Reviews tab ---
function renderReviews() {
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
  countEl.textContent = visible.length;
  const unreadCount = visible.filter(p => !p.is_read).length;
  countEl.classList.toggle('attention', unreadCount > 5);

  if (visible.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No review requests found</td></tr>';
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
      <td>${checksOverallPill(pr)}</td>
      <td>${commentCell(pr)}</td>
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
  renderReviews();
}

// --- Updates ---
function applyUpdate(batch) {
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
  allPrs.sort((a, b) => b.updated_at.localeCompare(a.updated_at));

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

  renderAll();
}

function toggleHidden(repo, number, hidden) {
  fetch('/api/toggle-hidden', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({repo, number, hidden}),
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
    body: JSON.stringify({repo, number, read}),
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
      body: JSON.stringify({repo, number, read: true}),
    });
    pr.is_read = true;
    renderReviews();
  }
}

// --- SSE ---
const evtSource = new EventSource('/api/events');

function checkBuildHash(hash) {
  if (buildHash === null) {
    buildHash = hash;
  } else if (hash !== buildHash) {
    location.reload();
  }
}

evtSource.addEventListener('init', (e) => {
  document.getElementById('conn-error').style.display = 'none';
  const data = JSON.parse(e.data);
  checkBuildHash(data.build_hash);
  allPrs = data.prs;
  allReviewPrs = data.review_prs;
  hiddenCount = data.hidden_count;
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
</script>
</body>
</html>
"##;
