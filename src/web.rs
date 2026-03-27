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
    include_str!("../static/style.css").hash(&mut hasher);
    include_str!("../static/app.js").hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub type BuildHash = web::Data<String>;

#[derive(Serialize)]
struct InitPayload {
    build_hash: String,
    prs: Vec<db::PrRow>,
    review_prs: Vec<db::ReviewPrRow>,
    merged_prs: Vec<db::MergedPrRow>,
    issues: Vec<db::IssueRow>,
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

const APP_ICON_SVG: &str = "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 128 128'><rect width='128' height='128' rx='24' fill='#0f172a'/><circle cx='40' cy='36' r='10' fill='none' stroke='#818cf8' stroke-width='7'/><line x1='40' y1='46' x2='40' y2='92' stroke='#818cf8' stroke-width='7' stroke-linecap='round'/><circle cx='88' cy='92' r='10' fill='none' stroke='#818cf8' stroke-width='7'/><line x1='88' y1='82' x2='88' y2='56' stroke='#818cf8' stroke-width='7' stroke-linecap='round'/><path d='M88 56 L76 48 M88 56 L76 64' fill='none' stroke='#818cf8' stroke-width='7' stroke-linecap='round' stroke-linejoin='round'/></svg>";

#[get("/")]
pub async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(PAGE_HTML.as_str())
}

#[get("/icon.svg")]
pub async fn icon() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("image/svg+xml")
        .body(APP_ICON_SVG)
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
        let issues = db::list_issues(&conn, &user);
        let hidden_count = db::hidden_count(&conn, &user);
        let init = InitPayload { build_hash: hash.as_ref().clone(), prs, review_prs, merged_prs, issues, hidden_count };
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
            issue_updates: vec![],
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
            issue_updates: vec![],
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

const PAGE_CSS: &str = include_str!("../static/style.css");
const PAGE_JS: &str = include_str!("../static/app.js");

const PAGE_HTML_TEMPLATE: &str = include_str!("../static/index.html");

static PAGE_HTML: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let hash = build_hash();
    PAGE_HTML_TEMPLATE.replace("style.css", &format!("style.css?v={}", hash))
                      .replace("app.js", &format!("app.js?v={}", hash))
});

// Serve static assets with compile-time embedding
#[get("/static/style.css")]
pub async fn static_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css")
        .insert_header(("Cache-Control", "public, max-age=31536000, immutable"))
        .body(PAGE_CSS)
}

#[get("/static/app.js")]
pub async fn static_js() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/javascript")
        .insert_header(("Cache-Control", "public, max-age=31536000, immutable"))
        .body(PAGE_JS)
}

