macro_rules! log {
    ($($arg:tt)*) => {{
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let secs = (now_secs % 86400) as u32;
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        let (yr, mo, day) = $crate::github::days_to_ymd((now_secs / 86400) as i64);
        eprintln!("{:04}-{:02}-{:02} {:02}:{:02}:{:02} {}", yr, mo, day, h, m, s, format_args!($($arg)*));
    }};
}

mod db;
mod github;
mod web;
mod worker;

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use actix_web::{App, HttpServer};
use clap::Parser;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use socket2::{Domain, Socket, Type};

#[derive(Parser)]
#[command(name = "prview", about = "PR and issue viewer")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 44200)]
    port: u16,

    /// Path to TLS certificate file (PEM)
    #[arg(long)]
    cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM)
    #[arg(long)]
    key: Option<PathBuf>,

    /// Path to SQLite database file
    #[arg(long)]
    db: Option<PathBuf>,

    /// How often to fetch PRs from GitHub (e.g. "5m", "30s", "1h")
    #[arg(long, default_value = "1m", value_parser = humantime::parse_duration)]
    interval: Duration,

    /// HTTP proxy URL (e.g. "http://fwdproxy:8080")
    #[arg(long)]
    proxy: Option<String>,

    /// Log file path (default: next to DB). Use "-" for stderr only.
    #[arg(long)]
    log: Option<String>,
}

const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
const LOG_KEEP: usize = 3; // keep .1 .2 .3

fn rotate_and_open_log(path: &std::path::Path) {
    // Rotate if current log exceeds size limit
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.len() > MAX_LOG_SIZE {
            for i in (1..LOG_KEEP).rev() {
                let from = path.with_extension(format!("log.{}", i));
                let to = path.with_extension(format!("log.{}", i + 1));
                let _ = std::fs::rename(&from, &to);
            }
            let _ = std::fs::rename(path, path.with_extension("log.1"));
            eprintln!("Log rotated");
        }
    }

    // Open log file for append and redirect stderr to it
    use std::os::unix::io::AsRawFd;
    if let Ok(file) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        unsafe {
            libc::dup2(file.as_raw_fd(), 2); // redirect stderr
        }
    } else {
        eprintln!("Warning: could not open log file {:?}", path);
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // Set up log file (unless "-" for stderr-only)
    if args.log.as_deref() != Some("-") {
        let log_path = match &args.log {
            Some(p) => PathBuf::from(p),
            None => {
                let data_dir = std::env::var("XDG_DATA_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|_| {
                        let home = std::env::var("HOME").expect("HOME not set");
                        PathBuf::from(home).join(".local/share")
                    });
                data_dir.join("prview/prview.log")
            }
        };
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        rotate_and_open_log(&log_path);
        log!("Logging to {:?}", log_path);

        // Check for rotation hourly
        let log_path_clone = log_path.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                rotate_and_open_log(&log_path_clone);
            }
        });
    }

    github::init_client(args.proxy.as_deref());
    let gh_user = github::whoami().await;
    log!("Authenticated as: {} (@me)", gh_user);

    // Resolve cert/key paths from current hostname if not specified
    let fqdn = hostname::get()
        .expect("Failed to get hostname")
        .to_string_lossy()
        .into_owned();
    let cert = args
        .cert
        .unwrap_or_else(|| PathBuf::from(format!("/etc/ssl/certs/{}.crt", fqdn)));
    let key = args
        .key
        .unwrap_or_else(|| PathBuf::from(format!("/etc/ssl/certs/{}.key", fqdn)));

    // Resolve DB path: --db flag, or $XDG_DATA_HOME/prview/prview.db
    let db_path = args.db.unwrap_or_else(|| {
        let data_dir = std::env::var("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").expect("HOME not set");
                PathBuf::from(home).join(".local/share")
            });
        data_dir.join("prview/prview.db")
    });
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let conn = db::init_db(&db_path);
    let db = Arc::new(Mutex::new(conn));

    let (tx, _) = tokio::sync::broadcast::channel::<worker::UpdateBatch>(64);

    let build_hash = web::build_hash();
    log!("Build hash: {}", build_hash);

    let nudge = Arc::new(AtomicBool::new(false));
    let active_users = Arc::new(Mutex::new(HashMap::<String, usize>::new()));

    // Spawn background PR fetcher
    let db_clone = db.clone();
    let tx_clone = tx.clone();
    let nudge_clone = nudge.clone();
    let active_users_clone = active_users.clone();
    let interval = args.interval;
    log!("Refresh interval: {}", humantime::format_duration(interval));
    tokio::spawn(async move {
        worker::fetch_prs_loop(db_clone, interval, tx_clone, nudge_clone, active_users_clone).await;
    });

    // Spawn background detail fetcher
    let db_clone2 = db.clone();
    let tx_clone2 = tx.clone();
    let active_users_clone2 = active_users.clone();
    tokio::spawn(async move {
        worker::fetch_details_loop(db_clone2, tx_clone2, active_users_clone2).await;
    });

    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file(&key, SslFiletype::PEM)
        .unwrap_or_else(|e| panic!("Failed to load key {:?}: {}", key, e));
    builder
        .set_certificate_chain_file(&cert)
        .unwrap_or_else(|e| panic!("Failed to load cert {:?}: {}", cert, e));

    let display_host = fqdn.replace(".facebook.com", ".fbinfra.net");
    println!("https://{}:{}", display_host, args.port);

    let db_data = actix_web::web::Data::new(db);
    let tx_data = actix_web::web::Data::new(tx);
    let hash_data = actix_web::web::Data::new(build_hash);
    let nudge_data = actix_web::web::Data::new(nudge);
    let active_users_data = actix_web::web::Data::new(active_users);
    HttpServer::new(move || {
        App::new()
            .app_data(db_data.clone())
            .app_data(tx_data.clone())
            .app_data(hash_data.clone())
            .app_data(nudge_data.clone())
            .app_data(active_users_data.clone())
            .service(web::index)
            .service(web::icon)
            .service(web::static_css)
            .service(web::static_js)
            .service(web::events)
            .service(web::api_toggle_hidden)
            .service(web::api_toggle_review_read)
            .service(web::api_refresh)
            .service(web::api_set_user)
    })
    .listen_openssl(reuseaddr_listener(args.port)?, builder)?
    .run()
    .await
}

fn reuseaddr_listener(port: u16) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::IPV6, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.set_only_v6(false)?;
    socket.bind(&format!("[::]:{}",  port).parse::<std::net::SocketAddr>().unwrap().into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}
