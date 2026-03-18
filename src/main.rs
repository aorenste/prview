mod db;
mod github;
mod web;
mod worker;

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
    #[arg(long, default_value = "/etc/ssl/certs/devgpu035.nha1.facebook.com.crt")]
    cert: PathBuf,

    /// Path to TLS private key file (PEM)
    #[arg(long, default_value = "/etc/ssl/certs/devgpu035.nha1.facebook.com.key")]
    key: PathBuf,

    /// Path to SQLite database file
    #[arg(long, default_value = "prview.db")]
    db: PathBuf,

    /// How often to fetch PRs from GitHub (e.g. "5m", "30s", "1h")
    #[arg(long, default_value = "1m", value_parser = humantime::parse_duration)]
    interval: Duration,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let conn = db::init_db(&args.db);
    let db = Arc::new(Mutex::new(conn));

    let (tx, _) = tokio::sync::broadcast::channel::<worker::UpdateBatch>(64);

    let build_hash = web::build_hash();
    eprintln!("Build hash: {}", build_hash);

    // Spawn background PR fetcher
    let db_clone = db.clone();
    let tx_clone = tx.clone();
    let interval = args.interval;
    eprintln!("Refresh interval: {}", humantime::format_duration(interval));
    tokio::spawn(async move {
        worker::fetch_prs_loop(db_clone, interval, tx_clone).await;
    });

    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file(&args.key, SslFiletype::PEM)
        .unwrap_or_else(|e| panic!("Failed to load key {:?}: {}", args.key, e));
    builder
        .set_certificate_chain_file(&args.cert)
        .unwrap_or_else(|e| panic!("Failed to load cert {:?}: {}", args.cert, e));

    let hostname = hostname::get()
        .map(|h| {
            let name = h.to_string_lossy().into_owned();
            name.replace(".facebook.com", ".fbinfra.net")
        })
        .unwrap_or_else(|_| "localhost".to_string());

    println!("https://{}:{}", hostname, args.port);

    let db_data = actix_web::web::Data::new(db);
    let tx_data = actix_web::web::Data::new(tx);
    let hash_data = actix_web::web::Data::new(build_hash);
    HttpServer::new(move || {
        App::new()
            .app_data(db_data.clone())
            .app_data(tx_data.clone())
            .app_data(hash_data.clone())
            .service(web::index)
            .service(web::events)
            .service(web::api_toggle_hidden)
            .service(web::api_toggle_review_read)
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
