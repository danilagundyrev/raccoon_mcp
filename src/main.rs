mod config;
mod domain;
mod server;
mod store;

use anyhow::Context;
use clap::Parser;
use config::Args;
use store::SqliteStore;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(&args.log_level);

    let store = SqliteStore::open(
        &args.database,
        args.max_db_connections,
        args.lease_seconds,
        args.worker_lease_seconds,
    )
    .await
    .with_context(|| {
        format!(
            "failed to initialize SQLite database at {}",
            args.database.display()
        )
    })?;

    server::run_server(
        args.host,
        args.port,
        store,
        args.database,
        args.max_summary_bytes,
        args.allow_hosts,
    )
    .await
}

fn init_logging(level: &str) {
    let level_filter = match level {
        "DEBUG" => LevelFilter::DEBUG,
        "WARNING" => LevelFilter::WARN,
        "ERROR" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    };

    let _ = tracing_subscriber::fmt()
        .with_max_level(level_filter)
        .try_init();
}
