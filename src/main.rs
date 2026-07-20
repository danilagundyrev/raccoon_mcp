#![recursion_limit = "256"]

mod config;
mod domain;
mod server;
mod store;

use anyhow::Context;
use clap::Parser;
use config::Args;
use store::SqliteStore;
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(&args.log_level);

    let store = SqliteStore::open(
        &args.database,
        args.max_db_connections,
        args.lease_seconds,
        args.worker_lease_seconds,
        args.request_claim_lease_seconds,
        args.work_item_claim_lease_seconds,
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
        server::RuntimeConfig {
            max_summary_bytes: args.max_summary_bytes,
            max_artifact_bytes: args.max_artifact_bytes,
            max_wait_seconds: args.max_wait_seconds,
            maintenance_interval: std::time::Duration::from_secs(u64::from(
                args.maintenance_interval_seconds,
            )),
            retention_interval: (args.retention_interval_seconds > 0).then(|| {
                std::time::Duration::from_secs(u64::from(args.retention_interval_seconds))
            }),
            retention: std::time::Duration::from_secs(u64::from(args.retention_seconds)),
            enable_metrics: args.enable_metrics,
            session_idle_timeout: std::time::Duration::from_secs(u64::from(
                args.session_idle_timeout_seconds,
            )),
            max_sessions: usize::from(args.max_sessions),
        },
        args.allow_hosts,
    )
    .await
}

fn init_logging(level: &str) {
    let _ = tracing_subscriber::registry()
        .with(logging_targets(level))
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

fn logging_targets(level: &str) -> Targets {
    let application_level = match level {
        "DEBUG" => LevelFilter::DEBUG,
        "WARNING" => LevelFilter::WARN,
        "ERROR" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    };
    let dependency_level = if level == "ERROR" {
        LevelFilter::ERROR
    } else {
        LevelFilter::WARN
    };

    Targets::new()
        .with_default(dependency_level)
        .with_target(env!("CARGO_CRATE_NAME"), application_level)
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    use super::*;

    #[test]
    fn debug_logging_does_not_enable_dependency_query_traces() {
        let targets = logging_targets("DEBUG");
        assert!(targets.would_enable("raccoon_mcp::server", &Level::DEBUG));
        assert!(targets.would_enable("sqlx::query", &Level::WARN));
        assert!(!targets.would_enable("sqlx::query", &Level::DEBUG));
        assert!(!targets.would_enable("axum::routing", &Level::INFO));
    }

    #[test]
    fn application_log_level_remains_configurable() {
        let info = logging_targets("INFO");
        assert!(info.would_enable("raccoon_mcp::store", &Level::INFO));
        assert!(!info.would_enable("raccoon_mcp::store", &Level::DEBUG));

        let errors = logging_targets("ERROR");
        assert!(errors.would_enable("raccoon_mcp::server", &Level::ERROR));
        assert!(!errors.would_enable("sqlx::query", &Level::WARN));
    }
}
