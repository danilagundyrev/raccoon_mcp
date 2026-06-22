mod server;
mod store;

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use server::{resolve_state_file, run_server};
use store::SyncStore;
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Run the local agent sync MCP server.")]
struct Args {
    #[arg(long, default_value = "127.0.0.1", help = "Host interface to bind.")]
    host: String,

    #[arg(long, default_value_t = 8765, help = "TCP port to bind.")]
    port: u16,

    #[arg(
        long,
        help = "Explicit path to the persisted server state JSON file. Overrides --session-id and --state-dir."
    )]
    state_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Logical task/session identifier used to derive state_<session-id>.json when --state-file is omitted."
    )]
    session_id: Option<String>,

    #[arg(
        long,
        default_value = "tools/agent_sync_mcp/.local",
        help = "Directory used for auto-generated session-specific state files."
    )]
    state_dir: PathBuf,

    #[arg(
        long,
        default_value = "INFO",
        value_parser = ["DEBUG", "INFO", "WARNING", "ERROR"],
        help = "Logging level."
    )]
    log_level: String,

    #[arg(
        long = "allow-host",
        help = "Extra Origin hostnames to allow if the request includes an Origin header."
    )]
    allow_hosts: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(&args.log_level);

    let (state_path, session_id) = resolve_state_file(
        args.state_file.as_deref(),
        args.session_id.as_deref(),
        &args.state_dir,
    )?;
    let store = SyncStore::new(state_path.clone(), Some(session_id.clone()))
        .with_context(|| format!("failed to load state from {}", state_path.display()))?;

    run_server(
        args.host,
        args.port,
        store,
        state_path,
        session_id,
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
