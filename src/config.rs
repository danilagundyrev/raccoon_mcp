use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about = "Run the local agent coordination MCP hub.")]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1", help = "Host interface to bind.")]
    pub host: String,

    #[arg(long, default_value_t = 8765, help = "TCP port to bind.")]
    pub port: u16,

    #[arg(
        long,
        default_value = ".agent-sync/hub.sqlite3",
        help = "Path to the local SQLite database."
    )]
    pub database: PathBuf,

    #[arg(
        long,
        default_value_t = 4,
        value_parser = clap::value_parser!(u32).range(1..=32),
        help = "Maximum number of SQLite connections."
    )]
    pub max_db_connections: u32,

    #[arg(
        long,
        default_value_t = 1800,
        value_parser = clap::value_parser!(u16).range(60..=65535),
        help = "Default orchestrator lease duration in seconds."
    )]
    pub lease_seconds: u16,

    #[arg(
        long,
        default_value_t = 3600,
        value_parser = clap::value_parser!(u16).range(60..=65535),
        help = "Default worker-run lease duration in seconds."
    )]
    pub worker_lease_seconds: u16,

    #[arg(
        long,
        default_value_t = 600,
        value_parser = clap::value_parser!(u16).range(60..=65535),
        help = "Default repository-request claim lease duration in seconds."
    )]
    pub request_claim_lease_seconds: u16,

    #[arg(
        long,
        default_value_t = 1024,
        value_parser = clap::value_parser!(u16).range(128..=65535),
        help = "Maximum UTF-8 byte length of compact summary fields."
    )]
    pub max_summary_bytes: u16,

    #[arg(
        long,
        default_value_t = 524_288,
        value_parser = clap::value_parser!(u32).range(1024..=1_048_576),
        help = "Maximum UTF-8 byte length of one artifact body."
    )]
    pub max_artifact_bytes: u32,

    #[arg(
        long,
        default_value = "INFO",
        value_parser = ["DEBUG", "INFO", "WARNING", "ERROR"],
        help = "Logging level."
    )]
    pub log_level: String,

    #[arg(
        long = "allow-host",
        help = "Extra Origin hostnames to allow if the request includes an Origin header."
    )]
    pub allow_hosts: Vec<String>,
}
