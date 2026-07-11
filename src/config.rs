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
        value_parser = clap::value_parser!(u64).range(60..=86400),
        help = "Default orchestrator lease duration in seconds."
    )]
    pub lease_seconds: u64,

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
