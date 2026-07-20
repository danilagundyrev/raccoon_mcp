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
        default_value = ".raccoon-mcp/hub.sqlite3",
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
        default_value_t = 3600,
        value_parser = clap::value_parser!(u16).range(60..=65535),
        help = "Default work-item claim lease duration in seconds."
    )]
    pub work_item_claim_lease_seconds: u16,

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
        default_value_t = 60,
        value_parser = clap::value_parser!(u16).range(1..=65535),
        help = "Maximum duration of one wait_for_events call in seconds."
    )]
    pub max_wait_seconds: u16,

    #[arg(
        long,
        default_value_t = 30,
        value_parser = clap::value_parser!(u16).range(1..=3600),
        help = "Interval between maintenance passes in seconds."
    )]
    pub maintenance_interval_seconds: u16,

    #[arg(
        long,
        default_value_t = 0,
        value_parser = clap::value_parser!(u16).range(0..=65535),
        help = "Interval between bounded retention passes in seconds; 0 disables retention."
    )]
    pub retention_interval_seconds: u16,

    #[arg(
        long,
        default_value_t = 604_800,
        value_parser = clap::value_parser!(u32).range(3600..=31_536_000),
        help = "Retention for append-only events and idempotency records in seconds."
    )]
    pub retention_seconds: u32,

    #[arg(
        long,
        default_value_t = false,
        help = "Expose the optional GET /metrics endpoint."
    )]
    pub enable_metrics: bool,

    #[arg(
        long,
        default_value_t = 3600,
        value_parser = clap::value_parser!(u16).range(60..=65535),
        help = "Idle MCP session timeout in seconds."
    )]
    pub session_idle_timeout_seconds: u16,

    #[arg(
        long,
        default_value_t = 128,
        value_parser = clap::value_parser!(u16).range(1..=4096),
        help = "Maximum number of in-memory MCP sessions."
    )]
    pub max_sessions: u16,

    #[arg(
        long,
        default_value = "INFO",
        value_parser = ["DEBUG", "INFO", "WARNING", "ERROR"],
        help = "Application logging level; dependencies remain at WARN (ERROR at application ERROR)."
    )]
    pub log_level: String,

    #[arg(
        long = "allow-host",
        help = "Extra Origin hostnames to allow if the request includes an Origin header."
    )]
    pub allow_hosts: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_defaults_keep_optional_operations_disabled() {
        let args = Args::try_parse_from(["raccoon-mcp"]).unwrap();
        assert!(!args.enable_metrics);
        assert_eq!(args.retention_interval_seconds, 0);
        assert_eq!(args.maintenance_interval_seconds, 30);
        assert_eq!(args.max_sessions, 128);
        assert_eq!(args.max_wait_seconds, 60);
    }
}
