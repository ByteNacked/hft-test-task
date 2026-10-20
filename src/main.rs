use clap::Parser;
use hft_test_task::{config::Cli, metrics};
use std::process::ExitCode;

fn main() -> ExitCode {
    let _log_guard = metrics::init_logging();
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let status = if error.use_stderr() {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            };
            if error.print().is_err() {
                return ExitCode::FAILURE;
            }
            // Return normally so the logging guard flushes even on invalid CLI numbers.
            return status;
        }
    };
    match cli.robot_config() {
        Err(error) => {
            tracing::error!(%error, "startup failed");
            ExitCode::FAILURE
        }
        Ok(config) if cli.check_config => {
            tracing::info!(?config, "configuration valid");
            ExitCode::SUCCESS
        }
        Ok(_) => {
            tracing::error!(
                "full service startup is not wired yet; use --check-config or cargo test --locked"
            );
            ExitCode::FAILURE
        }
    }
}
