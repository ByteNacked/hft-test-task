use clap::Parser;
use hft_test_task::{aster::AsterAdapter, binance::BinanceAdapter, config::Cli, metrics, service};
use std::process::ExitCode;

fn main() -> ExitCode {
    let (_log_guard, log_errors) = metrics::init_logging();
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
        Ok(config) => {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(service::ServiceError::from)
                .and_then(|runtime| {
                    runtime.block_on(async {
                        use tokio::signal::unix::{SignalKind, signal};
                        // Install both handlers before starting workers.
                        let mut interrupt = signal(SignalKind::interrupt())?;
                        let mut terminate = signal(SignalKind::terminate())?;
                        let shutdown = async move {
                            tokio::select! {_=interrupt.recv()=>{},_=terminate.recv()=>{}}
                            Ok(())
                        };
                        service::run(
                            config,
                            BinanceAdapter::default(),
                            AsterAdapter::default(),
                            shutdown,
                            metrics::decision_reporter(),
                            || log_errors.dropped_lines(),
                        )
                        .await
                    })
                });
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    tracing::error!(%error,"service failed");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
