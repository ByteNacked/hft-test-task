//! Main-thread supervision. Only finished OS threads are joined here.
use crate::{
    book,
    config::{self, RobotConfig},
    md::{self, AdapterError, MDAdapter},
    robot::{Decision, Robot},
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};
use tokio::{
    sync::watch,
    time::{MissedTickBehavior, interval},
};

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Publication(#[from] book::BookError),
    #[error("{worker}: {detail}")]
    Worker {
        worker: &'static str,
        detail: String,
    },
}

#[derive(Default)]
struct Workers {
    feeds: [Option<JoinHandle<Result<(), AdapterError>>>; 2],
    robot: Option<JoinHandle<()>>,
}
impl Workers {
    fn empty(&self) -> bool {
        self.robot.is_none() && self.feeds.iter().all(Option::is_none)
    }
    fn reap(&mut self, stopping: bool) -> Option<ServiceError> {
        let mut first = None;
        for (name, slot) in ["binance", "aster"].into_iter().zip(&mut self.feeds) {
            if slot.as_ref().is_some_and(JoinHandle::is_finished)
                && let Some(handle) = slot.take()
            {
                let error = match handle.join() {
                    Ok(Ok(())) if stopping => None,
                    Ok(Ok(())) => Some("unexpected worker exit".to_owned()),
                    Ok(Err(error)) => Some(error.to_string()),
                    Err(panic) => Some(panic_message(panic)),
                };
                record_error(&mut first, name, error);
            }
        }
        if self.robot.as_ref().is_some_and(JoinHandle::is_finished)
            && let Some(handle) = self.robot.take()
        {
            let error = match handle.join() {
                Ok(()) if stopping => None,
                Ok(()) => Some("unexpected worker exit".to_owned()),
                Err(panic) => Some(panic_message(panic)),
            };
            record_error(&mut first, "robot", error);
        }
        first
    }
}
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("unknown payload");
    format!("worker panicked: {message}")
}
fn record_error(first: &mut Option<ServiceError>, worker: &'static str, detail: Option<String>) {
    if let Some(detail) = detail {
        tracing::error!(worker, %detail, "worker failed");
        first.get_or_insert(ServiceError::Worker { worker, detail });
    }
}

/// The caller owns the runtime and logging guard and must await this future through cleanup.
/// Network failures recover inside adapters; worker errors/early exits stop the whole service.
pub async fn run(
    robot_config: RobotConfig,
    binance: impl MDAdapter,
    aster: impl MDAdapter,
    shutdown: impl Future<Output = std::io::Result<()>>,
    report: impl FnMut(&Decision) + Send + 'static,
) -> Result<(), ServiceError> {
    let (bw, br) = book::channel(config::BOOK_LEVELS)?;
    let (aw, ar) = book::channel(config::BOOK_LEVELS)?;
    let (stop_feeds, shutdown_feeds) = watch::channel(false);
    let stop_robot = Arc::new(AtomicBool::new(false));
    let mut workers = Workers::default();
    // Preserve ownership of every started worker if a later thread cannot be spawned.
    let startup = (|| -> Result<(), ServiceError> {
        workers.robot = Some(Robot::new(br, ar, robot_config).spawn(stop_robot.clone(), report)?);
        workers.feeds[0] = Some(md::spawn_adapter(
            "binance",
            binance,
            bw,
            shutdown_feeds.clone(),
        )?);
        workers.feeds[1] = Some(md::spawn_adapter("aster", aster, aw, shutdown_feeds)?);
        Ok(())
    })();
    let mut failure = startup.err();
    let mut stopping = failure.is_some();
    if stopping {
        stop_robot.store(true, Ordering::Relaxed);
        stop_feeds.send_replace(true);
    }
    let mut checks = interval(config::SUPERVISION_INTERVAL);
    checks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    loop {
        if let Some(error) = workers.reap(stopping) {
            failure.get_or_insert(error);
        }
        if failure.is_some() && !stopping {
            stopping = true;
            // Relaxed stop flag publishes no data; book ownership uses triple_buffer.
            stop_robot.store(true, Ordering::Relaxed);
            stop_feeds.send_replace(true);
        }
        if stopping && workers.empty() {
            break;
        }
        tokio::select! {
            result=&mut shutdown, if !stopping => {
                // Detect workers that already exited before cancellation was requested.
                if let Some(error)=workers.reap(false) {failure.get_or_insert(error);}
                if let Err(error)=result {failure.get_or_insert(error.into());}
                stopping=true;stop_robot.store(true,Ordering::Relaxed);stop_feeds.send_replace(true);
                tracing::debug!("shutdown requested");
            }
            _=checks.tick() => {},
        }
    }
    tracing::debug!("service stopped");
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests;
