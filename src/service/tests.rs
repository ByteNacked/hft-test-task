use super::*;
use crate::{
    book::{BookPublisher, BookStatus},
    metrics::{FeedMetrics, FeedState},
    robot::Bias,
};
use std::{sync::atomic::AtomicUsize, time::Duration};
use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};
use triple_buffer::Input;

#[derive(Clone, Copy)]
enum Mode {
    Wait,
    Error,
    Exit,
    Panic,
}
struct MockAdapter {
    mode: Mode,
    stopped: Arc<AtomicBool>,
    cleanup: Duration,
}
impl MockAdapter {
    fn new(mode: Mode) -> (Self, Arc<AtomicBool>) {
        let stopped = Arc::new(AtomicBool::new(false));
        (
            Self {
                mode,
                stopped: stopped.clone(),
                cleanup: Duration::from_millis(20),
            },
            stopped,
        )
    }
}
impl MDAdapter for MockAdapter {
    async fn run(
        self,
        mut books: BookPublisher,
        mut metrics: Input<FeedMetrics>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), AdapterError> {
        match self.mode {
            Mode::Error => return Err(AdapterError::Sequence("test failure")),
            Mode::Exit => return Ok(()),
            Mode::Panic => panic!("test adapter panic"),
            Mode::Wait => {}
        }
        md::shutdown_requested(&mut shutdown).await;
        let book = book::OrderBook::new(crate::test_support::rules(), config::BOOK_LEVELS)?;
        books.publish(&book, BookStatus::Offline)?;
        // Model an already-running, non-cancellable snapshot worker during shutdown.
        tokio::task::spawn_blocking(move || std::thread::sleep(self.cleanup)).await?;
        metrics.write(FeedMetrics {
            state: FeedState::Stopped,
            ..Default::default()
        });
        self.stopped.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_joins_all_workers_without_blocking_main_runtime() {
    let (mut b, bs) = MockAdapter::new(Mode::Wait);
    let (mut a, as_) = MockAdapter::new(Mode::Wait);
    b.cleanup = Duration::from_millis(100);
    a.cleanup = b.cleanup;
    let (signal, shutdown) = oneshot::channel();
    let mut signal = Some(signal);
    let done = AtomicBool::new(false);
    let ticks = AtomicUsize::new(0);
    let service = async {
        let result = run(
            Default::default(),
            b,
            a,
            async { shutdown.await.map_err(std::io::Error::other) },
            move |d| {
                assert_eq!(d.bias, Bias::Hold);
                assert_eq!(d.binance.status, BookStatus::Offline);
                assert_eq!(d.aster.status, BookStatus::Offline);
                if let Some(signal) = signal.take() {
                    let _ = signal.send(());
                }
            },
            || 0,
        )
        .await;
        done.store(true, Ordering::Relaxed);
        result
    };
    let heartbeat = async {
        while !done.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(1)).await;
            ticks.fetch_add(1, Ordering::Relaxed);
        }
    };
    let (result, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(service, heartbeat)
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(bs.load(Ordering::Relaxed) && as_.load(Ordering::Relaxed));
    assert!(
        ticks.load(Ordering::Relaxed) > 5,
        "supervisor blocked during cleanup"
    );
}

#[tokio::test]
async fn adapter_error_panic_or_unexpected_exit_stops_and_joins_peer() {
    for mode in [Mode::Error, Mode::Exit, Mode::Panic] {
        for failed_binance in [true, false] {
            let (failed, _) = MockAdapter::new(mode);
            let (peer, stopped) = MockAdapter::new(Mode::Wait);
            let (b, a) = if failed_binance {
                (failed, peer)
            } else {
                (peer, failed)
            };
            let result = timeout(
                Duration::from_secs(3),
                run(
                    Default::default(),
                    b,
                    a,
                    std::future::pending(),
                    |_| {},
                    || 0,
                ),
            )
            .await
            .unwrap();
            let expected = if failed_binance { "binance" } else { "aster" };
            assert!(matches!(result,Err(ServiceError::Worker {worker,..}) if worker==expected));
            assert!(stopped.load(Ordering::Relaxed));
        }
    }
}

#[tokio::test]
async fn robot_panic_stops_and_joins_both_adapters() {
    let (b, bs) = MockAdapter::new(Mode::Wait);
    let (a, as_) = MockAdapter::new(Mode::Wait);
    let result = timeout(
        Duration::from_secs(3),
        run(
            Default::default(),
            b,
            a,
            std::future::pending(),
            |_| panic!("test robot panic"),
            || 0,
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(ServiceError::Worker {
            worker: "robot",
            ..
        })
    ));
    assert!(bs.load(Ordering::Relaxed) && as_.load(Ordering::Relaxed));
}

#[tokio::test]
async fn shutdown_source_failure_is_reported_after_cleanup() {
    let (b, bs) = MockAdapter::new(Mode::Wait);
    let (a, as_) = MockAdapter::new(Mode::Wait);
    let result = timeout(
        Duration::from_secs(3),
        run(
            Default::default(),
            b,
            a,
            async { Err(std::io::Error::other("signal error")) },
            |_| {},
            || 0,
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(ServiceError::Io(_))));
    assert!(bs.load(Ordering::Relaxed) && as_.load(Ordering::Relaxed));
}
