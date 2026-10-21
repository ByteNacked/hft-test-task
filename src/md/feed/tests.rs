use super::*;
use crate::book;
use tokio::sync::oneshot;
mod mock;
mod service_tests;

#[tokio::test]
async fn old_parse_worker_is_retained_joined_and_its_snapshot_is_discarded() {
    let rules = crate::binance::BinanceAdapter::decode_metadata(include_bytes!(
        "../../../tests/fixtures/binance/metadata.json"
    ))
    .unwrap();
    let (books, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(config::BOOK_LEVELS, &EPOCH)
    }
    .unwrap();
    let mut runner = Runner::<crate::binance::BinanceAdapter> {
        protocol: PhantomData,
        settings: Settings::for_adapter::<crate::binance::BinanceAdapter>(),
        client: reqwest::Client::new(),
        rules,
        synced: Box::new(SyncedBook::new().unwrap()),
        books,
        state: FeedState::Connecting,
        sync_epoch: 0,
        last_diff_id: None,
        next_log: Instant::now(),
        queue: VecDeque::with_capacity(config::SYNC_MESSAGES),
        queued_bytes: 0,
        job: SnapshotJob::Idle,
        backoff: Backoff::new(),
        next_snapshot: Instant::now(),
        sync_deadline: Instant::now(),
        live_since: None,
        online: false,
    };
    runner.begin_sync(Duration::ZERO).unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let (started, ready) = oneshot::channel();
    runner.job = SnapshotJob::Parse {
        epoch: runner.sync_epoch,
        worker: tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            crate::binance::BinanceAdapter::decode_snapshot(
                include_bytes!("../../../tests/fixtures/binance/snapshot.json"),
                rules,
            )
            .map(Box::new)
            .map_err(Into::into)
        }),
    };
    ready.await.unwrap();
    runner.recover(AdapterError::QueueOverflow).unwrap();
    assert!(!runner.job.is_idle());
    assert!(
        timeout(Duration::from_millis(10), runner.job.next())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let completion = runner.job.next().await;
    runner.complete(completion).unwrap();
    assert!(!runner.synced.has_snapshot());
    reader.update();
    assert_eq!(reader.view().status, BookStatus::Offline);
    runner.job.parse::<crate::binance::BinanceAdapter>(
        runner.sync_epoch,
        include_bytes!("../../../tests/fixtures/binance/snapshot.json").to_vec(),
        rules,
    );
    let completion = runner.job.next().await;
    runner.complete(completion).unwrap();
    assert!(runner.synced.has_snapshot());
    // Shutdown also waits for an uncancellable parse, instead of detaching it.
    let (release, wait) = std::sync::mpsc::channel();
    runner.job = SnapshotJob::Parse {
        epoch: runner.sync_epoch,
        worker: tokio::task::spawn_blocking(move || {
            wait.recv().unwrap();
            Ok(Box::new(
                crate::binance::BinanceAdapter::decode_snapshot(
                    include_bytes!("../../../tests/fixtures/binance/snapshot.json"),
                    rules,
                )
                .unwrap(),
            ))
        }),
    };
    let drain = runner.job.drain();
    tokio::pin!(drain);
    assert!(
        timeout(Duration::from_millis(10), &mut drain)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    drain.await.unwrap();
}
