use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexpected_worker_exit_or_panic_is_an_error() {
    for panic in [false, true] {
        let handle = tokio::spawn(async move {
            if panic {
                panic!("worker failure");
            }
            Ok(())
        });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let mut workers = Workers {
            feeds: [Some(handle), None],
            robot: None,
        };
        assert!(matches!(
            workers.reap(false).await,
            Some(ServiceError::Worker {
                worker: "binance",
                ..
            })
        ));
        assert!(workers.empty());

        let handle = std::thread::spawn(move || {
            assert!(!panic, "robot failure");
        });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        workers.robot = Some(handle);
        assert!(matches!(
            workers.reap(false).await,
            Some(ServiceError::Worker {
                worker: "robot",
                ..
            })
        ));
        assert!(workers.empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_still_reports_worker_errors() {
    let handle = tokio::spawn(async { Err(AdapterError::Disconnected) });
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    let mut workers = Workers {
        feeds: [Some(handle), None],
        robot: None,
    };
    assert!(workers.reap(true).await.is_some());
}
