use super::*;

#[test]
fn unexpected_worker_exit_or_panic_is_an_error() {
    for panic in [false, true] {
        let handle = std::thread::spawn(move || {
            if panic {
                panic!("worker failure");
            }
            Ok(())
        });
        while !handle.is_finished() {
            std::thread::yield_now();
        }
        let mut workers = Workers {
            feeds: [Some(handle), None],
            robot: None,
        };
        assert!(matches!(
            workers.reap(false),
            Some(ServiceError::Worker {
                worker: "binance",
                ..
            })
        ));
        assert!(workers.empty());
    }
}

#[test]
fn shutdown_still_reports_worker_errors() {
    let handle = std::thread::spawn(|| Err(AdapterError::Disconnected));
    while !handle.is_finished() {
        std::thread::yield_now();
    }
    let mut workers = Workers {
        feeds: [Some(handle), None],
        robot: None,
    };
    assert!(workers.reap(true).is_some());
}
