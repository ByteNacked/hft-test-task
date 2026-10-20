use super::*;
use crate::{
    md::{EventTime, ReceiveTime},
    numeric::SCALE,
    test_support::*,
};
use std::time::{Duration, Instant};

fn event(now: Instant, first: u64, last: u64, previous: u64, quantity: i64) -> DepthDiff {
    let mut event = diff(now);
    event.first_id = first;
    event.final_id = last;
    event.previous_id = previous;
    event.bids.push(level(99, quantity));
    event
}
#[test]
fn snapshot_bridge_is_inclusive_and_futures_ids_need_not_be_adjacent() {
    let now = Instant::now();
    for (first, last) in [(99, 100), (100, 100), (100, 102), (99, 102)] {
        let mut synced = SyncedBook::new(rules()).unwrap();
        synced.load(&mut snapshot(100, SCALE, 6)).unwrap();
        assert_eq!(
            synced.apply(event(now, 98, 99, 97, 2 * SCALE)).unwrap(),
            Applied::BeforeSnapshot
        );
        assert_eq!(synced.book.event_time(), None);
        assert_eq!(
            synced
                .apply(event(now, first, last, 98, 2 * SCALE))
                .unwrap(),
            Applied::Update
        );
        assert_eq!(
            synced.apply(event(now, 110, 120, last, 3 * SCALE)).unwrap(),
            Applied::Update
        );
        assert_eq!(synced.book.bids()[0].qty.raw(), 3 * SCALE);
    }
}
#[test]
fn duplicate_ignores_receive_time_but_conflicts_regressions_and_gaps_fail() {
    let now = Instant::now();
    let mut synced = SyncedBook::new(rules()).unwrap();
    synced.load(&mut snapshot(100, SCALE, 6)).unwrap();
    synced.apply(event(now, 100, 102, 99, 2 * SCALE)).unwrap();
    assert_eq!(
        synced
            .apply(event(now + Duration::from_secs(1), 100, 102, 99, 2 * SCALE))
            .unwrap(),
        Applied::Duplicate
    );
    assert_eq!(synced.book.event_time().unwrap().received_at(), now);
    for (first, last, previous, qty) in [
        (100, 102, 99, 3 * SCALE),
        (99, 101, 98, 2 * SCALE),
        (103, 104, 101, 2 * SCALE),
    ] {
        assert!(matches!(
            synced.apply(event(now, first, last, previous, qty)),
            Err(AdapterError::Sequence(_))
        ));
        assert_eq!(synced.book.bids()[0].qty.raw(), 2 * SCALE);
        assert_eq!(synced.book.event_time().unwrap().received_at(), now);
    }
    let mut conflict = event(now, 100, 102, 99, 2 * SCALE);
    conflict.time = EventTime::new(123, ReceiveTime::new(456, now).unwrap()).unwrap();
    assert!(matches!(
        synced.apply(conflict),
        Err(AdapterError::Sequence(_))
    ));
    synced.reset();
    synced.load(&mut snapshot(100, SCALE, 6)).unwrap();
    assert!(matches!(
        synced.apply(event(now, 101, 102, 100, SCALE)),
        Err(AdapterError::Sequence(_))
    ));
    assert!(!synced.ready());
}
