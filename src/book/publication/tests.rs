use crate::test_support::*;
use crate::{
    book::OrderBook,
    book::{self, BookError, BookStatus},
    numeric::SCALE,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant,
};

#[test]
fn offline_start_and_online_requires_applied_diff() {
    let (mut writer, reader) = book::channel(16).unwrap();
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert_eq!(reader.view().exchange_time, None);
    assert_eq!(reader.view().platform_time, None);
    let mut book = OrderBook::new(16).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 6)).unwrap();
    assert_eq!(
        writer.publish(&book, BookStatus::Online),
        Err(BookError::NotReady)
    );
}
#[test]
fn held_snapshot_is_stable_and_brief_offline_can_be_skipped() {
    let now = Instant::now();
    let book = book(now, 100, SCALE, 16);
    let (mut writer, mut reader) = book::channel(16).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    reader.update();
    let held = reader.view();
    writer.publish(&book, BookStatus::Offline).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert_eq!(held.bids.as_slice(), book.bids());
    assert_eq!(held.status, BookStatus::Online);
    assert!(reader.update());

    assert_eq!(reader.view().status, BookStatus::Online);
}
#[test]
fn concurrent_slow_reader_sees_complete_versions() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE, 16);
    let (mut writer, mut reader) = book::channel(16).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let producer_done = done.clone();
    let producer = thread::spawn(move || {
        for n in 1..=2000 {
            let mut update = diff(now);
            update.bids.push(level(99, n));
            update.asks.push(level(101, n));
            book.apply_diff(&update).unwrap();
            writer.publish(&book, BookStatus::Online).unwrap();
        }
        // This flag carries completion only. Book visibility remains triple_buffer's job.
        producer_done.store(true, Ordering::Relaxed);
    });
    while !done.load(Ordering::Relaxed) {
        if reader.update() {
            let view = reader.view();
            assert_eq!(view.bids[0].qty, view.asks[0].qty);
            assert_eq!(view.received_at, Some(now));
        }
        thread::yield_now();
    }
    producer.join().unwrap();
    reader.update();
    assert_eq!(reader.view().bids[0].qty.raw(), 2000);
}
