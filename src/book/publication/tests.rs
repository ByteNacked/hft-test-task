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
    assert_eq!(reader.view().status(), BookStatus::Offline);
    assert_eq!(reader.view().exchange_time(), None);
    assert_eq!(reader.view().platform_time(), None);
    let mut book = OrderBook::new(rules(), 16).unwrap();
    book.load_snapshot(&mut snapshot(100, SCALE, 6)).unwrap();
    assert_eq!(
        writer.publish(&book, BookStatus::Online),
        Err(BookError::NotReady)
    );
}
#[test]
fn every_slot_retains_capacity_and_status_keeps_time() {
    let now = Instant::now();
    let book = book(now, 100, SCALE, 16);
    let (mut writer, mut reader) = book::channel(16).unwrap();
    let mut allocations = std::collections::BTreeSet::new();
    allocations.insert(reader.view().bids().as_ptr() as usize);
    for n in 1..=20 {
        let status = if n % 2 == 0 {
            BookStatus::Offline
        } else {
            BookStatus::Online
        };
        assert_eq!(writer.publish(&book, status).unwrap(), n);
        assert!(reader.update());
        assert_eq!(reader.view().allocated_capacities(), [16; 2]);
        allocations.insert(reader.view().bids().as_ptr() as usize);
        assert_eq!(reader.view().received_at(), Some(now));
        assert_eq!(reader.view().exchange_time(), Some(1_700_000_000_000));
        assert_eq!(reader.view().platform_time(), Some(1_700_000_000_010));
        assert_eq!(reader.view().status(), status);
    }
    assert_eq!(allocations.len(), 3);
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
    assert_eq!(held.sequence(), 1);
    assert_eq!(held.status(), BookStatus::Online);
    assert!(reader.update());
    assert_eq!(reader.view().sequence(), 3);
    assert_eq!(reader.view().status(), BookStatus::Online);
    assert_eq!(reader.skipped_publications(), 1);
    assert!(!reader.update());
    assert_eq!(reader.skipped_publications(), 1);
    writer.publish(&book, BookStatus::Online).unwrap();
    reader.update();
    assert_eq!(reader.skipped_publications(), 1);
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
            book.apply_diff(&mut update).unwrap();
            writer.publish(&book, BookStatus::Online).unwrap();
        }
        // This flag carries completion only. Book visibility remains triple_buffer's job.
        producer_done.store(true, Ordering::Relaxed);
    });
    while !done.load(Ordering::Relaxed) {
        if reader.update() {
            let view = reader.view();
            assert_eq!(view.bids()[0].qty.raw() as u64, view.sequence());
            assert_eq!(view.asks()[0].qty.raw() as u64, view.sequence());
            assert_eq!(view.received_at(), Some(now));
        }
        thread::yield_now();
    }
    producer.join().unwrap();
    reader.update();
    assert_eq!(reader.view().sequence(), 2000);
}
