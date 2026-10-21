use crate::test_support::*;
use crate::{
    book::OrderBook,
    book::{self, BookError, BookStatus},
    config::PUBLISHED_LEVELS,
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
fn cached_top_tracks_boundaries_and_independent_sides() {
    let now = Instant::now();
    let mut book = OrderBook::new(price(1)).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 25)).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(PUBLISHED_LEVELS, &EPOCH).unwrap();
    let mut publish = |book: &OrderBook| {
        writer.publish(book, BookStatus::Online).unwrap();
        assert!(reader.update());
        let view = reader.view();
        assert_eq!(view.bids, book.bids().take(20).collect::<Vec<_>>());
        assert_eq!(view.asks, book.asks().take(20).collect::<Vec<_>>());
        assert_eq!(
            view.platform_time,
            book.event_time().map(|t| t.platform_time())
        );
        assert_eq!(
            view.exchange_time,
            book.event_time().map(|t| t.exchange_time())
        );
    };
    // Cycle all triple-buffer slots: unchanged top levels still get new timestamps.
    for i in 1..=4 {
        let mut update = diff(now + std::time::Duration::from_millis(i));
        update.bids.push(indexed(79, i as i64 * SCALE));
        update.asks.push(indexed(121, i as i64 * SCALE));
        book.apply_diff(&update).unwrap();
        publish(&book);
    }
    // Change the quantity exactly at the cutoff, one side at a time.
    for is_bid in [true, false] {
        let mut update = diff(now);
        if is_bid {
            update.bids.push(indexed(80, 2 * SCALE));
        } else {
            update.asks.push(indexed(120, 3 * SCALE));
        }
        book.apply_diff(&update).unwrap();
        publish(&book);
    }
    // Removing the cutoff must promote a deeper level changed in the same diff.
    let mut update = diff(now);
    update.bids.extend([indexed(80, 0), indexed(79, 5 * SCALE)]);
    update
        .asks
        .extend([indexed(120, 0), indexed(121, 6 * SCALE)]);
    book.apply_diff(&update).unwrap();
    publish(&book);
    update.bids.clear();
    update.asks.clear();
    update.bids.push(indexed(100, SCALE)); // New best, asks untouched.
    book.apply_diff(&update).unwrap();
    publish(&book);
}

#[test]
fn publication_limits_depth_and_promotes_retained_levels_after_deletion() {
    let now = Instant::now();
    let mut book = OrderBook::new(price(1)).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 25)).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(PUBLISHED_LEVELS, &EPOCH).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().bids, book.bids().take(20).collect::<Vec<_>>());
    assert_eq!(reader.view().asks, book.asks().take(20).collect::<Vec<_>>());
    assert_eq!(book.bids().count(), 25);
    assert_eq!(book.asks().count(), 25);

    let later = now + std::time::Duration::from_millis(1);
    let mut update = diff(later);
    update.bids.push(indexed(99, 0));
    update.asks.push(indexed(101, 0));
    book.apply_diff(&update).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().bids, book.bids().take(20).collect::<Vec<_>>());
    assert_eq!(reader.view().asks, book.asks().take(20).collect::<Vec<_>>());
    assert_eq!(reader.view().bids[19].price, price(79));
    assert_eq!(reader.view().asks[19].price, price(121));
    assert_eq!(
        reader.view().exchange_time,
        Some(update.time.exchange_time())
    );
    assert_eq!(reader.view().platform_time, Some(later));

    // A shallower recovery must not retain stale levels from an earlier view.
    writer.publish(&book, BookStatus::Offline).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 6)).unwrap();
    book.apply_diff(&diff(later)).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    assert_eq!(reader.view().bids, book.bids().collect::<Vec<_>>());
    assert_eq!(reader.view().asks, book.asks().collect::<Vec<_>>());
}

#[test]
fn offline_start_and_online_requires_applied_diff() {
    let (mut writer, reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(16, &EPOCH)
    }
    .unwrap();
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert_eq!(reader.view().exchange_time, None);
    assert_eq!(reader.view().platform_time, None);
    let mut book = OrderBook::new(price(1)).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 6)).unwrap();
    assert_eq!(
        writer.publish(&book, BookStatus::Online),
        Err(BookError::NotReady)
    );
}
#[test]
fn held_snapshot_is_stable_and_offline_precedes_latest_recovery() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE);
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(16, &EPOCH)
    }
    .unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    reader.update();
    let held = reader.view();
    writer.publish(&book, BookStatus::Offline).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert_eq!(held.bids.as_slice(), book.bids().collect::<Vec<_>>());
    assert_eq!(held.status, BookStatus::Online);
    assert!(reader.update());

    assert_eq!(reader.view().status, BookStatus::Offline);
    // A newer book published while Hold is handled wins over the held recovery.
    let mut update = diff(now);
    update.bids.push(indexed(99, 123));
    book.apply_diff(&update).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    assert_eq!(reader.view().bids[0].qty.raw(), 123);
}
#[test]
fn slow_reader_gets_latest_version_without_blocking_publication() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE);
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(16, &EPOCH).unwrap();
    for n in 1..=100 {
        let mut update = diff(now);
        update.bids.push(indexed(99, n));
        book.apply_diff(&update).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
    }
    assert!(reader.update());
    assert_eq!(reader.view().bids[0].qty.raw(), 100);
    assert!(!reader.update());
}
#[test]
fn offline_invalidates_unread_online_until_a_recovered_book_arrives() {
    let book = book(Instant::now(), 100, SCALE);
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(16, &EPOCH).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    writer.publish(&book, BookStatus::Online).unwrap();
    writer.publish(&book, BookStatus::Offline).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    // The unread pre-gap book must not become Online on a later pass.
    assert!(!reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    // A gap is delivered even when no book has been published since the last read.
    writer.publish(&book, BookStatus::Offline).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
}
#[test]
fn multiple_recoveries_cannot_revive_an_invalidated_version() {
    let book = book(Instant::now(), 100, SCALE);
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(16, &EPOCH)
    }
    .unwrap();
    writer.publish(&book, BookStatus::Offline).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    writer.publish(&book, BookStatus::Offline).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    assert!(!reader.update());
}
#[test]
fn concurrent_slow_reader_sees_complete_versions() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE);
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(16, &EPOCH)
    }
    .unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let producer_done = done.clone();
    let producer = thread::spawn(move || {
        for n in 1..=2000 {
            let mut update = diff(now);
            update.bids.push(indexed(99, n));
            update.asks.push(indexed(101, n));
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
            assert_eq!(view.platform_time, Some(now));
        }
        thread::yield_now();
    }
    producer.join().unwrap();
    while reader.update() {}
    assert_eq!(reader.view().bids[0].qty.raw(), 2000);
}
