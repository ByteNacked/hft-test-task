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
fn cached_top_tracks_boundaries_and_independent_sides() {
    let now = Instant::now();
    let mut book = OrderBook::new(price(1)).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 25)).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(&EPOCH);
    assert_eq!(reader.view().status, BookStatus::Offline);
    let mut publish = |book: &OrderBook| {
        writer.publish(book, BookStatus::Online).unwrap();
        assert!(reader.update());
        let view = reader.view();
        assert_eq!(
            view.bids.as_slice(),
            book.bids().take(20).collect::<Vec<_>>()
        );
        assert_eq!(
            view.asks.as_slice(),
            book.asks().take(20).collect::<Vec<_>>()
        );
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
    writer.publish(&book, BookStatus::Offline).unwrap();
    book.load_snapshot(&snapshot(100, SCALE, 6)).unwrap();
    assert_eq!(
        writer.publish(&book, BookStatus::Online),
        Err(BookError::NotReady)
    );
    book.apply_diff(&diff(now)).unwrap();
    writer.publish(&book, BookStatus::Online).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    assert_eq!(
        reader.view().bids.as_slice(),
        book.bids().collect::<Vec<_>>()
    );
    assert_eq!(
        reader.view().asks.as_slice(),
        book.asks().collect::<Vec<_>>()
    );
}

#[test]
fn gaps_invalidate_old_views_and_precede_latest_recovery() {
    let book = book(Instant::now(), 100, SCALE);
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (mut writer, mut reader) = book::channel(&EPOCH);
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
    // Held view stays stable; several recoveries between reads still deliver Offline first.
    let held = reader.view();
    for _ in 0..2 {
        writer.publish(&book, BookStatus::Offline).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
    }
    assert_eq!(held.status, BookStatus::Online);
    assert_eq!(held.bids.as_slice(), book.bids().collect::<Vec<_>>());
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Online);
    assert!(!reader.update());
    writer.publish(&book, BookStatus::Offline).unwrap();
    assert!(reader.update());
    assert_eq!(reader.view().status, BookStatus::Offline);
}
#[test]
fn concurrent_slow_reader_sees_complete_versions() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE);
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(&EPOCH)
    };
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
