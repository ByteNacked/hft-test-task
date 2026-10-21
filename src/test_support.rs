use crate::{
    book::{self, BookReader, BookStatus},
    book::{Level, OrderBook},
    md::{DepthDiff, EventTime, Snapshot},
    numeric::{InstrumentRules, Price, Qty},
};
use std::time::Instant;
pub fn price(raw: i64) -> Price {
    Price::from_raw(raw).unwrap()
}
pub fn qty(raw: i64) -> Qty {
    Qty::from_raw(raw).unwrap()
}
pub fn level(price_raw: i64, qty_raw: i64) -> Level {
    Level {
        price: price(price_raw),
        qty: qty(qty_raw),
    }
}
pub fn rules() -> InstrumentRules {
    InstrumentRules::new(price(1), qty(1)).unwrap()
}
pub fn time(now: Instant) -> EventTime {
    EventTime::new(1_700_000_000_000, now).unwrap()
}
pub fn snapshot(mid: i64, quantity: i64, count: usize) -> Snapshot {
    Snapshot {
        last_update_id: 100,
        bids: (1..=count)
            .map(|i| level(mid - i as i64, quantity))
            .collect(),
        asks: (1..=count)
            .map(|i| level(mid + i as i64, quantity))
            .collect(),
    }
}
pub fn diff(now: Instant) -> DepthDiff {
    DepthDiff {
        first_id: 100,
        final_id: 102,
        previous_id: 99,
        bids: Default::default(),
        asks: Default::default(),
        time: time(now),
    }
}
pub fn book(now: Instant, mid: i64, quantity: i64) -> OrderBook {
    let mut book = OrderBook::new(price(1)).unwrap();
    book.load_snapshot(&snapshot(mid, quantity, 6)).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    book
}
pub fn view(book: &OrderBook) -> BookReader {
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(crate::config::PUBLISHED_LEVELS, &EPOCH)
    }
    .unwrap();
    writer.publish(book, BookStatus::Online).unwrap();
    reader.update();
    reader
}
