use crate::test_support::*;
use crate::{
    book::{BookError, IndexedLevel, OrderBook},
    config::BOOK_PRICE_SLOTS,
    numeric::{InstrumentRules, SCALE},
};
use proptest::prelude::*;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[test]
fn updates_preserve_distant_levels_and_absolute_quantities() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE);
    let mut change = diff(now);
    change
        .bids
        .extend([indexed(99, 2 * SCALE), indexed(10, SCALE), indexed(20, 0)]);
    change.asks.extend([indexed(101, 0), indexed(200, SCALE)]);
    book.apply_diff(&change).unwrap();
    assert_eq!(book.bids().next().unwrap(), level(99, 2 * SCALE));
    assert_eq!(book.bids().last().unwrap().price.raw(), 10);
    assert_eq!(book.asks().next().unwrap().price.raw(), 102);
    assert_eq!(book.asks().last().unwrap().price.raw(), 200);
    let mut next = diff(now);
    next.bids.push(indexed(99, SCALE));
    book.apply_diff(&next).unwrap();
    assert_eq!(book.bids().next().unwrap().qty.raw(), SCALE);
    assert_eq!(book.bids().last().unwrap().price.raw(), 10);
}
#[test]
fn invalid_updates_require_snapshot_and_cannot_publish_online() {
    use crate::book::{BookStatus, channel};
    let now = Instant::now();
    for error in [
        BookError::Crossed,
        BookError::CoverageLost,
        BookError::EmptySide,
    ] {
        let mut book = book(now, 100, SCALE);
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let (mut writer, mut reader) = channel(20, &EPOCH).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
        reader.update();
        let held_bids = reader.view().bids.clone();
        let held_asks = reader.view().asks.clone();
        let mut change = diff(now + Duration::from_secs(1));
        change.bids.push(indexed(99, 2 * SCALE));
        match error {
            BookError::Crossed => change.asks.push(indexed(99, SCALE)),
            BookError::CoverageLost => change.asks.extend([indexed(101, 0), indexed(102, 0)]),
            BookError::EmptySide => change.asks.extend((101..=106).map(|p| indexed(p, 0))),
            _ => unreachable!(),
        }
        assert_eq!(book.apply_diff(&change), Err(error));
        assert_eq!(book.event_time(), None);
        assert_eq!(book.apply_diff(&diff(now)), Err(BookError::NoSnapshot));
        assert_eq!(
            writer.publish(&book, BookStatus::Online),
            Err(BookError::NotReady)
        );
        assert!(!reader.update());
        assert_eq!(reader.view().bids, held_bids);
        assert_eq!(reader.view().asks, held_asks);
        writer.publish(&book, BookStatus::Offline).unwrap();
        reader.update();
        assert_eq!(reader.view().status, BookStatus::Offline);
        assert_eq!(
            book.load_snapshot(&snapshot(200, SCALE, 4)),
            Err(BookError::CoverageLost)
        );
        assert_eq!(book.apply_diff(&diff(now)), Err(BookError::NoSnapshot));
        book.load_snapshot(&snapshot(200, SCALE, 6)).unwrap();
        assert_eq!(
            writer.publish(&book, BookStatus::Online),
            Err(BookError::NotReady)
        );
        book.apply_diff(&diff(now)).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
        reader.update();
        assert_eq!(reader.view().status, BookStatus::Online);
        assert_eq!(reader.view().bids, book.bids().collect::<Vec<_>>());
        assert_eq!(reader.view().asks, book.asks().collect::<Vec<_>>());
    }
}
#[test]
fn snapshot_validation_and_new_epoch_clears_timestamps() {
    let now = Instant::now();
    let mut book = OrderBook::new(price(1)).unwrap();
    assert_eq!(book.apply_diff(&diff(now)), Err(BookError::NoSnapshot));
    assert_eq!(
        book.load_snapshot(&snapshot(100, SCALE, 4)),
        Err(BookError::CoverageLost)
    );
    assert!(matches!(
        book.load_snapshot(&snapshot(BOOK_PRICE_SLOTS as i64 - 3, SCALE, 6)),
        Err(BookError::PriceRange { .. })
    ));
    book.load_snapshot(&snapshot(100, SCALE, 6)).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    book.load_snapshot(&snapshot(200, SCALE, 6)).unwrap();
    assert_eq!(book.event_time(), None);
}
proptest! {
    #[test]
    fn updates_match_btree(operations in prop::collection::vec((any::<bool>(), 0_i64..50, 0_i64..5), 1..250)) {
        let now = Instant::now(); let mut book = book(now,100,SCALE);
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let (mut writer, mut reader) = crate::book::channel(crate::config::PUBLISHED_LEVELS, &EPOCH).unwrap();
        let to_map = |levels: Vec<crate::book::Level>| levels.iter().map(|l| (l.price.raw(), l.qty.raw())).collect::<BTreeMap<_,_>>();
        let (mut bids,mut asks) = (to_map(book.bids().collect()), to_map(book.asks().collect()));
        for (is_bid, offset, units) in operations {
            let price = if is_bid { 99-offset } else { 101+offset };
            let mut new_bids=bids.clone(); let mut new_asks=asks.clone();
            let side=if is_bid { &mut new_bids } else { &mut new_asks };
            if units==0 { side.remove(&price); } else { side.insert(price,units*SCALE); }
            let valid=new_bids.range(94..).count()>=5 && new_asks.range(..=106).count()>=5;
            let mut change=diff(now); if is_bid { change.bids.push(indexed(price,units*SCALE)); } else { change.asks.push(indexed(price,units*SCALE)); }
            prop_assert_eq!(book.apply_diff(&change).is_ok(), valid);
            if valid { bids=new_bids; asks=new_asks; }
            else {
                prop_assert_eq!(book.apply_diff(&diff(now)), Err(BookError::NoSnapshot));
                prop_assert_eq!(writer.publish(&book, crate::book::BookStatus::Online), Err(BookError::NotReady));
                book.load_snapshot(&snapshot(100,SCALE,6)).unwrap();
                book.apply_diff(&diff(now)).unwrap();
                bids=to_map(book.bids().collect()); asks=to_map(book.asks().collect());
            }
            prop_assert_eq!(to_map(book.bids().collect()), bids.clone()); prop_assert_eq!(to_map(book.asks().collect()), asks.clone());
            prop_assert!(book.bids().collect::<Vec<_>>().windows(2).all(|p| p[0].price>p[1].price));
            prop_assert!(book.asks().collect::<Vec<_>>().windows(2).all(|p| p[0].price<p[1].price));
            writer.publish(&book, crate::book::BookStatus::Online).unwrap();
            reader.update();
            prop_assert_eq!(&reader.view().bids, &bids.iter().rev().take(20).map(|(&p,&q)|level(p,q)).collect::<Vec<_>>());
            prop_assert_eq!(&reader.view().asks, &asks.iter().take(20).map(|(&p,&q)|level(p,q)).collect::<Vec<_>>());
        }
    }
}

#[test]
fn sparse_bitmap_boundaries_and_snapshot_replacement() {
    let now = Instant::now();
    let tick = price(10);
    let instrument = InstrumentRules::new(tick, qty(1)).unwrap();
    let indexed = |p, q| IndexedLevel::new(level(p, q), instrument).unwrap();
    let mut book = OrderBook::new(tick).unwrap();
    let mut initial = snapshot(500_000, SCALE, 6);
    for l in initial.bids.iter_mut().chain(&mut initial.asks) {
        l.price = price(l.price.raw() * tick.raw());
    }
    book.load_snapshot(&initial).unwrap();
    let indices = [1, 63, 64, 65, 4095, 4096, 4097];
    let max = (BOOK_PRICE_SLOTS - 1) as i64 * tick.raw();
    let mut change = diff(now);
    change
        .bids
        .extend(indices.iter().rev().map(|i| indexed(i * tick.raw(), SCALE)));
    change.asks.push(indexed(max, SCALE));
    book.apply_diff(&change).unwrap();
    let original_bids = book.bids().collect::<Vec<_>>();
    let original_asks = book.asks().collect::<Vec<_>>();
    assert_eq!(
        original_bids[6..],
        indices
            .iter()
            .rev()
            .map(|i| level(i * tick.raw(), SCALE))
            .collect::<Vec<_>>()
    );
    assert_eq!(original_asks.last().unwrap().price, price(max));

    // Delete levels across bitmap word/group boundaries.
    for l in &mut change.bids {
        l.qty = qty(0);
    }
    // Out-of-range deletions and non-tick prices must also be rejected.
    for (raw, error) in [
        (
            max + tick.raw(),
            BookError::PriceRange {
                price: price(max + tick.raw()),
                max_price: price(max),
            },
        ),
        (
            max - 1,
            BookError::Numeric(crate::numeric::NumericError::InvalidStep),
        ),
    ] {
        assert_eq!(IndexedLevel::new(level(raw, 0), instrument), Err(error));
    }
    change.asks.clear();
    change.asks.push(indexed(max, 0));
    book.apply_diff(&change).unwrap();
    assert_eq!(book.bids().collect::<Vec<_>>(), initial.bids.as_slice());
    assert_eq!(book.asks().collect::<Vec<_>>(), initial.asks.as_slice());
    for l in &mut change.bids {
        l.qty = qty(2 * SCALE);
    }
    change.asks[0].qty = qty(SCALE);
    book.apply_diff(&change).unwrap();
    let before = (
        book.bids().collect::<Vec<_>>(),
        book.asks().collect::<Vec<_>>(),
        book.event_time(),
    );
    let mut invalid = crate::md::Snapshot {
        last_update_id: initial.last_update_id,
        bids: initial.bids.clone(),
        asks: initial.asks.clone(),
    };
    invalid.asks.last_mut().unwrap().price = price(max + tick.raw());
    assert!(matches!(
        book.load_snapshot(&invalid),
        Err(BookError::PriceRange { .. })
    ));
    assert_eq!(
        (
            book.bids().collect::<Vec<_>>(),
            book.asks().collect::<Vec<_>>(),
            book.event_time()
        ),
        before
    );
    book.load_snapshot(&initial).unwrap();
    assert_eq!(book.bids().collect::<Vec<_>>(), initial.bids.as_slice());
    assert_eq!(book.asks().collect::<Vec<_>>(), initial.asks.as_slice());
    assert_eq!(book.event_time(), None);
}

#[test]
fn best_groups_follow_insert_delete_and_snapshot() {
    let now = Instant::now();
    let mut book = OrderBook::new(price(1)).unwrap();
    let initial = crate::md::Snapshot {
        last_update_id: 100,
        bids: [8192, 4096, 4095, 4094, 4093, 64, 63, 1]
            .into_iter()
            .map(|p| level(p, SCALE))
            .collect(),
        asks: [
            16383,
            16384,
            16385,
            20480,
            24576,
            32768,
            65536,
            BOOK_PRICE_SLOTS as i64 - 1,
        ]
        .into_iter()
        .map(|p| level(p, SCALE))
        .collect(),
    };
    book.load_snapshot(&initial).unwrap();
    let mut change = diff(now);
    change.bids.push(indexed(12288, 2 * SCALE));
    change.asks.push(indexed(12289, 2 * SCALE));
    book.apply_diff(&change).unwrap();
    assert_eq!(
        book.bids().next().unwrap(),
        level(12288, change.bids[0].qty.raw())
    );
    assert_eq!(
        book.asks().next().unwrap(),
        level(12289, change.asks[0].qty.raw())
    );

    change.bids[0].qty = qty(3 * SCALE);
    change.asks[0].qty = qty(3 * SCALE);
    book.apply_diff(&change).unwrap();
    assert_eq!(
        book.bids().next().unwrap(),
        level(12288, change.bids[0].qty.raw())
    );
    assert_eq!(
        book.asks().next().unwrap(),
        level(12289, change.asks[0].qty.raw())
    );

    // Empty the best groups, including the original snapshot's best prices.
    change.bids.clear();
    change.asks.clear();
    change.bids.extend([indexed(12288, 0), indexed(8192, 0)]);
    change.asks.extend([indexed(12289, 0), indexed(16383, 0)]);
    book.apply_diff(&change).unwrap();
    assert_eq!(book.bids().collect::<Vec<_>>(), initial.bids[1..]);
    assert_eq!(book.asks().collect::<Vec<_>>(), initial.asks[1..]);

    // A new snapshot starts in completely different bitmap groups.
    let replacement = snapshot(500_000, SCALE, 6);
    book.load_snapshot(&replacement).unwrap();
    book.apply_diff(&diff(now)).unwrap();
    assert_eq!(book.bids().collect::<Vec<_>>(), replacement.bids.as_slice());
    assert_eq!(book.asks().collect::<Vec<_>>(), replacement.asks.as_slice());
}
