use crate::test_support::*;
use crate::{
    book::{BookError, OrderBook},
    numeric::SCALE,
};
use proptest::prelude::*;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[test]
fn transactional_merge_preserves_distant_levels_and_absolute_quantities() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE, 32);
    let mut change = diff(now);
    change
        .bids
        .extend([level(99, 2 * SCALE), level(10, SCALE), level(20, 0)]);
    change.asks.extend([level(101, 0), level(200, SCALE)]);
    book.apply_diff(&mut change).unwrap();
    assert_eq!(book.bids()[0], level(99, 2 * SCALE));
    assert_eq!(book.bids().last().unwrap().price.raw(), 10);
    assert_eq!(book.asks()[0].price.raw(), 102);
    assert_eq!(book.asks().last().unwrap().price.raw(), 200);
    let mut next = diff(now);
    next.bids.push(level(99, SCALE));
    book.apply_diff(&mut next).unwrap();
    assert_eq!(book.bids()[0].qty.raw(), SCALE);
    assert_eq!(book.bids().last().unwrap().price.raw(), 10);
    assert_eq!(book.allocated_capacities(), [32; 4]);
}
#[test]
fn errors_do_not_commit_either_side_or_timestamp() {
    let now = Instant::now();
    let mut book = book(now, 100, SCALE, 6);
    let original = (
        book.bids().to_vec(),
        book.asks().to_vec(),
        book.coverage(),
        book.event_time(),
    );
    let mut change = diff(now + Duration::from_secs(1));
    change.bids.push(level(99, 2 * SCALE));
    change.asks.push(level(110, SCALE));
    assert_eq!(book.apply_diff(&mut change), Err(BookError::Capacity));
    assert_eq!(
        (book.bids(), book.asks(), book.coverage(), book.event_time()),
        (&original.0[..], &original.1[..], original.2, original.3)
    );
    change.asks.clear();
    change.asks.push(level(98, SCALE));
    change.asks.push(level(106, 0));
    assert_eq!(book.apply_diff(&mut change), Err(BookError::Crossed));
    assert_eq!(book.bids(), original.0);
    change.asks.clear();
    change.asks.extend([level(101, 0), level(102, 0)]);
    assert_eq!(book.apply_diff(&mut change), Err(BookError::CoverageLost));
    assert_eq!(book.asks(), original.1);
    change.asks.clear();
    change.asks.extend((101..=106).map(|p| level(p, 0)));
    assert_eq!(book.apply_diff(&mut change), Err(BookError::EmptySide));
    assert_eq!(book.asks(), original.1);
    change.asks.clear();
    change.bids.extend([level(98, SCALE), level(98, 0)]);
    assert_eq!(book.apply_diff(&mut change), Err(BookError::DuplicatePrice));
    assert_eq!(book.bids(), original.0);
    assert_eq!(book.event_time(), original.3);
}
#[test]
fn snapshot_validation_and_new_epoch_clears_timestamps() {
    let now = Instant::now();
    let mut book = OrderBook::new(rules(), 8).unwrap();
    assert_eq!(book.apply_diff(&mut diff(now)), Err(BookError::NoSnapshot));
    assert_eq!(
        book.load_snapshot(&mut snapshot(100, SCALE, 4)),
        Err(BookError::CoverageLost)
    );
    assert_eq!(
        book.load_snapshot(&mut snapshot(100, SCALE, 9)),
        Err(BookError::Capacity)
    );
    let mut snap = snapshot(100, SCALE, 6);
    snap.bids[0].qty = qty(0);
    assert_eq!(
        book.load_snapshot(&mut snap),
        Err(BookError::ZeroSnapshotQuantity)
    );
    book.load_snapshot(&mut snapshot(100, SCALE, 6)).unwrap();
    book.apply_diff(&mut diff(now)).unwrap();
    let original = book.bids().to_vec();
    let mut invalid = snapshot(200, SCALE, 6);
    invalid.asks[0] = invalid.asks[1];
    assert_eq!(
        book.load_snapshot(&mut invalid),
        Err(BookError::DuplicatePrice)
    );
    assert_eq!(book.bids(), original);
    book.load_snapshot(&mut snapshot(200, SCALE, 6)).unwrap();
    assert_eq!(book.event_time(), None);
}
proptest! {
    #[test]
    fn merge_matches_btree(operations in prop::collection::vec((any::<bool>(), 0_i64..50, 0_i64..5), 1..250)) {
        let now = Instant::now(); let mut book = book(now,100,SCALE,32);
        let to_map = |levels: &[crate::book::Level]| levels.iter().map(|l| (l.price.raw(), l.qty.raw())).collect::<BTreeMap<_,_>>();
        let (mut bids,mut asks) = (to_map(book.bids()), to_map(book.asks()));
        for (is_bid, offset, units) in operations {
            let price = if is_bid { 99-offset } else { 101+offset };
            let mut new_bids=bids.clone(); let mut new_asks=asks.clone();
            let side=if is_bid { &mut new_bids } else { &mut new_asks };
            if units==0 { side.remove(&price); } else { side.insert(price,units*SCALE); }
            let valid=new_bids.len()<=32 && new_asks.len()<=32 && new_bids.range(94..).count()>=5 && new_asks.range(..=106).count()>=5;
            let mut change=diff(now); if is_bid { change.bids.push(level(price,units*SCALE)); } else { change.asks.push(level(price,units*SCALE)); }
            prop_assert_eq!(book.apply_diff(&mut change).is_ok(), valid);
            if valid { bids=new_bids; asks=new_asks; }
            prop_assert_eq!(to_map(book.bids()), bids.clone()); prop_assert_eq!(to_map(book.asks()), asks.clone());
            prop_assert!(book.bids().windows(2).all(|p| p[0].price>p[1].price));
            prop_assert!(book.asks().windows(2).all(|p| p[0].price<p[1].price));
            prop_assert_eq!(book.allocated_capacities(),[32;4]);
        }
    }
}
