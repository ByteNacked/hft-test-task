//! Single-writer, price-indexed book. Failed diffs restore only touched cells.
mod publication;
mod side;
pub use publication::{BookPublisher, BookReader, BookStatus, BookView, channel};
use side::Side;

use crate::{
    config::{BOOK_PRICE_SLOTS, DIFF_LEVELS, ROBOT_LEVELS, SNAPSHOT_LEVELS},
    md::{DepthDiff, EventTime, Snapshot},
    numeric::{InstrumentRules, NumericError, Price, Qty},
};
use arrayvec::ArrayVec;
use serde::{Deserialize, Deserializer};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Price,
    pub qty: Qty,
}
impl<'de> Deserialize<'de> for Level {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (price, qty) = <(Price, Qty)>::deserialize(d)?;
        Ok(Self { price, qty })
    }
}
pub type DiffSide = ArrayVec<Level, DIFF_LEVELS>;
pub type SnapshotSide = ArrayVec<Level, SNAPSHOT_LEVELS>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Coverage {
    bid_floor: Price,
    ask_ceiling: Price,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BookError {
    #[error("book publication epoch overflow")]
    PublicationEpochOverflow,
    #[error(transparent)]
    Numeric(#[from] NumericError),
    #[error("duplicate price in one payload side")]
    DuplicatePrice,
    #[error("book capacity exceeded")]
    Capacity,
    #[error("price {price} is outside book range (max {max_price})")]
    PriceRange { price: Price, max_price: Price },
    #[error("empty book side")]
    EmptySide,
    #[error("snapshot contains a zero quantity")]
    ZeroSnapshotQuantity,
    #[error("crossed or locked book")]
    Crossed,
    #[error("fewer than five trusted levels per side")]
    CoverageLost,
    #[error("snapshot required before applying diffs")]
    NoSnapshot,
    #[error("Online requires a covered book and an applied diff")]
    NotReady,
}

#[derive(Debug)]
pub struct OrderBook {
    bids: Side,
    asks: Side,
    tick_size: Price,
    max_price: Price,
    coverage: Option<Coverage>,
    event_time: Option<EventTime>,
}
impl OrderBook {
    pub fn new(tick_size: Price) -> Result<Self, BookError> {
        let max_price = Price::from_raw(
            tick_size
                .raw()
                .checked_mul((BOOK_PRICE_SLOTS - 1) as i64)
                .ok_or(NumericError::Overflow)?,
        )?;
        Ok(Self {
            bids: Side::new(),
            asks: Side::new(),
            tick_size,
            max_price,
            coverage: None,
            event_time: None,
        })
    }
    pub fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids.levels(self.tick_size, true)
    }
    pub fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks.levels(self.tick_size, false)
    }
    pub fn event_time(&self) -> Option<EventTime> {
        self.event_time
    }
    fn check_prices(&self, levels: &[Level]) -> Result<(), BookError> {
        for level in levels {
            level.price.check_step(self.tick_size)?;
            if level.price > self.max_price {
                return Err(BookError::PriceRange {
                    price: level.price,
                    max_price: self.max_price,
                });
            }
        }
        Ok(())
    }
    /// Snapshot levels are already sorted and validated by the decoder.
    pub(crate) fn load_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), BookError> {
        self.check_prices(&snapshot.bids)?;
        self.check_prices(&snapshot.asks)?;
        let coverage = Coverage {
            bid_floor: snapshot.bids.last().ok_or(BookError::EmptySide)?.price,
            ask_ceiling: snapshot.asks.last().ok_or(BookError::EmptySide)?.price,
        };
        validate_snapshot(&snapshot.bids, &snapshot.asks)?;
        self.bids.load(&snapshot.bids, self.tick_size);
        self.asks.load(&snapshot.asks, self.tick_size);
        self.coverage = Some(coverage);
        self.event_time = None;
        Ok(())
    }
    /// Normalized input has unique prices. No reader observes this mutable book.
    pub(crate) fn apply_diff(&mut self, diff: &DepthDiff) -> Result<(), BookError> {
        let coverage = self.coverage.ok_or(BookError::NoSnapshot)?;
        self.check_prices(&diff.bids)?;
        self.check_prices(&diff.asks)?;
        self.bids.apply(&diff.bids, self.tick_size);
        self.asks.apply(&diff.asks, self.tick_size);
        let result = self.validate(coverage);
        if result.is_err() {
            self.bids.rollback(&diff.bids, self.tick_size);
            self.asks.rollback(&diff.asks, self.tick_size);
        } else {
            self.event_time = Some(diff.time);
        }
        result
    }
    fn validate(&self, coverage: Coverage) -> Result<(), BookError> {
        let mut bids = self.bids();
        let mut asks = self.asks();
        let bid = bids.next().ok_or(BookError::EmptySide)?;
        let ask = asks.next().ok_or(BookError::EmptySide)?;
        if bid.price >= ask.price {
            return Err(BookError::Crossed);
        }
        // Only the fifth best on each side is needed to check trusted coverage.
        if bids
            .nth(ROBOT_LEVELS - 2)
            .is_none_or(|l| l.price < coverage.bid_floor)
            || asks
                .nth(ROBOT_LEVELS - 2)
                .is_none_or(|l| l.price > coverage.ask_ceiling)
        {
            return Err(BookError::CoverageLost);
        }
        Ok(())
    }
}

pub(crate) fn prepare(
    levels: &mut [Level],
    bids: bool,
    rules: InstrumentRules,
) -> Result<(), BookError> {
    for level in levels.iter() {
        rules.validate(level.price, level.qty)?;
    }
    levels.sort_unstable_by(|a, b| {
        if bids {
            b.price.cmp(&a.price)
        } else {
            a.price.cmp(&b.price)
        }
    });
    if levels.windows(2).any(|pair| pair[0].price == pair[1].price) {
        return Err(BookError::DuplicatePrice);
    }
    Ok(())
}
fn validate_snapshot(bids: &[Level], asks: &[Level]) -> Result<(), BookError> {
    let bid = bids.first().ok_or(BookError::EmptySide)?;
    let ask = asks.first().ok_or(BookError::EmptySide)?;
    if bid.price >= ask.price {
        return Err(BookError::Crossed);
    }
    if bids.len() < ROBOT_LEVELS || asks.len() < ROBOT_LEVELS {
        return Err(BookError::CoverageLost);
    }
    Ok(())
}
#[cfg(test)]
mod tests;
