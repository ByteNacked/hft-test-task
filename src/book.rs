//! Single-writer, price-indexed book. Failed updates require a new snapshot.
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
/// A diff level checked against its feed's instrument rules and price-slot limit.
/// Keeps the same 16-byte footprint as Level; the price is reconstructed only for readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedLevel {
    index: usize,
    pub qty: Qty,
}
impl IndexedLevel {
    pub(crate) fn new(
        level: Level,
        rules: InstrumentRules,
        window: PriceWindow,
    ) -> Result<Self, BookError> {
        debug_assert_eq!(rules.tick_size(), window.tick);
        let index = window.index(level.price)?;
        rules.validate_qty(level.qty)?;
        Ok(Self {
            index,
            qty: level.qty,
        })
    }
    pub(crate) fn index(self) -> usize {
        self.index
    }
}
/// Fixed price window shared by decoding and both sides until the next snapshot.
#[derive(Debug, Clone, Copy)]
pub struct PriceWindow {
    tick: Price,
    base_ticks: i64,
}
impl PriceWindow {
    pub(crate) fn new(tick: Price) -> Result<Self, BookError> {
        tick.raw()
            .checked_mul((BOOK_PRICE_SLOTS - 1) as i64)
            .ok_or(NumericError::Overflow)?;
        Ok(Self {
            tick,
            base_ticks: 0,
        })
    }
    fn centered(self, snapshot: &Snapshot) -> Self {
        // Snapshot sides are nonempty and spread is checked before this call.
        let bid = snapshot.bids[0].price.raw() / self.tick.raw();
        let ask = snapshot.asks[0].price.raw() / self.tick.raw();
        let mid = bid + (ask - bid) / 2;
        let max_base = i64::MAX / self.tick.raw() - (BOOK_PRICE_SLOTS - 1) as i64;
        Self {
            base_ticks: (mid - (BOOK_PRICE_SLOTS / 2) as i64).clamp(0, max_base),
            ..self
        }
    }
    fn index(self, price: Price) -> Result<usize, BookError> {
        let ticks = price.raw() / self.tick.raw();
        if ticks * self.tick.raw() != price.raw() {
            return Err(NumericError::InvalidStep.into());
        }
        let offset = ticks - self.base_ticks;
        if !(0..BOOK_PRICE_SLOTS as i64).contains(&offset) {
            return Err(BookError::PriceRange {
                price,
                min_price: Price::from_raw(self.base_ticks.max(1) * self.tick.raw())?,
                max_price: self.price(BOOK_PRICE_SLOTS - 1),
            });
        }
        Ok(offset as usize)
    }
    fn price(self, index: usize) -> Price {
        // Window bounds fit i64; occupied slots originate from checked positive prices.
        Price::from_raw((self.base_ticks + index as i64) * self.tick.raw()).unwrap()
    }
}
pub type DiffSide = ArrayVec<IndexedLevel, DIFF_LEVELS>;
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
    #[error("price {price} is outside book range [{min_price}, {max_price}]")]
    PriceRange {
        price: Price,
        min_price: Price,
        max_price: Price,
    },
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
    window: PriceWindow,
    coverage: Option<Coverage>,
    event_time: Option<EventTime>,
}
impl OrderBook {
    pub(crate) fn new(tick_size: Price) -> Result<Self, BookError> {
        let window = PriceWindow::new(tick_size)?;
        Ok(Self {
            bids: Side::new(true),
            asks: Side::new(false),
            window,
            coverage: None,
            event_time: None,
        })
    }
    pub(crate) fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids.levels(self.window)
    }
    pub(crate) fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks.levels(self.window)
    }
    pub(crate) fn event_time(&self) -> Option<EventTime> {
        self.event_time
    }
    pub(crate) fn window(&self) -> PriceWindow {
        self.window
    }
    /// Snapshot levels are already sorted and validated by the decoder.
    pub(crate) fn load_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), BookError> {
        let coverage = Coverage {
            bid_floor: snapshot.bids.last().ok_or(BookError::EmptySide)?.price,
            ask_ceiling: snapshot.asks.last().ok_or(BookError::EmptySide)?.price,
        };
        validate_snapshot(&snapshot.bids, &snapshot.asks)?;
        let window = self.window.centered(snapshot);
        for level in snapshot.bids.iter().chain(&snapshot.asks) {
            window.index(level.price)?;
        }
        self.bids.load(&snapshot.bids, window);
        self.asks.load(&snapshot.asks, window);
        self.window = window;
        self.coverage = Some(coverage);
        self.event_time = None;
        Ok(())
    }
    /// Input uses this book's instrument rules/current window and has unique indices.
    /// No reader observes this mutable book. A failed update invalidates it until reload.
    pub(crate) fn apply_diff(&mut self, diff: &DepthDiff) -> Result<(), BookError> {
        let coverage = self.coverage.ok_or(BookError::NoSnapshot)?;
        self.bids.apply(&diff.bids);
        self.asks.apply(&diff.asks);
        if let Err(error) = self.validate(coverage) {
            // Do not undo writes: the adapter publishes Offline and reloads a snapshot.
            // Reject further updates and Online publication of this damaged book.
            self.coverage = None;
            self.event_time = None;
            return Err(error);
        }
        self.bids.refresh_top(self.window);
        self.asks.refresh_top(self.window);
        self.event_time = Some(diff.time);
        Ok(())
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
