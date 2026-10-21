//! Single-writer book. Scratch is committed only after both sides pass validation.
mod publication;
pub use publication::{BookPublisher, BookReader, BookStatus, BookView, channel};

use crate::{
    config::{BOOK_LEVELS, DIFF_LEVELS, ROBOT_LEVELS, SNAPSHOT_LEVELS},
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
pub struct Coverage {
    pub bid_floor: Price,
    pub ask_ceiling: Price,
}
impl Coverage {
    pub fn counts(self, bids: &[Level], asks: &[Level]) -> (usize, usize) {
        (
            bids.partition_point(|l| l.price >= self.bid_floor),
            asks.partition_point(|l| l.price <= self.ask_ceiling),
        )
    }
    pub fn is_sufficient(self, bids: &[Level], asks: &[Level]) -> bool {
        let (bids, asks) = self.counts(bids, asks);
        bids >= ROBOT_LEVELS && asks >= ROBOT_LEVELS
    }
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
    bids: Vec<Level>,
    asks: Vec<Level>,
    scratch_bids: Vec<Level>,
    scratch_asks: Vec<Level>,
    capacity: usize,
    coverage: Option<Coverage>,
    event_time: Option<EventTime>,
}
impl OrderBook {
    pub fn new(capacity: usize) -> Result<Self, BookError> {
        if !(ROBOT_LEVELS..=BOOK_LEVELS).contains(&capacity) {
            return Err(BookError::Capacity);
        }
        Ok(Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
            scratch_bids: Vec::with_capacity(capacity),
            scratch_asks: Vec::with_capacity(capacity),
            capacity,
            coverage: None,
            event_time: None,
        })
    }
    pub fn bids(&self) -> &[Level] {
        &self.bids
    }
    pub fn asks(&self) -> &[Level] {
        &self.asks
    }
    pub fn coverage(&self) -> Option<Coverage> {
        self.coverage
    }
    pub fn event_time(&self) -> Option<EventTime> {
        self.event_time
    }

    /// Snapshot levels are already sorted and validated by the decoder.
    pub(crate) fn load_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), BookError> {
        if snapshot.bids.len() > self.capacity || snapshot.asks.len() > self.capacity {
            return Err(BookError::Capacity);
        }
        let coverage = Coverage {
            bid_floor: snapshot.bids.last().ok_or(BookError::EmptySide)?.price,
            ask_ceiling: snapshot.asks.last().ok_or(BookError::EmptySide)?.price,
        };
        validate_book(&snapshot.bids, &snapshot.asks, coverage)?;
        self.scratch_bids.clear();
        self.scratch_asks.clear();
        self.scratch_bids.extend_from_slice(&snapshot.bids);
        self.scratch_asks.extend_from_slice(&snapshot.asks);
        self.commit();
        self.coverage = Some(coverage);
        self.event_time = None;
        Ok(())
    }
    /// Input is normalized by the decoder; SyncedBook checks exchange continuity.
    pub(crate) fn apply_diff(&mut self, diff: &DepthDiff) -> Result<(), BookError> {
        let coverage = self.coverage.ok_or(BookError::NoSnapshot)?;
        merge(
            &self.bids,
            &diff.bids,
            &mut self.scratch_bids,
            true,
            self.capacity,
        )?;
        merge(
            &self.asks,
            &diff.asks,
            &mut self.scratch_asks,
            false,
            self.capacity,
        )?;
        validate_book(&self.scratch_bids, &self.scratch_asks, coverage)?;
        self.commit();
        self.event_time = Some(diff.time);
        Ok(())
    }
    fn commit(&mut self) {
        std::mem::swap(&mut self.bids, &mut self.scratch_bids);
        std::mem::swap(&mut self.asks, &mut self.scratch_asks);
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
fn validate_book(bids: &[Level], asks: &[Level], coverage: Coverage) -> Result<(), BookError> {
    let bid = bids.first().ok_or(BookError::EmptySide)?;
    let ask = asks.first().ok_or(BookError::EmptySide)?;
    if bid.price >= ask.price {
        return Err(BookError::Crossed);
    }
    if !coverage.is_sufficient(bids, asks) {
        return Err(BookError::CoverageLost);
    }
    Ok(())
}
fn merge(
    old: &[Level],
    changes: &[Level],
    out: &mut Vec<Level>,
    bids: bool,
    capacity: usize,
) -> Result<(), BookError> {
    out.clear();
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < changes.len() {
        let level = match (old.get(i), changes.get(j)) {
            (Some(a), Some(b)) if a.price == b.price => {
                i += 1;
                j += 1;
                *b
            }
            (Some(a), Some(b)) if (bids && a.price > b.price) || (!bids && a.price < b.price) => {
                i += 1;
                *a
            }
            (_, Some(b)) => {
                j += 1;
                *b
            }
            (Some(a), None) => {
                i += 1;
                *a
            }
            (None, None) => break,
        };
        if level.qty != Qty::ZERO {
            if out.len() == capacity {
                return Err(BookError::Capacity);
            }
            out.push(level);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
