//! Market-data adapter contract and transport-independent normalized data.
mod adapter;
mod diff;
mod rest;
pub(crate) use diff::decode_into as decode_diff_into;
pub(crate) mod feed;
pub(crate) mod sync;
use crate::{
    book::{BookError, DiffSide, SnapshotSide, prepare},
    config,
    numeric::{InstrumentRules, Qty},
};
pub use adapter::{AdapterError, MDAdapter};
pub(crate) use adapter::{Backoff, get_body, shutdown_requested};
use std::time::Instant;
use thiserror::Error;

pub const MAX_UNIX_MS: u64 = 253_402_300_799_999; // Last millisecond of year 9999.
/// Monotonic timestamp captured on receipt of a complete WS message, before parsing.
pub type ReceiveTime = Instant;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTime {
    exchange_time: u64,
    platform_time: ReceiveTime,
}
impl EventTime {
    pub fn new(exchange_time: u64, receive: ReceiveTime) -> Result<Self, ProtocolError> {
        validate_timestamp(exchange_time)?;
        Ok(Self {
            exchange_time,
            platform_time: receive,
        })
    }
    pub fn exchange_time(self) -> u64 {
        self.exchange_time
    }
    pub fn platform_time(self) -> Instant {
        self.platform_time
    }
}
pub(crate) fn validate_timestamp(value: u64) -> Result<(), ProtocolError> {
    if value > MAX_UNIX_MS {
        Err(ProtocolError::Timestamp)
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DepthDiff {
    pub first_id: u64,
    pub final_id: u64,
    pub previous_id: u64,
    pub bids: DiffSide,
    pub asks: DiffSide,
    pub time: EventTime,
}
impl Default for DepthDiff {
    /// Empty workspace; decode successfully before applying it to a book.
    fn default() -> Self {
        Self {
            first_id: 0,
            final_id: 0,
            previous_id: 0,
            bids: Default::default(),
            asks: Default::default(),
            time: EventTime {
                exchange_time: 0,
                platform_time: Instant::now(),
            },
        }
    }
}
#[derive(Debug)]
pub struct Snapshot {
    pub last_update_id: u64,
    pub bids: SnapshotSide,
    pub asks: SnapshotSide,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("body exceeds configured limit")]
    BodyTooLarge,
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unexpected symbol, stream or event")]
    WrongStream,
    #[error("invalid update ID range")]
    UpdateIds,
    #[error("Unix milliseconds outside supported range")]
    Timestamp,
    #[error("missing, duplicated or incompatible ETHUSDT perpetual metadata")]
    Metadata,
    #[error(transparent)]
    Book(#[from] BookError),
    #[error(transparent)]
    Numeric(#[from] crate::numeric::NumericError),
}
pub(crate) fn check_body(body: &[u8], limit: usize) -> Result<(), ProtocolError> {
    if body.len() > limit {
        Err(ProtocolError::BodyTooLarge)
    } else {
        Ok(())
    }
}
/// Reusable duplicate-price bitmap, owned by one adapter; allocated only at startup.
pub struct DiffScratch {
    seen: Box<[u64]>,
}
impl Default for DiffScratch {
    fn default() -> Self {
        Self {
            seen: vec![0; config::BOOK_PRICE_SLOTS.div_ceil(64)].into_boxed_slice(),
        }
    }
}
impl DiffScratch {
    fn validate(&mut self, diff: &DepthDiff) -> Result<(), ProtocolError> {
        if diff.first_id > diff.final_id || diff.previous_id >= diff.final_id {
            return Err(ProtocolError::UpdateIds);
        }
        for (side, descending) in [(&diff.bids, true), (&diff.asks, false)] {
            // Strict wire order already proves uniqueness (also covers 0/1 levels).
            if side.windows(2).all(|pair| {
                if descending {
                    pair[0].index() > pair[1].index()
                } else {
                    pair[0].index() < pair[1].index()
                }
            }) {
                continue;
            }
            let duplicate = side.iter().any(|level| {
                let index = level.index();
                let word = &mut self.seen[index / 64];
                let bit = 1 << (index % 64);
                let duplicate = *word & bit != 0;
                *word |= bit;
                duplicate
            });
            // All indices were checked during decoding. Clear touched words even on
            // error; both sides and subsequent messages reuse the same clean bitmap.
            for level in side {
                self.seen[level.index() / 64] = 0;
            }
            if duplicate {
                return Err(BookError::DuplicatePrice.into());
            }
        }
        Ok(())
    }
}
pub(crate) fn normalize_snapshot(
    mut snapshot: Snapshot,
    rules: InstrumentRules,
) -> Result<Snapshot, ProtocolError> {
    prepare(&mut snapshot.bids, true, rules)?;
    prepare(&mut snapshot.asks, false, rules)?;
    if snapshot
        .bids
        .iter()
        .chain(snapshot.asks.iter())
        .any(|l| l.qty == Qty::ZERO)
    {
        return Err(BookError::ZeroSnapshotQuantity.into());
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests;
