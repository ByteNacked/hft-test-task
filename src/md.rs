//! Market-data adapter contract and transport-independent normalized data.
mod adapter;
mod diff;
pub(crate) use diff::decode_into as decode_diff_into;
pub(crate) mod feed;
pub(crate) mod sync;
use crate::{
    book::{BookError, DiffSide, SnapshotSide, prepare},
    config,
    numeric::{InstrumentRules, Price, Qty},
};
pub use adapter::{AdapterError, MDAdapter};
pub(crate) use adapter::{Backoff, get_body, shutdown_requested};
use serde::Deserialize;
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
pub(crate) fn normalize_diff(diff: &mut DepthDiff) -> Result<(), ProtocolError> {
    if diff.first_id > diff.final_id || diff.previous_id >= diff.final_id {
        return Err(ProtocolError::UpdateIds);
    }
    diff.bids
        .sort_unstable_by_key(|l| std::cmp::Reverse(l.index()));
    diff.asks.sort_unstable_by_key(|l| l.index());
    for side in [&diff.bids, &diff.asks] {
        if side.windows(2).any(|p| p[0].index() == p[1].index()) {
            return Err(BookError::DuplicatePrice.into());
        }
    }
    Ok(())
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

// Metadata is cold-path and capped before deserialization. Bounded symbol/filter arrays
// and borrowed strings prevent unbounded allocation even for adversarial JSON.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Symbol<'a> {
    #[serde(borrow)]
    pub status: &'a str,
    pub contract_type: &'a str,
    pub base_asset: &'a str,
    pub quote_asset: &'a str,
    pub margin_asset: &'a str,
    pub filters: arrayvec::ArrayVec<Filter<'a>, 32>,
    pub contract_size: Option<Qty>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Filter<'a> {
    #[serde(borrow)]
    pub filter_type: &'a str,
    pub tick_size: Option<Price>,
    pub step_size: Option<Qty>,
}
pub(crate) fn instrument<'a>(
    symbols: impl Iterator<Item = &'a serde_json::value::RawValue>,
) -> Result<InstrumentRules, ProtocolError> {
    let mut result = None;
    #[derive(Deserialize)]
    struct SymbolName<'a> {
        #[serde(borrow)]
        symbol: &'a str,
    }
    for raw in symbols {
        let name: SymbolName<'_> = serde_json::from_str(raw.get())?;
        if name.symbol != config::SYMBOL {
            continue;
        }
        // Other markets may have different units, disabled filters or contract multipliers.
        // Interpret numeric metadata only for the instrument that will feed this book.
        let symbol: Symbol<'_> = serde_json::from_str(raw.get())?;
        if result.is_some()
            || symbol.status != "TRADING"
            || symbol.contract_type != "PERPETUAL"
            || symbol.base_asset != "ETH"
            || symbol.quote_asset != "USDT"
            || symbol.margin_asset != "USDT"
            || symbol
                .contract_size
                .is_some_and(|size| size.raw() != crate::numeric::SCALE)
        {
            return Err(ProtocolError::Metadata);
        }
        let (mut tick, mut lot) = (None, None);
        for filter in symbol.filters {
            match filter.filter_type {
                "PRICE_FILTER" => {
                    if tick.is_some() {
                        return Err(ProtocolError::Metadata);
                    }
                    tick = Some(filter.tick_size.ok_or(ProtocolError::Metadata)?);
                }
                "LOT_SIZE" => {
                    if lot.is_some() {
                        return Err(ProtocolError::Metadata);
                    }
                    lot = Some(filter.step_size.ok_or(ProtocolError::Metadata)?);
                }
                _ => {}
            }
        }
        result = Some(InstrumentRules::new(
            tick.ok_or(ProtocolError::Metadata)?,
            lot.ok_or(ProtocolError::Metadata)?,
        )?);
    }
    result.ok_or(ProtocolError::Metadata)
}

pub(crate) struct Symbols<'a>(pub Vec<&'a serde_json::value::RawValue>);
impl<'de> Deserialize<'de> for Symbols<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Symbols<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("at most 4096 instrument records")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut symbols = Vec::with_capacity(32);
                while let Some(symbol) = seq.next_element()? {
                    if symbols.len() == 4096 {
                        return Err(serde::de::Error::custom("too many instruments"));
                    }
                    symbols.push(symbol);
                }
                Ok(Symbols(symbols))
            }
        }
        d.deserialize_seq(Visitor)
    }
}

/// Decode snapshot levels into bounded storage without a temporary side.
pub(crate) fn decode_levels<const N: usize>(
    raw: &serde_json::value::RawValue,
    out: &mut arrayvec::ArrayVec<crate::book::Level, N>,
) -> Result<(), ProtocolError> {
    use serde::de::DeserializeSeed;
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    LevelsSeed(out).deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(())
}

/// Deserialize directly into the caller's bounded array, without a temporary side.
struct LevelsSeed<'a, const N: usize>(&'a mut arrayvec::ArrayVec<crate::book::Level, N>);
impl<'de, const N: usize> serde::de::DeserializeSeed<'de> for LevelsSeed<'_, N> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, const N: usize> serde::de::Visitor<'de> for LevelsSeed<'_, N> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bounded array of price/quantity pairs")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        self.0.clear();
        while let Some(level) = seq.next_element()? {
            self.0
                .try_push(level)
                .map_err(|_| serde::de::Error::custom("too many levels"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
