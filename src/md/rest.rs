//! Shared public snapshot and instrument metadata schemas.
use super::{ProtocolError, Snapshot};
use crate::{
    book::SnapshotSide,
    config,
    numeric::{InstrumentRules, Price, Qty},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Depth<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E")]
    exchange_time: u64,
    #[serde(rename = "T")]
    transaction_time: u64,
    #[serde(borrow)]
    bids: &'a serde_json::value::RawValue,
    #[serde(borrow)]
    asks: &'a serde_json::value::RawValue,
}
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a, 'a: 'de"))]
struct ExchangeInfo<'a> {
    #[serde(borrow)]
    symbols: Symbols<'a>,
    #[serde(rename = "futuresType")]
    futures_type: Option<&'a str>,
}

pub(super) fn decode_snapshot(
    body: &[u8],
    rules: InstrumentRules,
) -> Result<Snapshot, ProtocolError> {
    super::check_body(body, config::MESSAGE_BYTES)?;
    let depth: Depth<'_> = serde_json::from_slice(body)?;
    super::validate_timestamp(depth.exchange_time)?;
    super::validate_timestamp(depth.transaction_time)?;
    let mut normalized = Snapshot {
        last_update_id: depth.last_update_id,
        bids: Default::default(),
        asks: Default::default(),
    };
    decode_levels(depth.bids, &mut normalized.bids)?;
    decode_levels(depth.asks, &mut normalized.asks)?;
    super::normalize_snapshot(normalized, rules)
}
pub(super) fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError> {
    super::check_body(body, config::METADATA_BYTES)?;
    let info: ExchangeInfo<'_> = serde_json::from_slice(body)?;
    if info
        .futures_type
        .is_some_and(|market| market != "U_MARGINED")
    {
        return Err(ProtocolError::Metadata);
    }
    instrument(info.symbols.0.into_iter())
}

// Metadata is cold-path and capped before deserialization. Bounded symbol/filter arrays
// and borrowed strings prevent unbounded allocation even for adversarial JSON.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Symbol<'a> {
    #[serde(borrow)]
    status: &'a str,
    contract_type: &'a str,
    base_asset: &'a str,
    quote_asset: &'a str,
    margin_asset: &'a str,
    filters: arrayvec::ArrayVec<Filter<'a>, 32>,
    contract_size: Option<Qty>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Filter<'a> {
    #[serde(borrow)]
    filter_type: &'a str,
    tick_size: Option<Price>,
    step_size: Option<Qty>,
}
fn instrument<'a>(
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

struct Symbols<'a>(Vec<&'a serde_json::value::RawValue>);
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
fn decode_levels(
    raw: &serde_json::value::RawValue,
    out: &mut SnapshotSide,
) -> Result<(), ProtocolError> {
    use serde::de::DeserializeSeed;
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    LevelsSeed(out).deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(())
}

/// Deserialize directly into the caller's bounded array, without a temporary side.
struct LevelsSeed<'a>(&'a mut SnapshotSide);
impl<'de> serde::de::DeserializeSeed<'de> for LevelsSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de> serde::de::Visitor<'de> for LevelsSeed<'_> {
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
