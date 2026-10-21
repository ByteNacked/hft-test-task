//! Aster public market-data adapter and wire schema.
mod adapter;
use crate::{
    config,
    md::{self, DepthDiff, EventTime, ProtocolError, ReceiveTime, Snapshot},
    numeric::InstrumentRules,
};
pub use adapter::AsterAdapter;
use serde::Deserialize;

pub const WS_URL: &str = "wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms";
pub const METADATA_URL: &str = "https://fapi.asterdex.com/fapi/v3/exchangeInfo";
pub const SNAPSHOT_URL: &str = "https://fapi.asterdex.com/fapi/v3/depth?symbol=ETHUSDT&limit=1000";

#[derive(Deserialize)]
struct Wrapper<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: Diff<'a>,
}
#[derive(Deserialize)]
struct Diff<'a> {
    #[serde(borrow)]
    e: &'a str,
    s: &'a str,
    #[serde(rename = "E")]
    exchange_time: u64,
    #[serde(rename = "T")]
    transaction_time: u64,
    #[serde(rename = "U")]
    first_id: u64,
    #[serde(rename = "u")]
    final_id: u64,
    #[serde(rename = "pu")]
    previous_id: u64,
    #[serde(borrow)]
    b: &'a serde_json::value::RawValue,
    #[serde(borrow)]
    a: &'a serde_json::value::RawValue,
}
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
    symbols: md::Symbols<'a>,
    #[serde(rename = "futuresType")]
    futures_type: Option<&'a str>,
}

pub fn decode_diff(
    body: &[u8],
    receive: ReceiveTime,
    rules: InstrumentRules,
) -> Result<DepthDiff, ProtocolError> {
    md::check_body(body, config::MESSAGE_BYTES)?;
    let wrapper: Wrapper<'_> = serde_json::from_slice(body)?;
    let diff = wrapper.data;
    if wrapper.stream != config::STREAM || diff.e != "depthUpdate" || diff.s != config::SYMBOL {
        return Err(ProtocolError::WrongStream);
    }
    md::validate_timestamp(diff.transaction_time)?;
    let time = EventTime::new(diff.exchange_time, receive)?;
    let mut normalized = DepthDiff {
        first_id: diff.first_id,
        final_id: diff.final_id,
        previous_id: diff.previous_id,
        bids: Default::default(),
        asks: Default::default(),
        time,
    };
    md::decode_levels(diff.b, &mut normalized.bids)?;
    md::decode_levels(diff.a, &mut normalized.asks)?;
    md::normalize_diff(normalized, rules)
}
pub fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError> {
    md::check_body(body, config::MESSAGE_BYTES)?;
    let depth: Depth<'_> = serde_json::from_slice(body)?;
    md::validate_timestamp(depth.exchange_time)?;
    md::validate_timestamp(depth.transaction_time)?;
    let mut normalized = Snapshot {
        last_update_id: depth.last_update_id,
        bids: Default::default(),
        asks: Default::default(),
    };
    md::decode_levels(depth.bids, &mut normalized.bids)?;
    md::decode_levels(depth.asks, &mut normalized.asks)?;
    md::normalize_snapshot(normalized, rules)
}
pub fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError> {
    md::check_body(body, config::METADATA_BYTES)?;
    let info: ExchangeInfo<'_> = serde_json::from_slice(body)?;
    if info
        .futures_type
        .is_some_and(|market| market != "U_MARGINED")
    {
        return Err(ProtocolError::Metadata);
    }
    md::instrument(info.symbols.0.into_iter())
}
