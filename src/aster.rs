//! Aster public market-data adapter and wire schema.
use crate::{
    book::BookPublisher,
    md::{
        AdapterError, MDAdapter,
        feed::{self, Settings},
    },
};
use crate::{
    config,
    md::{self, DepthDiff, DiffScratch, ProtocolError, ReceiveTime, Snapshot},
    numeric::InstrumentRules,
};
use serde::Deserialize;
use tokio::sync::watch;

pub const WS_URL: &str = "wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms";
pub const METADATA_URL: &str = "https://fapi.asterdex.com/fapi/v3/exchangeInfo";
pub const SNAPSHOT_URL: &str = "https://fapi.asterdex.com/fapi/v3/depth?symbol=ETHUSDT&limit=1000";

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

pub struct AsterAdapter {
    pub(crate) settings: Settings,
}
impl Default for AsterAdapter {
    fn default() -> Self {
        Self {
            settings: Settings::for_adapter::<Self>(),
        }
    }
}
impl MDAdapter for AsterAdapter {
    const NAME: &'static str = "aster";
    const WS_URL: &'static str = WS_URL;
    const METADATA_URL: &'static str = METADATA_URL;
    const SNAPSHOT_URL: &'static str = SNAPSHOT_URL;
    async fn run(
        self,
        books: BookPublisher,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), AdapterError> {
        feed::run::<Self>(self.settings, books, shutdown).await
    }
    fn decode_diff_into(
        body: &[u8],
        receive: ReceiveTime,
        rules: InstrumentRules,
        out: &mut DepthDiff,
        scratch: &mut DiffScratch,
    ) -> Result<(), ProtocolError> {
        md::decode_diff_into::<false>(body, receive, rules, out, scratch)
    }
    fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError> {
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
    fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError> {
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
}
