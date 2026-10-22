//! Binance USD-M wire schema and snapshot/diff adapter.
use crate::{
    book::BookPublisher,
    md::{
        AdapterError, MDAdapter,
        feed::{self, Settings},
    },
};
use crate::{
    md::{self, DepthDiff, DiffScratch, ProtocolError, ReceiveTime},
    numeric::InstrumentRules,
};
use tokio::sync::watch;

pub const WS_URL: &str = "wss://fstream.binance.com/public/stream?streams=ethusdt@depth@100ms";
pub const METADATA_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
pub const SNAPSHOT_URL: &str = "https://fapi.binance.com/fapi/v1/depth?symbol=ETHUSDT&limit=1000";

pub struct BinanceAdapter {
    pub(crate) settings: Settings,
}
impl Default for BinanceAdapter {
    fn default() -> Self {
        Self {
            settings: Settings::for_adapter::<Self>(),
        }
    }
}
impl MDAdapter for BinanceAdapter {
    const NAME: &'static str = "binance";
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
        window: crate::book::PriceWindow,
    ) -> Result<(), ProtocolError> {
        md::decode_diff_into::<true>(body, receive, rules, out, scratch, window)
    }
}
