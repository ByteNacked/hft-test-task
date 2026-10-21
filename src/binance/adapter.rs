use crate::{
    book::BookPublisher,
    md::{
        AdapterError, DepthDiff, MDAdapter, ProtocolError, ReceiveTime, Snapshot,
        feed::{self, Protocol, Settings},
    },
    metrics::FeedMetrics,
    numeric::InstrumentRules,
};
use tokio::sync::watch;
use triple_buffer::Input;

pub struct BinanceAdapter {
    pub(crate) settings: Settings,
}
impl Default for BinanceAdapter {
    fn default() -> Self {
        Self {
            settings: Settings::for_protocol::<Self>(),
        }
    }
}
impl MDAdapter for BinanceAdapter {
    async fn run(
        self,
        books: BookPublisher,
        metrics: Input<FeedMetrics>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), AdapterError> {
        feed::run::<Self>(self.settings, books, metrics, shutdown).await
    }
}
impl Protocol for BinanceAdapter {
    const NAME: &'static str = "binance";
    const WS_URL: &'static str = super::WS_URL;
    const METADATA_URL: &'static str = super::METADATA_URL;
    const SNAPSHOT_URL: &'static str = super::SNAPSHOT_URL;
    fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError> {
        super::decode_metadata(body)
    }
    fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError> {
        super::decode_snapshot(body, rules)
    }
    fn decode_diff(
        body: &[u8],
        received: ReceiveTime,
        rules: InstrumentRules,
    ) -> Result<DepthDiff, ProtocolError> {
        super::decode_diff(body, received, rules)
    }
}
