use super::*;
mod binance {
    use super::*;
    use crate::binance::BinanceAdapter as TestAdapter;
    pub(super) const METADATA: &[u8] =
        include_bytes!("../../../tests/fixtures/binance/metadata.json");
    pub(super) const SNAPSHOT: &[u8] =
        include_bytes!("../../../tests/fixtures/binance/snapshot.json");
    const BID: u64 = 3499;
    const ASK: u64 = 3501;
    include!("recovery_tests.rs");
}
mod aster {
    use super::*;
    use crate::aster::AsterAdapter as TestAdapter;
    pub(super) const METADATA: &[u8] =
        include_bytes!("../../../tests/fixtures/aster/metadata.json");
    pub(super) const SNAPSHOT: &[u8] =
        include_bytes!("../../../tests/fixtures/aster/snapshot.json");
    const BID: u64 = 3495;
    const ASK: u64 = 3497;
    include!("recovery_tests.rs");
}

#[tokio::test]
async fn feeds_keep_books_recovery_and_shutdown_independent() {
    use crate::{aster::AsterAdapter, binance::BinanceAdapter};
    use futures_util::SinkExt;
    let mut bm = binance::Mock::new().await;
    let mut am = aster::Mock::new().await;
    let mut b = binance::Harness::start(std::mem::replace(
        &mut bm.settings,
        Settings::for_protocol::<BinanceAdapter>(),
    ));
    let mut a = aster::Harness::start(std::mem::replace(
        &mut am.settings,
        Settings::for_protocol::<AsterAdapter>(),
    ));
    bm.request("/metadata")
        .await
        .send(binance::Response::ok(binance::METADATA))
        .unwrap();
    am.request("/metadata")
        .await
        .send(aster::Response::ok(aster::METADATA))
        .unwrap();
    let mut bw = bm.socket().await;
    let mut aw = am.socket().await;
    let pending = am.request("/snapshot").await;
    b.live(&mut bm, &mut bw).await;
    // Aster's slow HTTP must not stop Binance's live processing.
    let seq = b.reader.view().sequence();
    bw.send(binance::event(103, 104, 102, "0.4")).await.unwrap();
    binance::wait_book(&mut b.reader, BookStatus::Online, seq).await;
    assert_eq!(b.reader.view().bids()[0].qty.raw(), 400_000_000);
    a.reader.update();
    assert_eq!(a.reader.view().status(), BookStatus::Offline);
    pending
        .send(aster::Response {
            status: 429,
            headers: "Retry-After: 1\r\n",
            body: Vec::new(),
        })
        .unwrap();
    aster::wait_metrics(&mut a.stats, |s| s.resyncs == 2).await;
    let seq = b.reader.view().sequence();
    bw.send(binance::event(105, 106, 104, "0.5")).await.unwrap();
    binance::wait_book(&mut b.reader, BookStatus::Online, seq).await;
    binance::wait_metrics(&mut b.stats, |s| s.applied_diffs == 3).await;
    assert_eq!(b.stats.output_buffer().resyncs, 1);
    assert_eq!(b.stats.output_buffer().last_fault, None);
    a.live(&mut am, &mut aw).await;
    // Now fail Binance while Aster continues on its own sequence and book.
    let seq = b.reader.view().sequence();
    bw.send(binance::event(107, 108, 105, "0.6")).await.unwrap();
    binance::wait_book(&mut b.reader, BookStatus::Offline, seq).await;
    let seq = a.reader.view().sequence();
    aw.send(aster::event(103, 104, 102, "0.6")).await.unwrap();
    aster::wait_book(&mut a.reader, BookStatus::Online, seq).await;
    assert_eq!(a.reader.view().bids()[0].price.raw(), 3_495_000_000_000);
    assert_eq!(a.reader.view().bids()[0].qty.raw(), 600_000_000);
    aster::wait_metrics(&mut a.stats, |s| s.applied_diffs == 2).await;
    assert_eq!(a.stats.output_buffer().resyncs, 2);
    assert_eq!(a.stats.output_buffer().gaps, 0);
    assert_eq!(a.stats.output_buffer().connection_epoch, 1);
    b.live(&mut bm, &mut bw).await;
    a.finish().await;
    // Stopping Aster does not close Binance or reuse its shutdown signal.
    let seq = b.reader.view().sequence();
    bw.send(binance::event(103, 104, 102, "0.7")).await.unwrap();
    binance::wait_book(&mut b.reader, BookStatus::Online, seq).await;
    assert_eq!(b.reader.view().bids()[0].price.raw(), 3_499_000_000_000);
    assert_eq!(b.reader.view().bids()[0].qty.raw(), 700_000_000);
    binance::wait_metrics(&mut b.stats, |s| s.applied_diffs == 5).await;
    assert_eq!(b.stats.output_buffer().resyncs, 2);
    assert_eq!(b.stats.output_buffer().gaps, 1);
    assert_eq!(b.stats.output_buffer().connection_epoch, 1);
    b.finish().await;
}
