//! Included only in a temporary checkout by validate_hotpath.py.
use super::*;
use crate::{
    aster::AsterAdapter,
    binance::BinanceAdapter,
    logging,
    numeric::SCALE,
    robot::{Bias, BookNumbers},
    service,
    test_support::{diff, level, price, qty},
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

type Times = Arc<[AtomicU64]>;
fn times(count: usize) -> Times {
    (0..count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>()
        .into()
}

struct BenchAdapter<P> {
    protocol: PhantomData<P>,
    depth: usize,
    bodies: Vec<Utf8Bytes>,
    received: Times,
    origin: std::time::Instant,
    warmup: usize,
    done: tokio::sync::mpsc::Sender<serde_json::Value>,
}

impl<P: MDAdapter> MDAdapter for BenchAdapter<P> {
    const NAME: &'static str = P::NAME;
    const WS_URL: &'static str = P::WS_URL;
    const METADATA_URL: &'static str = P::METADATA_URL;
    const SNAPSHOT_URL: &'static str = P::SNAPSHOT_URL;
    fn decode_metadata(body: &[u8]) -> Result<InstrumentRules, ProtocolError> {
        P::decode_metadata(body)
    }
    fn decode_snapshot(body: &[u8], rules: InstrumentRules) -> Result<Snapshot, ProtocolError> {
        P::decode_snapshot(body, rules)
    }
    #[cfg(not(feature = "bench-old-api"))]
    fn decode_diff_into(
        body: &[u8],
        received: ReceiveTime,
        rules: InstrumentRules,
        out: &mut DepthDiff,
    ) -> Result<(), ProtocolError> {
        P::decode_diff_into(body, received, rules, out)
    }
    #[cfg(feature = "bench-old-api")]
    fn decode_diff(
        body: &[u8],
        received: ReceiveTime,
        rules: InstrumentRules,
    ) -> Result<crate::md::DepthDiff, ProtocolError> {
        P::decode_diff(body, received, rules)
    }
    async fn run(
        self,
        books: BookPublisher,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), AdapterError> {
        let rules = InstrumentRules::new(price(10_000_000), qty(1_000_000)).unwrap();
        let mut runner = Runner::<P> {
            protocol: PhantomData,
            settings: Settings::for_adapter::<P>(),
            client: reqwest::Client::new(),
            rules,
            #[cfg(not(feature = "bench-price-slots"))]
            synced: Box::new(SyncedBook::new()?),
            #[cfg(feature = "bench-price-slots")]
            synced: Box::new(SyncedBook::new(rules)?),
            #[cfg(not(feature = "bench-old-api"))]
            incoming: Box::default(),
            books,
            state: FeedState::Connecting,
            sync_epoch: 0,
            last_diff_id: None,
            next_log: Instant::now(),
            queue: VecDeque::new(),
            queued_bytes: 0,
            job: SnapshotJob::Idle,
            backoff: Backoff::new(),
            next_snapshot: Instant::now(),
            sync_deadline: Instant::now(),
            live_since: None,
            online: false,
        };
        runner.synced.load(&Snapshot {
            last_update_id: 100,
            bids: (0..1000)
                .map(|i| level(3490 * SCALE - i * 10_000_000, SCALE))
                .collect(),
            asks: (0..1000)
                .map(|i| level(3510 * SCALE + i * 10_000_000, SCALE))
                .collect(),
        })?;
        for begin in (1000..self.depth).step_by(4096) {
            let mut extra = diff(std::time::Instant::now());
            for i in begin..(begin + 4096).min(self.depth) {
                let bid = level(3490 * SCALE - i as i64 * 10_000_000, SCALE);
                let ask = level(3510 * SCALE + i as i64 * 10_000_000, SCALE);
                #[cfg(feature = "bench-indexed-diff")]
                let bid = crate::book::IndexedLevel::new(bid, rules)?;
                #[cfg(feature = "bench-indexed-diff")]
                let ask = crate::book::IndexedLevel::new(ask, rules)?;
                extra.bids.push(bid);
                extra.asks.push(ask);
            }
            runner.synced.book.apply_diff(&extra)?;
        }
        let start_thread = format!("{:?}", std::thread::current().id());
        let mut tick = tokio::time::interval(Duration::from_millis(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut measured_start = None;
        for (i, text) in self.bodies.iter().enumerate() {
            tokio::select! {
                _ = shutdown_requested(&mut shutdown) => { runner.offline()?; return Ok(()); }
                _ = tick.tick() => {}
            }
            let text = text.clone();
            let received = ReceiveTime::now();
            if i == self.warmup {
                measured_start = Some(received);
            }
            // Release precedes publication; the callback acquires the matching timestamp.
            self.received[i].store(
                (received - self.origin).as_nanos() as u64,
                Ordering::Release,
            );
            if runner.apply(Buffered { text, received })? {
                runner.publish_online()?;
            }
        }
        let elapsed = measured_start.unwrap().elapsed().as_secs_f64();
        self.done
            .send(serde_json::json!({
                "venue": P::NAME, "start_thread": start_thread,
                "end_thread": format!("{:?}", std::thread::current().id()),
                "published": self.bodies.len()-self.warmup,
                "updates_per_second": (self.bodies.len()-self.warmup) as f64/elapsed,
            }))
            .await
            .unwrap();
        shutdown_requested(&mut shutdown).await;
        runner.offline()
    }
}

fn bodies(total: usize, changes: usize, opposite: bool) -> Vec<Utf8Bytes> {
    (0..total).map(|i| {
        // Encode the message index in the top-five bid sum, preserving price/lot steps.
        // The temporary best bid/ask alternates the midpoint and exercises real bias logs.
        let high = (i % 2 == 0) ^ opposite;
        let q = SCALE + (i as i64+1)*1_000_000;
        let mut bids = vec![format!("[\"3490\",\"{}\"]", qty(q)),
            format!("[\"3509\",\"{}\"]", usize::from(high))];
        let mut asks = vec![format!("[\"3491\",\"{}\"]", usize::from(!high))];
        if changes == 100 {
            bids.extend((1..99).map(|j| format!("[\"{}\",\"1\"]",price(3490*SCALE-j*10_000_000))));
            asks.extend((0..99).map(|j| format!("[\"{}\",\"1\"]",price(3510*SCALE+j*10_000_000))));
        }
        let u = 101+i;
        format!(r#"{{"stream":"ethusdt@depth@100ms","data":{{"e":"depthUpdate","s":"ETHUSDT","E":1700000000002,"T":1700000000001,"U":{},"u":{u},"pu":{},"b":[{}],"a":[{}]}}}}"#,
            if i==0 {100} else {u},u-1,bids.join(","),asks.join(",")).into()
    }).collect()
}

fn message_index(numbers: &BookNumbers) -> Option<usize> {
    if numbers.status != BookStatus::Online {
        return None;
    }
    let encoded = numbers.bid_qty_raw? - 5 * i128::from(SCALE);
    (encoded > 0).then(|| (encoded / 1_000_000 - 1) as usize)
}

#[test]
#[ignore]
fn threaded_hotpath_validation() {
    let _logs = logging::init_logging();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let samples: usize = std::env::var("HFT_BENCH_SAMPLES")
        .unwrap_or("10000".into())
        .parse()
        .unwrap();
    let warmup = 1000;
    for (depth, changes) in [(1000, 2), (8000, 2), (1000, 100)] {
        let total = warmup + samples;
        let received = [times(total), times(total)];
        let latency = [times(total), times(total)];
        let observed = latency.clone();
        let timestamps = received.clone();
        let origin = std::time::Instant::now();
        let (done, mut complete) = tokio::sync::mpsc::channel(2);
        let b = BenchAdapter::<BinanceAdapter> {
            protocol: PhantomData,
            depth,
            bodies: bodies(total, changes, false),
            received: received[0].clone(),
            origin,
            warmup,
            done: done.clone(),
        };
        let a = BenchAdapter::<AsterAdapter> {
            protocol: PhantomData,
            depth,
            bodies: bodies(total, changes, true),
            received: received[1].clone(),
            origin,
            warmup,
            done,
        };
        let mut feeds = Vec::with_capacity(2);
        let mut reporter = logging::decision_reporter();
        let mut previous = Bias::Hold;
        runtime
            .block_on(service::run(
                config::RobotConfig::default(),
                b,
                a,
                async {
                    for _ in 0..2 {
                        feeds.push(
                            complete
                                .recv()
                                .await
                                .ok_or_else(|| std::io::Error::other("producer stopped"))?,
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                },
                move |decision| {
                    let end = origin.elapsed().as_nanos() as u64;
                    if decision.bias != previous {
                        previous = decision.bias;
                        let newest = [&decision.binance, &decision.aster]
                            .into_iter()
                            .enumerate()
                            .filter_map(|(venue, book)| {
                                let index = message_index(book)?;
                                let stamp = timestamps[venue].get(index)?.load(Ordering::Acquire);
                                (stamp != 0).then_some((stamp, venue, index))
                            })
                            .max();
                        if let Some((start, venue, index)) = newest
                            && index >= warmup
                            && observed[venue][index].load(Ordering::Relaxed) == 0
                        {
                            // One robot writer; collection happens after service joins that thread.
                            observed[venue][index].store(end - start, Ordering::Relaxed);
                        }
                    }
                    reporter(decision);
                },
            ))
            .unwrap();
        for (venue, values) in ["binance", "aster"].into_iter().zip(latency) {
            let mut ns: Vec<_> = values
                .iter()
                .skip(warmup)
                .map(|v| v.load(Ordering::Relaxed))
                .filter(|v| *v != 0)
                .collect();
            ns.sort_unstable();
            assert!(
                ns.len() >= 1000,
                "too few bias-change samples for {venue}: {}",
                ns.len()
            );
            let percentile = |p: usize| ns[((ns.len() * p).div_ceil(1000) - 1).min(ns.len() - 1)];
            println!(
                "HFT_BENCH {}",
                serde_json::json!({"venue":venue,"base_depth":depth,
                "bid_changes":changes,"ask_changes":if changes==2 {1} else {100},
                "samples":ns.len(),"published":samples,"p50_ns":percentile(500),
                "p99_ns":percentile(990),"p999_ns":percentile(999),"max_ns":ns.last(),
                "feed":feeds.iter().find(|f|f["venue"]==venue).unwrap()})
            );
        }
    }
}
