//! Complete path: local HTTP/WS -> adapters -> book publication -> robot -> supervision.
use super::mock::{Mock, Response};
use super::*;
use crate::{
    aster::AsterAdapter,
    binance::BinanceAdapter,
    numeric::Qty,
    robot::{Bias, Decision},
    service,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};

// Production counters are process-wide, so service instances must not overlap.
static SERVICE_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type ServerSocket = WebSocketStream<TcpStream>;
#[derive(Clone, Copy, Debug)]
enum Venue {
    Binance,
    Aster,
}
#[derive(Clone, Copy, Debug)]
enum Fault {
    Gap,
    Malformed,
    Close,
    Eof,
    Oversized,
    PriceRange,
}
struct Rig {
    _service_guard: tokio::sync::MutexGuard<'static, ()>,
    bm: Mock,
    am: Mock,
    bw: Option<ServerSocket>,
    aw: Option<ServerSocket>,
    decisions: mpsc::Receiver<Decision>,
    stop: oneshot::Sender<()>,
    service: JoinHandle<Result<(), service::ServiceError>>,
}
impl Rig {
    async fn new(queue_messages: usize) -> Self {
        Self::with_timing(
            queue_messages,
            Duration::from_secs(3),
            config::WATCHDOG,
            config::HTTP_TIMEOUT,
        )
        .await
    }
    async fn with_timing(
        queue_messages: usize,
        max_age: Duration,
        watchdog: Duration,
        http_timeout: Duration,
    ) -> Self {
        let service_guard = SERVICE_TEST.lock().await;
        let mut bm = Mock::new::<BinanceAdapter>().await;
        let mut am = Mock::new::<AsterAdapter>().await;
        bm.settings.queue_messages = queue_messages;
        am.settings.queue_messages = queue_messages;
        bm.settings.http_timeout = http_timeout;
        am.settings.http_timeout = http_timeout;
        bm.settings.watchdog = watchdog;
        am.settings.watchdog = watchdog;
        let b = BinanceAdapter {
            settings: std::mem::replace(
                &mut bm.settings,
                Settings::for_adapter::<BinanceAdapter>(),
            ),
        };
        let a = AsterAdapter {
            settings: std::mem::replace(&mut am.settings, Settings::for_adapter::<AsterAdapter>()),
        };
        let (send, decisions) = mpsc::channel(64);
        let (stop, shutdown) = oneshot::channel();
        // Isolate availability from age/skew guards (covered separately with a controlled clock).
        let config = config::RobotConfig::new(max_age, max_age, Qty::ONE, 5).unwrap();
        let service = tokio::spawn(service::run(
            config,
            b,
            a,
            async { shutdown.await.map_err(std::io::Error::other) },
            move |d| {
                if d.binance.status == BookStatus::Offline || d.aster.status == BookStatus::Offline
                {
                    assert_eq!(d.bias, Bias::Hold);
                }
                send.try_send(*d).unwrap();
            },
        ));
        bm.request("/metadata")
            .await
            .send(Response::ok(BINANCE_METADATA))
            .unwrap();
        am.request("/metadata")
            .await
            .send(Response::ok(ASTER_METADATA))
            .unwrap();
        let bw = Some(bm.socket().await);
        let aw = Some(am.socket().await);
        Self {
            _service_guard: service_guard,
            bm,
            am,
            bw,
            aw,
            decisions,
            stop,
            service,
        }
    }
    fn socket(&mut self, venue: Venue) -> &mut ServerSocket {
        match venue {
            Venue::Binance => self.bw.as_mut().unwrap(),
            Venue::Aster => self.aw.as_mut().unwrap(),
        }
    }
    async fn send(&mut self, venue: Venue, first: u64, last: u64, previous: u64) {
        let event = match venue {
            Venue::Binance => event(Venue::Binance, first, last, previous, "0.3"),
            Venue::Aster => event(Venue::Aster, first, last, previous, "0.3"),
        };
        self.socket(venue).send(event).await.unwrap();
    }
    async fn decision(&mut self, predicate: impl Fn(&Decision) -> bool) -> Decision {
        timeout(Duration::from_secs(3), async {
            loop {
                let d = self.decisions.recv().await.expect("robot stopped");
                if predicate(&d) {
                    break d;
                }
            }
        })
        .await
        .expect("expected decision was not published")
    }
    async fn ready(&mut self) -> Decision {
        let b = self.bm.request("/snapshot").await;
        let a = self.am.request("/snapshot").await;
        self.send(Venue::Binance, 100, 102, 99).await;
        self.send(Venue::Aster, 100, 102, 99).await;
        b.send(Response::ok(BINANCE_SNAPSHOT)).unwrap();
        a.send(Response::ok(ASTER_SNAPSHOT)).unwrap();
        self.decision(|d| d.bias == Bias::LongBiasAster).await
    }
    async fn fault(&mut self, venue: Venue, fault: Fault) {
        match fault {
            Fault::PriceRange => {
                self.socket(venue).send(Message::Text(
                    r#"{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","s":"ETHUSDT","E":1700000000002,"T":1700000000001,"U":103,"u":104,"pu":102,"b":[],"a":[["100000","1"]]}}"#.into()
                )).await.unwrap();
            }
            Fault::Gap => self.send(venue, 103, 104, 101).await,
            Fault::Malformed => self
                .socket(venue)
                .send(Message::Text("{bad".into()))
                .await
                .unwrap(),
            Fault::Close => self.socket(venue).close(None).await.unwrap(),
            Fault::Eof => match venue {
                Venue::Binance => {
                    self.bw.take();
                }
                Venue::Aster => {
                    self.aw.take();
                }
            },
            Fault::Oversized => {
                let _ = self
                    .socket(venue)
                    .send(Message::Text(" ".repeat(config::MESSAGE_BYTES + 1).into()))
                    .await;
            }
        }
    }
    async fn reconnect(&mut self, venue: Venue) {
        match venue {
            Venue::Binance => self.bw = Some(self.bm.socket().await),
            Venue::Aster => self.aw = Some(self.am.socket().await),
        }
    }
    async fn finish(self) {
        self.stop.send(()).unwrap();
        timeout(Duration::from_secs(3), self.service)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
fn failed_status(d: &Decision, venue: Venue) -> BookStatus {
    match venue {
        Venue::Binance => d.binance.status,
        Venue::Aster => d.aster.status,
    }
}
fn healthy(venue: Venue) -> Venue {
    match venue {
        Venue::Binance => Venue::Aster,
        Venue::Aster => Venue::Binance,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn any_feed_fault_holds_entire_instrument_until_new_snapshot_replay() {
    for venue in [Venue::Binance, Venue::Aster] {
        for fault in [
            Fault::Gap,
            Fault::Malformed,
            Fault::Close,
            Fault::Eof,
            Fault::Oversized,
            Fault::PriceRange,
        ] {
            let mut rig = Rig::new(config::SYNC_MESSAGES).await;
            let online = rig.ready().await;
            assert_eq!(online.binance.mid2_raw, Some(7_000_000_000_000));
            assert_eq!(online.aster.mid2_raw, Some(6_992_000_000_000));
            rig.fault(venue, fault).await;
            // Observe Offline before answering any recovery snapshot.
            let offline = rig
                .decision(|d| {
                    failed_status(d, venue) == BookStatus::Offline
                        && failed_status(d, healthy(venue)) == BookStatus::Online
                })
                .await;
            assert_eq!(offline.bias, Bias::Hold, "{venue:?}/{fault:?}");
            if matches!(fault, Fault::Close | Fault::Eof | Fault::Oversized) {
                rig.reconnect(venue).await;
            }
            // Healthy feed continues. Even a valid bridge on the failed feed cannot
            // restore the instrument without the outstanding REST snapshot.
            rig.send(healthy(venue), 103, 104, 102).await;
            rig.send(venue, 100, 102, 99).await;
            match venue {
                Venue::Binance => {
                    let reply = rig.bm.request("/snapshot").await;
                    assert_hold_while_pending(&mut rig, venue).await;
                    reply.send(Response::ok(BINANCE_SNAPSHOT)).unwrap();
                }
                Venue::Aster => {
                    let reply = rig.am.request("/snapshot").await;
                    assert_hold_while_pending(&mut rig, venue).await;
                    reply.send(Response::ok(ASTER_SNAPSHOT)).unwrap();
                }
            }
            rig.decision(|d| d.bias == Bias::LongBiasAster).await;
            rig.finish().await;
        }
    }
}
async fn assert_hold_while_pending(rig: &mut Rig, venue: Venue) {
    // Drain reports for a bounded interval; a valid WS message alone must not resume bias.
    let deadline = tokio::time::sleep(Duration::from_millis(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _=&mut deadline=>break,
            d=rig.decisions.recv()=>{let d=d.unwrap();assert_eq!(d.bias,Bias::Hold);assert_eq!(failed_status(&d,venue),BookStatus::Offline);}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_snapshot_overflow_and_late_http_keep_robot_on_hold() {
    let mut rig = Rig::new(1).await;
    let b = rig.bm.request("/snapshot").await;
    let old = rig.am.request("/snapshot").await;
    rig.socket(Venue::Aster)
        .send(Message::Ping(vec![42].into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), rig.socket(Venue::Aster).next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(vec![42].into())
    );
    rig.send(Venue::Binance, 100, 102, 99).await;
    b.send(Response::ok(BINANCE_SNAPSHOT)).unwrap();
    let only_binance = rig
        .decision(|d| {
            d.binance.status == BookStatus::Online && d.aster.status == BookStatus::Offline
        })
        .await;
    assert_eq!(only_binance.bias, Bias::Hold);
    rig.send(Venue::Aster, 100, 102, 99).await;
    rig.send(Venue::Aster, 103, 104, 102).await; // Overflow the one-message sync queue.
    let current = rig.am.request("/snapshot").await;
    old.send(Response::ok(ASTER_SNAPSHOT)).unwrap(); // Cancelled epoch's response.
    rig.send(Venue::Binance, 103, 104, 102).await;
    assert_hold_while_pending(&mut rig, Venue::Aster).await;
    rig.send(Venue::Aster, 100, 102, 99).await;
    current.send(Response::ok(ASTER_SNAPSHOT)).unwrap();
    rig.decision(|d| d.bias == Bias::LongBiasAster).await;
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_feed_with_pings_and_duplicates_holds_then_goes_offline() {
    let mut rig = Rig::with_timing(
        config::SYNC_MESSAGES,
        Duration::from_millis(200),
        Duration::from_secs(1),
        config::HTTP_TIMEOUT,
    )
    .await;
    rig.ready().await;
    let mut saw_stale = false;
    let mut previous = 102;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timeout(Duration::from_secs(3),async {
        loop {tokio::select! {
            _=tick.tick()=>{
                rig.send(Venue::Binance,previous+1,previous+2,previous).await;
                previous+=2;
                // Transport liveness and exact duplicates do not refresh market data.
                let _=rig.socket(Venue::Aster).send(event(Venue::Aster, 100,102,99,"0.3")).await;
                let _=rig.socket(Venue::Aster).send(Message::Ping(vec![1].into())).await;
            }
            d=rig.decisions.recv()=>{
                let d=d.unwrap();
                assert_eq!(d.binance.status,BookStatus::Online);
                if d.aster.status==BookStatus::Offline {
                    assert_eq!(d.bias,Bias::Hold);break;
                }
                if d.bias==Bias::Hold && d.aster.age.is_some_and(|age|age>Duration::from_millis(200)) {saw_stale=true;}
            }
        }}
    }).await.unwrap();
    assert!(
        saw_stale,
        "age guard must suspend the robot before the feed watchdog"
    );
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_timeout_recovers_while_other_feed_stays_live() {
    let mut rig = Rig::with_timing(
        config::SYNC_MESSAGES,
        Duration::from_secs(3),
        config::WATCHDOG,
        Duration::from_millis(150),
    )
    .await;
    let b = rig.bm.request("/snapshot").await;
    let old = rig.am.request("/snapshot").await;
    rig.send(Venue::Binance, 100, 102, 99).await;
    b.send(Response::ok(BINANCE_SNAPSHOT)).unwrap();
    let d = rig
        .decision(|d| d.binance.status == BookStatus::Online)
        .await;
    assert_eq!(d.bias, Bias::Hold);
    assert_eq!(d.aster.status, BookStatus::Offline);
    let retry = rig.am.request("/snapshot").await;
    drop(old);
    rig.socket(Venue::Binance)
        .send(event(Venue::Binance, 103, 104, 102, "0.4"))
        .await
        .unwrap();
    rig.send(Venue::Aster, 100, 102, 99).await;
    retry.send(Response::ok(ASTER_SNAPSHOT)).unwrap();
    let restored = rig
        .decision(|d| d.bias == Bias::LongBiasAster && d.binance.bid_qty_raw == Some(1_200_000_000))
        .await;
    assert_eq!(restored.binance.status, BookStatus::Online);
    rig.finish().await;
}

const BINANCE_METADATA: &[u8] = include_bytes!("../../../../tests/fixtures/binance/metadata.json");
const ASTER_METADATA: &[u8] = include_bytes!("../../../../tests/fixtures/aster/metadata.json");
const BINANCE_SNAPSHOT: &[u8] = include_bytes!("../../../../tests/fixtures/binance/snapshot.json");
const ASTER_SNAPSHOT: &[u8] = include_bytes!("../../../../tests/fixtures/aster/snapshot.json");
fn event(venue: Venue, first: u64, last: u64, previous: u64, quantity: &str) -> Message {
    let (bid, ask) = match venue {
        Venue::Binance => (3499, 3501),
        Venue::Aster => (3495, 3497),
    };
    Message::Text(format!(r#"{{"stream":"ethusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000002,"T":1700000000001,"s":"ETHUSDT","U":{first},"u":{last},"pu":{previous},"b":[["{bid}","{quantity}"]],"a":[["{ask}","{quantity}"]]}}}}"#).into())
}
