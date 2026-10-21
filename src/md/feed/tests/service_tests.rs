//! Complete path: local HTTP/WS -> adapters -> book publication -> robot -> supervision.
use super::*;
use crate::{
    aster::AsterAdapter,
    binance::BinanceAdapter,
    numeric::Qty,
    robot::{Bias, Decision},
    service,
};
use futures_util::SinkExt;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};

type ServerSocket = WebSocketStream<TcpStream>;
#[derive(Clone, Copy, Debug)]
enum Venue {
    Binance,
    Aster,
}
#[derive(Clone, Copy, Debug)]
enum Fault {
    Gap,
    Regression,
    Conflict,
    Malformed,
    Close,
    Eof,
    Oversized,
}
struct Rig {
    bm: binance::Mock,
    am: aster::Mock,
    bw: Option<ServerSocket>,
    aw: Option<ServerSocket>,
    decisions: mpsc::Receiver<Decision>,
    stop: oneshot::Sender<()>,
    service: JoinHandle<Result<(), service::ServiceError>>,
}
impl Rig {
    async fn new(queue_messages: usize) -> Self {
        Self::with_timing(queue_messages, Duration::from_secs(3), config::WATCHDOG).await
    }
    async fn with_timing(queue_messages: usize, max_age: Duration, watchdog: Duration) -> Self {
        let mut bm = binance::Mock::new().await;
        let mut am = aster::Mock::new().await;
        bm.settings.queue_messages = queue_messages;
        am.settings.queue_messages = queue_messages;
        bm.settings.watchdog = watchdog;
        am.settings.watchdog = watchdog;
        let b = BinanceAdapter {
            settings: std::mem::replace(
                &mut bm.settings,
                Settings::for_protocol::<BinanceAdapter>(),
            ),
        };
        let a = AsterAdapter {
            settings: std::mem::replace(&mut am.settings, Settings::for_protocol::<AsterAdapter>()),
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
                    assert!(d.edge_bps.is_none());
                }
                send.try_send(*d).unwrap();
            },
            || 0,
        ));
        bm.request("/metadata")
            .await
            .send(binance::Response::ok(binance::METADATA))
            .unwrap();
        am.request("/metadata")
            .await
            .send(aster::Response::ok(aster::METADATA))
            .unwrap();
        let bw = Some(bm.socket().await);
        let aw = Some(am.socket().await);
        Self {
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
            Venue::Binance => binance::event(first, last, previous, "0.3"),
            Venue::Aster => aster::event(first, last, previous, "0.3"),
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
        b.send(binance::Response::ok(binance::SNAPSHOT)).unwrap();
        a.send(aster::Response::ok(aster::SNAPSHOT)).unwrap();
        self.decision(|d| d.bias == Bias::LongBias).await
    }
    async fn fault(&mut self, venue: Venue, fault: Fault) {
        match fault {
            Fault::Gap => self.send(venue, 103, 104, 101).await,
            Fault::Regression => self.send(venue, 99, 101, 98).await,
            Fault::Conflict => {
                let msg = match venue {
                    Venue::Binance => binance::event(100, 102, 99, "0.4"),
                    Venue::Aster => aster::event(100, 102, 99, "0.4"),
                };
                self.socket(venue).send(msg).await.unwrap();
            }
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

#[tokio::test]
async fn any_feed_fault_holds_entire_instrument_until_new_snapshot_replay() {
    for venue in [Venue::Binance, Venue::Aster] {
        for fault in [
            Fault::Gap,
            Fault::Regression,
            Fault::Conflict,
            Fault::Malformed,
            Fault::Close,
            Fault::Eof,
            Fault::Oversized,
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
            assert_eq!(offline.binance.exchange_time, online.binance.exchange_time);
            assert_eq!(offline.aster.platform_time, online.aster.platform_time);
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
                    reply
                        .send(binance::Response::ok(binance::SNAPSHOT))
                        .unwrap();
                }
                Venue::Aster => {
                    let reply = rig.am.request("/snapshot").await;
                    assert_hold_while_pending(&mut rig, venue).await;
                    reply.send(aster::Response::ok(aster::SNAPSHOT)).unwrap();
                }
            }
            let restored = rig.decision(|d| d.bias == Bias::LongBias).await;
            assert!(restored.binance.sequence >= offline.binance.sequence);
            assert!(restored.aster.sequence >= offline.aster.sequence);
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

#[tokio::test]
async fn slow_snapshot_overflow_and_late_http_keep_robot_on_hold() {
    let mut rig = Rig::new(1).await;
    let b = rig.bm.request("/snapshot").await;
    let old = rig.am.request("/snapshot").await;
    rig.send(Venue::Binance, 100, 102, 99).await;
    b.send(binance::Response::ok(binance::SNAPSHOT)).unwrap();
    let only_binance = rig
        .decision(|d| {
            d.binance.status == BookStatus::Online && d.aster.status == BookStatus::Offline
        })
        .await;
    assert_eq!(only_binance.bias, Bias::Hold);
    rig.send(Venue::Aster, 100, 102, 99).await;
    rig.send(Venue::Aster, 103, 104, 102).await; // Overflow the one-message sync queue.
    let current = rig.am.request("/snapshot").await;
    old.send(aster::Response::ok(aster::SNAPSHOT)).unwrap(); // Cancelled epoch's response.
    rig.send(Venue::Binance, 103, 104, 102).await;
    assert_hold_while_pending(&mut rig, Venue::Aster).await;
    rig.send(Venue::Aster, 100, 102, 99).await;
    current.send(aster::Response::ok(aster::SNAPSHOT)).unwrap();
    rig.decision(|d| d.bias == Bias::LongBias).await;
    rig.finish().await;
}

#[tokio::test]
async fn silent_feed_with_pings_and_duplicates_holds_then_goes_offline() {
    let mut rig = Rig::with_timing(
        config::SYNC_MESSAGES,
        Duration::from_millis(200),
        Duration::from_secs(1),
    )
    .await;
    let online = rig.ready().await;
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
                let _=rig.socket(Venue::Aster).send(aster::event(100,102,99,"0.3")).await;
                let _=rig.socket(Venue::Aster).send(Message::Ping(vec![1].into())).await;
            }
            d=rig.decisions.recv()=>{
                let d=d.unwrap();
                assert_eq!(d.binance.status,BookStatus::Online);
                assert_eq!(d.aster.platform_time,online.aster.platform_time);
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
