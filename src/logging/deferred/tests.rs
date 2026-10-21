use super::*;
use crate::{
    config::RobotConfig,
    logging::TextFormat,
    robot,
    test_support::{book, view},
};
use std::{io::Write, sync::Mutex, time::Instant};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

// Scoped subscribers change tracing's global callsite interest/max-level caches.
// Keep their setup/teardown separate; producer threads inside each test still run concurrently.
static SUBSCRIBER_TEST: Mutex<()> = Mutex::new(());

type Seen = Arc<Mutex<Vec<(thread::ThreadId, Option<SystemTime>, String)>>>;
#[derive(Clone)]
struct Capture(Seen);
impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().push((
            thread::current().id(),
            crate::logging::EVENT_TIME.get(),
            String::from_utf8(bytes.to_vec()).unwrap(),
        ));
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
// A synchronous fmt subscriber makes the formatting thread observable, without the appender.
fn subscriber(seen: Seen, level: &str) -> impl tracing::Subscriber + Send + Sync {
    tracing_subscriber::fmt()
        .with_env_filter(format!("hft_test_task={level}"))
        .with_writer(move || Capture(seen.clone()))
        .with_ansi(false)
        .event_format(TextFormat { color: true })
        .finish()
}
fn decision(bias: Bias) -> Decision {
    let now = Instant::now();
    let reader = view(&book(now, 100_000, crate::numeric::SCALE));
    let mut d = robot::evaluate(
        reader.view(),
        reader.view(),
        &RobotConfig::default(),
        Instant::now(),
    );
    d.bias = bias;
    d.binance.best_bid = Some("3499.01".parse().unwrap());
    d
}

#[test]
fn formatting_is_off_robot_and_drop_drains_with_original_time_and_filters() {
    let _subscriber = SUBSCRIBER_TEST.lock().unwrap();
    for level in ["info", "debug", "warn"] {
        let seen = Seen::default();
        tracing::subscriber::with_default(subscriber(seen.clone(), level), || {
            let caller = thread::current().id();
            let events = Arc::new(Events::new(config::LOG_RECORDS));
            let guard = FormatterGuard::start(events.clone()).unwrap();
            let mut report = reporter(events.clone());
            let begin = SystemTime::now();
            report(&decision(Bias::Hold));
            let mut d = decision(Bias::LongBiasAster);
            report(&d);
            d.binance.best_bid = Some("100".parse().unwrap());
            report(&d);
            report(&decision(Bias::ShortBiasAster));
            let end = SystemTime::now();
            drop(report);
            drop(guard); // All accepted records must be formatted before this returns.
            let rows = seen.lock().unwrap();
            assert!(
                rows.iter()
                    .all(|(id, time, _)| *id != caller
                        && time.is_some_and(|t| t >= begin && t <= end))
            );
            let text = rows.iter().map(|(_, _, s)| s.as_str()).collect::<String>();
            assert_eq!(
                text.matches("bias changed").count(),
                if level == "warn" { 0 } else { 2 }
            );
            assert_eq!(
                text.matches("robot decision").count(),
                if level == "debug" { 4 } else { 0 }
            );
            if level != "warn" {
                assert!(text.contains("\x1b[42;30mINFO:"));
                assert!(text.contains("best_bid=3499.01"), "{text}");
                assert!(text.contains("LongBiasAster"));
                assert!(text.contains("ShortBiasAster"));
            }
        });
    }
}

struct LossCount(Arc<AtomicU64>);
impl<S: tracing::Subscriber> Layer<S> for LossCount {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        struct Fields<'a>(&'a AtomicU64);
        impl tracing::field::Visit for Fields<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "dropped" {
                    self.0.fetch_add(value, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        assert_eq!(*event.metadata().level(), tracing::Level::ERROR);
        event.record(&mut Fields(&self.0));
    }
}

#[test]
fn full_queue_drops_without_overwrite_and_retries_bias_then_reports_loss_on_close() {
    let _subscriber = SUBSCRIBER_TEST.lock().unwrap();
    let events = Arc::new(Events {
        robot_info: true,
        robot_debug: false,
        feed_info: true,
        ..Events::new(1)
    });
    let mut report = reporter(events.clone());
    report(&decision(Bias::LongBiasAster));
    report(&decision(Bias::ShortBiasAster)); // Full; preserve the earlier record.
    events.feed("aster", FeedEvent::Status(false)); // Other producers share the same bound.
    assert_eq!(events.dropped.load(Ordering::Relaxed), 2);
    assert!(matches!(
        events.queue.pop().unwrap().event,
        Event::Decision {
            decision: Decision {
                bias: Bias::LongBiasAster,
                ..
            },
            ..
        }
    ));
    report(&decision(Bias::ShortBiasAster)); // Retry the unreported transition.
    assert!(matches!(
        events.queue.pop().unwrap().event,
        Event::Decision {
            decision: Decision {
                bias: Bias::ShortBiasAster,
                ..
            },
            changed: true
        }
    ));
    report(&decision(Bias::ShortBiasAster));
    assert!(events.queue.pop().is_none()); // Unchanged bias at INFO does not fill the queue.
    drop(report);
    events.closed.store(true, Ordering::Release);
    let observed = Arc::new(AtomicU64::new(0));
    tracing::subscriber::with_default(
        tracing_subscriber::registry().with(LossCount(observed.clone())),
        || drain(&events),
    );
    assert_eq!(observed.load(Ordering::Relaxed), 2);
    assert_eq!(events.dropped.load(Ordering::Relaxed), 0);
}

#[test]
fn concurrent_feeds_keep_fields_order_colors_and_drain_after_producers_stop() {
    let _subscriber = SUBSCRIBER_TEST.lock().unwrap();
    let seen = Seen::default();
    tracing::subscriber::with_default(subscriber(seen.clone(), "debug"), || {
        let events = Arc::new(Events::new(config::LOG_RECORDS));
        let guard = FormatterGuard::start(events.clone()).unwrap();
        let producers = thread::scope(|scope| {
            ["binance", "aster"]
                .map(|venue| {
                    let events = &events;
                    scope.spawn(move || {
                        let id = thread::current().id();
                        events.feed(venue, FeedEvent::Started(id));
                        events.feed(venue, FeedEvent::Status(true));
                        for i in 0..100 {
                            events.feed(
                                venue,
                                FeedEvent::State {
                                    state: FeedState::Live,
                                    last_diff_id: Some(i),
                                    exchange_time: Some(1700000000000 + i),
                                },
                            );
                        }
                        events.feed(venue, FeedEvent::Status(false));
                        id
                    })
                })
                .map(|handle| handle.join().unwrap())
        });
        drop(guard);
        let rows = seen.lock().unwrap();
        assert_eq!(rows.len(), 206);
        assert!(
            rows.iter()
                .all(|(id, time, _)| !producers.contains(id) && time.is_some())
        );
        for (venue, producer) in ["binance", "aster"].into_iter().zip(producers) {
            let lines: Vec<_> = rows
                .iter()
                .map(|(_, _, s)| s)
                .filter(|s| s.contains(&format!("venue=\"{venue}\"")))
                .collect();
            assert!(lines[0].contains(&format!("thread_id={producer:?}")));
            assert!(lines[1].starts_with("\x1b[44;97mINFO:"));
            for (i, line) in lines[2..102].iter().enumerate() {
                assert!(line.contains(&format!(
                    "state=Live last_diff_id={i} exchange_time={}",
                    1700000000000_u64 + i as u64
                )));
            }
            assert!(lines[102].starts_with("\x1b[41;97mINFO:"));
            assert!(lines[102].contains("instrument Offline"));
        }
    });
}
