use super::*;
use crate::{
    config::RobotConfig,
    logging::TextFormat,
    robot,
    test_support::{book, view},
};
use std::{io::Write, sync::Mutex, time::Instant};

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
    let binance = view(&book(
        now,
        3500 * crate::numeric::SCALE,
        crate::numeric::SCALE,
    ));
    let aster_mid = match bias {
        Bias::LongBiasAster => 3499,
        Bias::ShortBiasAster => 3501,
        Bias::Hold => 3500,
    };
    let aster = view(&book(
        now,
        aster_mid * crate::numeric::SCALE,
        crate::numeric::SCALE,
    ));
    let mut d = robot::evaluate(
        binance.view(),
        aster.view(),
        &RobotConfig::default(),
        Instant::now(),
    );
    d.binance.best_bid = Some("3499.01".parse().unwrap());
    d
}

#[test]
fn bps_explanation_distinguishes_exact_threshold_from_a_crossing() {
    let now = Instant::now();
    let mid = 100 * crate::numeric::SCALE;
    let binance = view(&book(now, mid, crate::numeric::SCALE));
    let threshold = mid * 2 / 10_000;
    for (offset, bias, expected) in [
        (-threshold - 1, Bias::LongBiasAster, "-2.000000…/2"),
        (-threshold, Bias::Hold, "-2.000000/2"),
        (-threshold + 1, Bias::Hold, "-1.999999…/2"),
        (threshold - 1, Bias::Hold, "+1.999999…/2"),
        (threshold, Bias::Hold, "+2.000000/2"),
        (threshold + 1, Bias::ShortBiasAster, "+2.000000…/2"),
    ] {
        let aster = view(&book(now, mid + offset, crate::numeric::SCALE));
        let d = robot::evaluate(binance.view(), aster.view(), &RobotConfig::default(), now);
        assert_eq!(d.bias, bias);
        assert_eq!(Bps(&d).to_string(), expected);
    }
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
            thread::scope(|scope| {
                for venue in ["binance", "aster"] {
                    let events = &events;
                    scope.spawn(move || {
                        events.feed(venue, FeedEvent::Status(true));
                        events.feed(
                            venue,
                            FeedEvent::State {
                                state: FeedState::Live,
                                last_diff_id: Some(42),
                                exchange_time: Some(1700000000000),
                            },
                        );
                        events.feed(venue, FeedEvent::Status(false));
                    });
                }
            });
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
            assert_eq!(
                text.matches("last_diff_id=42").count(),
                if level == "debug" { 2 } else { 0 }
            );
            if level != "warn" {
                assert_eq!(text.matches("instrument Online").count(), 2);
                assert_eq!(text.matches("instrument Offline").count(), 2);
                assert!(text.contains("best_bid=3499.01"), "{text}");
                assert!(text.contains("LongBiasAster"));
                assert!(text.contains("ShortBiasAster"));
                assert!(text.contains("bps=-2.857142…/2"), "{text}");
                assert!(text.contains("bps=+2.857142…/2"), "{text}");
            }
        });
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
    let seen = Seen::default();
    tracing::subscriber::with_default(subscriber(seen.clone(), "error"), || drain(&events));
    let rows = seen.lock().unwrap();
    let text = rows.iter().map(|(_, _, s)| s.as_str()).collect::<String>();
    assert!(
        text.contains("ERROR:") && text.contains("dropped=2"),
        "{text}"
    );
    assert_eq!(events.dropped.load(Ordering::Relaxed), 0);
}
