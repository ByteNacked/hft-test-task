use crate::{
    book::BookStatus,
    config,
    md::feed::FeedState,
    robot::{Bias, Decision},
};
use crossbeam_queue::ArrayQueue;
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle, ThreadId},
    time::SystemTime,
};

pub(crate) enum FeedEvent {
    Started(ThreadId),
    Connected,
    State {
        state: FeedState,
        last_diff_id: Option<u64>,
        exchange_time: Option<u64>,
    },
    Status(bool),
}

// Keep the decision inline: boxing the large variant would allocate on the hot path.
#[allow(clippy::large_enum_variant)]
enum Event {
    Decision {
        decision: Decision,
        changed: bool,
    },
    Feed {
        venue: &'static str,
        event: FeedEvent,
    },
}
struct Record {
    time: SystemTime,
    event: Event,
}

struct Events {
    queue: ArrayQueue<Record>,
    dropped: AtomicU64,
    closed: AtomicBool,
    robot_info: bool,
    robot_debug: bool,
    feed_info: bool,
    feed_debug: bool,
}
impl Events {
    fn new(capacity: usize) -> Self {
        // Filters are fixed at startup; no subscriber callbacks on producer hot paths.
        Self {
            queue: ArrayQueue::new(capacity),
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            robot_info: tracing::enabled!(target: "hft_test_task::logging", tracing::Level::INFO),
            robot_debug: tracing::enabled!(target: "hft_test_task::logging", tracing::Level::DEBUG),
            feed_info: tracing::enabled!(target: "hft_test_task::md::feed", tracing::Level::INFO),
            feed_debug: tracing::enabled!(target: "hft_test_task::md::feed", tracing::Level::DEBUG),
        }
    }
    fn push(&self, event: Event) -> bool {
        // This flag only rejects late logs; ArrayQueue synchronizes the records themselves.
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        if self
            .queue
            .push(Record {
                time: SystemTime::now(),
                event,
            })
            .is_ok()
        {
            return true;
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
        false
    }
    fn feed(&self, venue: &'static str, event: FeedEvent) {
        if match event {
            FeedEvent::Status(_) => self.feed_info,
            _ => self.feed_debug,
        } {
            self.push(Event::Feed { venue, event });
        }
    }
    fn report_dropped(&self) {
        // Independent diagnostic count; no data is published through it.
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        if dropped != 0 {
            tracing::error!(target: "hft_test_task::logging", dropped, "log event queue full; records dropped");
        }
    }
}

// One logger per process; set before starting the robot or adapters. All producers share
// a bounded MPMC queue, used as MPSC, because Tokio feed tasks may migrate between workers.
static EVENTS: OnceLock<Arc<Events>> = OnceLock::new();

pub(crate) fn feed(venue: &'static str, event: FeedEvent) {
    if let Some(events) = EVENTS.get() {
        events.feed(venue, event);
    }
}

fn reporter(events: Arc<Events>) -> impl FnMut(&Decision) + Send + 'static {
    let mut previous = Bias::Hold;
    move |decision: &Decision| {
        let changed = decision.bias != previous;
        if (events.robot_info && changed || events.robot_debug)
            && events.push(Event::Decision {
                decision: *decision,
                changed,
            })
        {
            previous = decision.bias;
        }
    }
}

/// Copies decisions into the shared event queue. Keep LoggingGuard until service shutdown.
pub fn decision_reporter() -> std::io::Result<impl FnMut(&Decision) + Send + 'static> {
    let events = EVENTS
        .get()
        .ok_or_else(|| std::io::Error::other("logging is not initialized"))?;
    Ok(reporter(events.clone()))
}

struct Bps<'a>(&'a Decision);
impl std::fmt::Display for Bps<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = self.0;
        match d.binance.mid2_raw.zip(d.aster.mid2_raw) {
            Some((binance, aster))
                if d.binance.status == BookStatus::Online
                    && d.aster.status == BookStatus::Online =>
            {
                // Approximate display on the formatter thread; trading comparisons stay exact.
                let bps = (aster - binance) as f64 * 10_000.0 / binance as f64;
                write!(f, "{bps:+.6}/{}", d.threshold_bps)
            }
            _ => write!(f, "n/a/{}", d.threshold_bps),
        }
    }
}

fn emit(record: Record) {
    super::EVENT_TIME.set(Some(record.time));
    match record.event {
        Event::Decision {
            decision: d,
            changed,
        } => {
            if changed {
                tracing::info!(name: "bias_changed", target: "hft_test_task::logging", bias=?d.bias,
                    binance=%d.binance, aster=%d.aster, bps=%Bps(&d), "bias changed");
            }
            tracing::debug!(target: "hft_test_task::logging", binance=%d.binance, aster=%d.aster, bias=?d.bias,
                skew_ms=d.skew.map(|s|s.as_millis() as u64), bps=%Bps(&d), "robot decision");
        }
        Event::Feed { venue, event } => match event {
            FeedEvent::Started(id) => {
                tracing::debug!(target: "hft_test_task::md::feed", venue, thread_id=?id, "feed task started")
            }
            FeedEvent::Connected => {
                tracing::debug!(target: "hft_test_task::md::feed", venue, "market-data connected")
            }
            FeedEvent::State {
                state,
                last_diff_id,
                exchange_time,
            } => {
                tracing::debug!(target: "hft_test_task::md::feed", venue, ?state, last_diff_id, exchange_time, "feed state")
            }
            FeedEvent::Status(true) => {
                tracing::info!(name: "instrument_online", target: "hft_test_task::md::feed", venue, symbol=config::SYMBOL, "instrument Online")
            }
            FeedEvent::Status(false) => {
                tracing::info!(name: "instrument_offline", target: "hft_test_task::md::feed", venue, symbol=config::SYMBOL, "instrument Offline")
            }
        },
    }
    super::EVENT_TIME.set(None);
}

fn drain(events: &Events) {
    loop {
        for _ in 0..config::LOG_RECORDS {
            let Some(record) = events.queue.pop() else {
                break;
            };
            emit(record);
        }
        events.report_dropped();
        // The owner joins all service producers before closing. Acquire observes their
        // completed enqueue/count operations before the final drain and loss report.
        if events.closed.load(Ordering::Acquire) {
            while let Some(record) = events.queue.pop() {
                emit(record);
            }
            events.report_dropped();
            return;
        }
        if events.queue.is_empty() {
            thread::sleep(config::LOG_POLL_INTERVAL);
        }
    }
}

pub(super) struct FormatterGuard {
    events: Arc<Events>,
    worker: Option<JoinHandle<()>>,
}
impl FormatterGuard {
    fn start(events: Arc<Events>) -> std::io::Result<Self> {
        let worker_events = events.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let worker = thread::Builder::new()
            .name("log-format".into())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || drain(&worker_events));
            })?;
        Ok(Self {
            events,
            worker: Some(worker),
        })
    }
}
impl Drop for FormatterGuard {
    fn drop(&mut self) {
        self.events.closed.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::error!(target: "hft_test_task::logging", "log formatter panicked");
        }
    }
}

pub(super) fn init() -> std::io::Result<FormatterGuard> {
    let events = Arc::new(Events::new(config::LOG_RECORDS));
    let guard = FormatterGuard::start(events.clone())?;
    EVENTS
        .set(events)
        .map_err(|_| std::io::Error::other("logging already initialized"))?;
    Ok(guard)
}

#[cfg(test)]
mod tests;
