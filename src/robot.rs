use crate::{
    book::{BookReader, BookStatus, BookView},
    config::{REPORT_INTERVAL, ROBOT_LEVELS, RobotConfig},
    metrics::RobotMetrics,
    numeric::{NumericError, Price},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bias {
    Hold,
    LongBias,
    ShortBias,
}
/// Exact bps = numerator / denominator. No floating point or rounded formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ratio {
    pub numerator: i128,
    pub denominator: i128,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookNumbers {
    pub status: BookStatus,
    pub sequence: u64,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    /// Midpoint = mid2_raw / (2 * 10^9).
    pub mid2_raw: Option<i128>,
    /// Each volume is raw ETH / 10^9; None when fewer than five levels exist.
    pub bid_qty_raw: Option<i128>,
    pub ask_qty_raw: Option<i128>,
    pub trusted_levels: Option<(usize, usize)>,
    pub exchange_time: Option<u64>,
    pub platform_time: Option<u64>,
    pub age: Option<Duration>,
}
impl BookNumbers {
    fn from_view(view: &BookView, now: Instant) -> Self {
        let bid = view.bids.first().map(|l| l.price);
        let ask = view.asks.first().map(|l| l.price);
        let sum = |side: &[crate::book::Level]| -> Option<i128> {
            if side.len() < ROBOT_LEVELS {
                return None;
            }
            side[..ROBOT_LEVELS]
                .iter()
                .try_fold(0_i128, |sum, l| sum.checked_add(i128::from(l.qty.raw())))
        };
        Self {
            status: view.status,
            sequence: view.sequence,
            bid,
            ask,
            mid2_raw: bid
                .zip(ask)
                .and_then(|(b, a)| i128::from(b.raw()).checked_add(i128::from(a.raw()))),
            bid_qty_raw: sum(&view.bids),
            ask_qty_raw: sum(&view.asks),
            trusted_levels: view.coverage.map(|c| c.counts(&view.bids, &view.asks)),
            exchange_time: view.exchange_time,
            platform_time: view.platform_time,
            age: view
                .received_at
                .and_then(|received| now.checked_duration_since(received)),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub bias: Bias,
    pub binance: BookNumbers,
    pub aster: BookNumbers,
    pub skew: Option<Duration>,
    pub edge_bps: Option<Ratio>,
    pub threshold_bps: u32,
}
impl Decision {
    fn key(self) -> (Bias, BookStatus, BookStatus) {
        (self.bias, self.binance.status, self.aster.status)
    }
}

pub fn evaluate(
    binance: &BookView,
    aster: &BookView,
    config: &RobotConfig,
    now: Instant,
) -> Decision {
    let mut decision = Decision {
        bias: Bias::Hold,
        binance: BookNumbers::from_view(binance, now),
        aster: BookNumbers::from_view(aster, now),
        skew: binance.received_at.zip(aster.received_at).map(|(a, b)| {
            if a >= b {
                a.duration_since(b)
            } else {
                b.duration_since(a)
            }
        }),
        edge_bps: None,
        threshold_bps: config.threshold_bps(),
    };
    if binance.status != BookStatus::Online || aster.status != BookStatus::Online {
        return decision;
    }
    let usable = |book: &BookNumbers| {
        book.exchange_time.is_some()
            && book.platform_time.is_some()
            && book.age.is_some_and(|age| age <= config.max_age())
            && book
                .trusted_levels
                .is_some_and(|(b, a)| b >= ROBOT_LEVELS && a >= ROBOT_LEVELS)
            && book
                .bid_qty_raw
                .is_some_and(|qty| qty >= i128::from(config.min_depth_qty().raw()))
            && book
                .ask_qty_raw
                .is_some_and(|qty| qty >= i128::from(config.min_depth_qty().raw()))
    };
    if !usable(&decision.binance)
        || !usable(&decision.aster)
        || !decision.skew.is_some_and(|skew| skew <= config.max_skew())
    {
        return decision;
    }
    if let Some((mid_b, mid_a)) = decision.binance.mid2_raw.zip(decision.aster.mid2_raw)
        && let Ok((bias, edge)) = compare_mid(mid_b, mid_a, i128::from(config.threshold_bps()))
    {
        decision.bias = bias;
        decision.edge_bps = Some(edge);
    }
    decision
}

fn compare_mid(binance: i128, aster: i128, threshold: i128) -> Result<(Bias, Ratio), NumericError> {
    if binance <= 0 || aster <= 0 || threshold < 0 {
        return Err(NumericError::Domain);
    }
    let numerator = aster
        .checked_sub(binance)
        .and_then(|d| d.checked_mul(10_000))
        .ok_or(NumericError::Overflow)?;
    let bound = threshold
        .checked_mul(binance)
        .ok_or(NumericError::Overflow)?;
    let negative = bound.checked_neg().ok_or(NumericError::Overflow)?;
    let bias = if numerator < negative {
        Bias::LongBias
    } else if numerator > bound {
        Bias::ShortBias
    } else {
        Bias::Hold
    };
    Ok((
        bias,
        Ratio {
            numerator,
            denominator: binance,
        },
    ))
}

/// Deterministic polling step accepts a clock value; the production loop supplies Instant::now.
pub struct Robot {
    binance: BookReader,
    aster: BookReader,
    config: RobotConfig,
    next_freshness: Option<Instant>,
    next_report: Option<Instant>,
    last: Option<Decision>,
    #[cfg(test)]
    evaluations: u64,
}
impl Robot {
    pub fn new(binance: BookReader, aster: BookReader, config: RobotConfig) -> Self {
        Self {
            binance,
            aster,
            config,
            next_freshness: None,
            next_report: None,
            last: None,
            #[cfg(test)]
            evaluations: 0,
        }
    }

    fn poll_with_clock(&mut self, clock: impl FnOnce() -> Instant) -> Option<Decision> {
        let binance_changed = self.binance.update();
        let aster_changed = self.aster.update(); // Deliberately not short-circuited.
        // Sample AFTER acquiring both views. Otherwise a concurrent publication can
        // carry received_at later than our sampled now and spuriously look invalid.
        let now = clock();
        let report_due = self.next_report.is_none_or(|deadline| now >= deadline);
        let freshness_due = self.next_freshness.is_some_and(|deadline| now >= deadline);
        if self.last.is_some()
            && !binance_changed
            && !aster_changed
            && !report_due
            && !freshness_due
        {
            return None;
        }
        let decision = evaluate(self.binance.view(), self.aster.view(), &self.config, now);
        #[cfg(test)]
        {
            self.evaluations = self.evaluations.saturating_add(1);
        }
        // Freshness is inclusive at max_age: first stale instant is one nanosecond later.
        self.next_freshness = [self.binance.view(), self.aster.view()]
            .into_iter()
            .filter(|view| view.status == BookStatus::Online)
            .filter_map(|view| {
                view.received_at?
                    .checked_add(self.config.max_age())?
                    .checked_add(Duration::from_nanos(1))
            })
            .filter(|deadline| *deadline > now)
            .min();
        let changed = self.last.is_none_or(|last| last.key() != decision.key());
        self.last = Some(decision);
        if report_due || changed {
            self.next_report = now.checked_add(REPORT_INTERVAL);
            Some(decision)
        } else {
            None
        }
    }
    /// Callback must be nonblocking (for example the bounded tracing writer).
    pub fn spawn(
        mut self,
        stop: Arc<AtomicBool>,
        mut metrics: triple_buffer::Input<RobotMetrics>,
        mut report: impl FnMut(&Decision) + Send + 'static,
    ) -> std::io::Result<JoinHandle<()>> {
        thread::Builder::new().name("robot".into()).spawn(move || {
            // Relaxed: this flag carries no data; triple_buffer synchronizes book ownership.
            while !stop.load(Ordering::Relaxed) {
                if let Some(decision) = self.poll_with_clock(Instant::now) {
                    metrics.write(self.metrics());
                    report(&decision);
                } else {
                    std::hint::spin_loop();
                }
            }
            metrics.write(self.metrics());
        })
    }

    fn metrics(&self) -> RobotMetrics {
        RobotMetrics {
            binance_skipped_publications: self.binance.skipped_publications(),
            aster_skipped_publications: self.aster.skipped_publications(),
        }
    }

    #[cfg(test)]
    pub(crate) fn evaluations(&self) -> u64 {
        self.evaluations
    }

    #[cfg(test)]
    pub(crate) fn last_decision(&self) -> Option<&Decision> {
        self.last.as_ref()
    }

    #[cfg(test)]
    /// Returns only reports: bias/book-status changes or the one-second output deadline.
    pub(crate) fn poll(&mut self, now: Instant) -> Option<Decision> {
        self.poll_with_clock(|| now)
    }
}

#[cfg(test)]
mod defensive_tests {
    use super::*;
    use crate::{
        book,
        book::{Level, OrderBook},
        md::{DepthDiff, EventTime, ReceiveTime, Snapshot},
        numeric::{InstrumentRules, Qty},
    };
    fn ready_book(now: Instant) -> OrderBook {
        let rules =
            InstrumentRules::new(Price::from_raw(1).unwrap(), Qty::from_raw(1).unwrap()).unwrap();
        let make = |p| Level {
            price: Price::from_raw(p).unwrap(),
            qty: Qty::ONE,
        };
        let mut book = OrderBook::new(rules, 8).unwrap();
        book.load_snapshot(&mut Snapshot {
            last_update_id: 1,
            bids: (94..100).map(make).collect(),
            asks: (101..107).map(make).collect(),
        })
        .unwrap();
        book.apply_diff(&mut DepthDiff {
            first_id: 1,
            final_id: 2,
            previous_id: 0,
            bids: Default::default(),
            asks: Default::default(),
            time: EventTime::new(100, ReceiveTime::new(110, now).unwrap()).unwrap(),
        })
        .unwrap();
        book
    }
    fn ready(now: Instant) -> BookView {
        let book = ready_book(now);
        let (mut writer, mut reader) = book::channel(8).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
        reader.update();
        reader.view().clone()
    }
    #[test]
    fn clock_is_sampled_after_views_are_acquired() {
        let now = Instant::now();
        let book = ready_book(now);
        let (mut bw, br) = book::channel(8).unwrap();
        let (mut aw, ar) = book::channel(8).unwrap();
        let mut robot = Robot::new(br, ar, Default::default());
        let first = robot
            .poll_with_clock(|| {
                // Publication after acquisition must wait for the next iteration.
                bw.publish(&book, BookStatus::Online).unwrap();
                aw.publish(&book, BookStatus::Online).unwrap();
                now
            })
            .unwrap();
        assert_eq!(first.aster.status, BookStatus::Offline);
        assert!(robot.poll_with_clock(|| now).unwrap().edge_bps.is_some());
    }
    #[test]
    fn missing_future_time_and_missing_coverage_fail_closed() {
        let now = Instant::now();
        let b = ready(now);
        let mut a = b.clone();
        a.exchange_time = None;
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
        a = b.clone();
        a.received_at = None;
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
        a = b.clone();
        a.platform_time = None;
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
        a = b.clone();
        a.received_at = Some(now + Duration::from_nanos(1));
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
        a = b.clone();
        a.coverage = None;
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
        a = b.clone();
        a.asks.truncate(4);
        assert!(
            evaluate(&b, &a, &Default::default(), now)
                .edge_bps
                .is_none()
        );
    }
}

#[cfg(test)]
mod tests;
