use crate::{
    book::{BookReader, BookStatus, BookView},
    config::{REPORT_INTERVAL, ROBOT_LEVELS, RobotConfig},
    numeric::{Price, SCALE},
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
    LongBiasAster,
    ShortBiasAster,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookNumbers {
    pub status: BookStatus,
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    /// Midpoint = mid2_raw / (2 * 10^9).
    pub mid2_raw: Option<i128>,
    /// Each volume is raw ETH / 10^9; None when fewer than five levels exist.
    pub bid_qty_raw: Option<i128>,
    pub ask_qty_raw: Option<i128>,
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
            // At most five nonnegative i64 quantities fit comfortably in i128.
            Some(
                side[..ROBOT_LEVELS]
                    .iter()
                    .map(|l| i128::from(l.qty.raw()))
                    .sum(),
            )
        };
        Self {
            status: view.status,
            best_bid: bid,
            best_ask: ask,
            mid2_raw: bid
                .zip(ask)
                .map(|(b, a)| i128::from(b.raw()) + i128::from(a.raw())),
            bid_qty_raw: sum(&view.bids),
            ask_qty_raw: sum(&view.asks),
            age: view
                .platform_time
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
        skew: binance
            .platform_time
            .zip(aster.platform_time)
            .map(|(a, b)| {
                if a >= b {
                    a.duration_since(b)
                } else {
                    b.duration_since(a)
                }
            }),
        threshold_bps: config.threshold_bps(),
    };
    if binance.status != BookStatus::Online || aster.status != BookStatus::Online {
        return decision;
    }
    let usable = |book: &BookNumbers| {
        book.age.is_some_and(|age| age <= config.max_age())
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
    if let Some((mid_b, mid_a)) = decision.binance.mid2_raw.zip(decision.aster.mid2_raw) {
        // Prices are positive i64. Sums of two prices and multiplication by <= 10_000
        // fit i128; compare cross-products exactly, without rounding or division.
        let edge = (mid_a - mid_b) * 10_000;
        let threshold = mid_b * i128::from(config.threshold_bps());
        decision.bias = if edge < -threshold {
            Bias::LongBiasAster
        } else if edge > threshold {
            Bias::ShortBiasAster
        } else {
            Bias::Hold
        };
    }
    decision
}

impl std::fmt::Display for BookNumbers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.status)?;
        if let Some(bid) = self.best_bid {
            write!(f, " best_bid={bid}")?;
        }
        if let Some(ask) = self.best_ask {
            write!(f, " best_ask={ask}")?;
        }
        if let Some(mid) = self.mid2_raw {
            // A midpoint may need ten decimal places, even though prices have nine.
            let scale = 2 * i128::from(SCALE);
            write!(f, " mid={}.{:010}", mid / scale, (mid % scale) * 5)?;
        }
        if let Some(age) = self.age {
            write!(f, " age_ms={}", age.as_millis())?;
        }
        if let Some((bid, ask)) = self.bid_qty_raw.zip(self.ask_qty_raw) {
            let scale = i128::from(SCALE);
            write!(
                f,
                " depth={}.{:09}/{}.{:09}",
                bid / scale,
                bid % scale,
                ask / scale,
                ask % scale
            )?;
        }
        Ok(())
    }
}

/// Polls independently published books; freshness also expires without new messages.
pub struct Robot {
    binance: BookReader,
    aster: BookReader,
    config: RobotConfig,
    next_freshness: Option<Instant>,
    next_report: Option<Instant>,
    last: Option<Decision>,
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
        }
    }

    fn poll(&mut self) -> Option<Decision> {
        let binance_changed = self.binance.update();
        let aster_changed = self.aster.update(); // Deliberately not short-circuited.
        // Sample AFTER acquiring both views. Otherwise a concurrent publication can
        // carry platform_time later than our sampled now and spuriously look invalid.
        let now = Instant::now();
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
        // Freshness is inclusive at max_age: first stale instant is one nanosecond later.
        self.next_freshness = [self.binance.view(), self.aster.view()]
            .into_iter()
            .filter(|view| view.status == BookStatus::Online)
            .filter_map(|view| {
                view.platform_time?
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
        mut report: impl FnMut(&Decision) + Send + 'static,
    ) -> std::io::Result<JoinHandle<()>> {
        thread::Builder::new().name("robot".into()).spawn(move || {
            // Relaxed: this flag carries no data; triple_buffer synchronizes book ownership.
            while !stop.load(Ordering::Relaxed) {
                if let Some(decision) = self.poll() {
                    report(&decision);
                } else {
                    std::hint::spin_loop();
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
