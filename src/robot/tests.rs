use crate::test_support::*;
use crate::{
    book::{self, BookStatus},
    config::RobotConfig,
    numeric::SCALE,
    robot::{Bias, evaluate},
};
use std::time::{Duration, Instant};

#[test]
fn directions_threshold_equality_and_exact_explanation() {
    let now = Instant::now();
    let b = view(&book(now, 20_000, 200_000_000));
    for (mid, bias) in [
        (19_995, Bias::LongBiasAster),
        (19_996, Bias::Hold),
        (20_000, Bias::Hold),
        (20_004, Bias::Hold),
        (20_005, Bias::ShortBiasAster),
    ] {
        let a = view(&book(now, mid, 200_000_000));
        let d = evaluate(b.view(), a.view(), &Default::default(), now);
        assert_eq!(d.bias, bias);
        assert_eq!(d.binance.mid2_raw, Some(40_000));
        assert_eq!(d.binance.bid_qty_raw, Some(i128::from(SCALE)));
    }

    // Half-tick midpoint: truncating it to Price would incorrectly produce Hold.
    let mut snapshot = snapshot(20_000, SCALE, 6);
    for ask in &mut snapshot.asks {
        ask.price = price(ask.price.raw() + 1);
    }
    let mut binance = book(now, 20_000, SCALE);
    binance.load_snapshot(&snapshot).unwrap();
    binance.apply_diff(&diff(now)).unwrap();
    let b = view(&binance);
    let a = view(&book(now, 19_996, SCALE));
    let d = evaluate(b.view(), a.view(), &Default::default(), now);
    assert_eq!(d.binance.mid2_raw, Some(40_001));
    assert_eq!(d.bias, Bias::LongBiasAster);
    assert!(d.binance.to_string().contains("mid=0.0000200005"));

    // Valid prices at opposite domain extremes must not overflow intermediate sums/products.
    for (binance, aster, bias) in [
        (7, i64::MAX - 10, Bias::ShortBiasAster),
        (i64::MAX - 10, 7, Bias::LongBiasAster),
    ] {
        let b = view(&book(now, binance, i64::MAX));
        let a = view(&book(now, aster, i64::MAX));
        assert_eq!(
            evaluate(b.view(), a.view(), &Default::default(), now).bias,
            bias
        );
    }
}
#[test]
fn freshness_skew_and_volume_boundaries() {
    let now = Instant::now();
    let config = RobotConfig::default();
    let b = view(&book(now, 100_000, SCALE));
    let a = view(&book(now, 99_000, SCALE));
    assert_eq!(
        evaluate(b.view(), a.view(), &config, now + config.max_age()).bias,
        Bias::LongBiasAster
    );
    assert_eq!(
        evaluate(
            b.view(),
            a.view(),
            &config,
            now + config.max_age() + Duration::from_nanos(1)
        )
        .bias,
        Bias::Hold
    );
    for (skew, bias) in [
        (config.max_skew(), Bias::LongBiasAster),
        (config.max_skew() + Duration::from_nanos(1), Bias::Hold),
    ] {
        let a = view(&book(now + skew, 99_000, SCALE));
        assert_eq!(evaluate(b.view(), a.view(), &config, now + skew).bias, bias);
    }
    let a = view(&book(now, 99_000, 199_999_999));
    assert_eq!(evaluate(b.view(), a.view(), &config, now).bias, Bias::Hold);
    let a = view(&book(now, 99_000, i64::MAX));
    assert_eq!(
        evaluate(b.view(), a.view(), &config, now).aster.bid_qty_raw,
        Some(5 * i128::from(i64::MAX))
    );
}
#[test]
fn latest_recovery_cannot_skip_hold_after_offline() {
    for fault_on_binance in [false, true] {
        let now = Instant::now();
        let binance = book(now, 100_000, SCALE);
        let aster = book(now, 99_000, SCALE);
        let (mut bw, br) = {
            static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            book::channel(&EPOCH)
        };
        let (mut aw, ar) = {
            static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            book::channel(&EPOCH)
        };
        bw.publish(&binance, BookStatus::Online).unwrap();
        aw.publish(&aster, BookStatus::Online).unwrap();
        let mut robot = super::Robot::new(br, ar, RobotConfig::default());
        assert_eq!(robot.poll().unwrap().bias, Bias::LongBiasAster);
        let (writer, book) = if fault_on_binance {
            (&mut bw, &binance)
        } else {
            (&mut aw, &aster)
        };
        writer.publish(book, BookStatus::Online).unwrap();
        writer.publish(book, BookStatus::Offline).unwrap();
        writer.publish(book, BookStatus::Online).unwrap();
        // Only the newest book remains, but Offline must be evaluated first.
        assert_eq!(robot.poll().unwrap().bias, Bias::Hold);
        assert_eq!(robot.poll().unwrap().bias, Bias::LongBiasAster);
    }
}

#[test]
fn idle_books_expire_without_waiting_for_the_periodic_report() {
    let config =
        RobotConfig::new(Duration::from_millis(100), Duration::ZERO, qty(SCALE), 2).unwrap();
    let mut binance = book(Instant::now(), 100_000, SCALE);
    let mut aster = book(Instant::now(), 99_000, SCALE);
    // Stamp after allocation so setup time does not consume the freshness budget.
    let received = Instant::now();
    binance.apply_diff(&diff(received)).unwrap();
    aster.apply_diff(&diff(received)).unwrap();
    let mut robot = super::Robot::new(view(&binance), view(&aster), config);
    assert_eq!(robot.poll().unwrap().bias, Bias::LongBiasAster);
    std::thread::sleep(config.max_age() + Duration::from_millis(10));
    // Keep the report deadline in the future even if the test thread was descheduled.
    robot.next_report = Some(Instant::now() + crate::config::REPORT_INTERVAL);
    let decision = robot
        .poll()
        .expect("freshness deadline must trigger a decision");
    assert_eq!(decision.bias, Bias::Hold);
    assert_eq!(decision.binance.status, BookStatus::Online);
    assert_eq!(decision.aster.status, BookStatus::Online);
}
