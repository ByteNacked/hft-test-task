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
    let b = view(&book(now, 20_000, 200_000_000, 16));
    for (mid, bias) in [
        (19_989, Bias::LongBiasAster),
        (19_990, Bias::Hold),
        (20_000, Bias::Hold),
        (20_010, Bias::Hold),
        (20_011, Bias::ShortBiasAster),
    ] {
        let a = view(&book(now, mid, 200_000_000, 16));
        let d = evaluate(b.view(), a.view(), &Default::default(), now);
        assert_eq!(d.bias, bias);
        assert_eq!(d.binance.mid2_raw, Some(40_000));
        assert_eq!(d.binance.bid_qty_raw, Some(i128::from(SCALE)));
    }
}
#[test]
fn freshness_skew_and_volume_boundaries() {
    let now = Instant::now();
    let config = RobotConfig::default();
    let b = view(&book(now, 100_000, SCALE, 16));
    let a = view(&book(now, 99_000, SCALE, 16));
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
        let a = view(&book(now + skew, 99_000, SCALE, 16));
        assert_eq!(evaluate(b.view(), a.view(), &config, now + skew).bias, bias);
    }
    let a = view(&book(now, 99_000, 199_999_999, 16));
    assert_eq!(evaluate(b.view(), a.view(), &config, now).bias, Bias::Hold);
    let a = view(&book(now, 99_000, i64::MAX, 16));
    assert_eq!(
        evaluate(b.view(), a.view(), &config, now).aster.bid_qty_raw,
        Some(5 * i128::from(i64::MAX))
    );
}
#[test]
fn either_offline_book_blocks_bias() {
    let now = Instant::now();
    let book = book(now, 100, SCALE, 16);
    let b = view(&book);
    let (mut writer, mut reader) = {
        static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        book::channel(16, &EPOCH)
    }
    .unwrap();
    writer.publish(&book, BookStatus::Offline).unwrap();
    reader.update();
    assert_eq!(
        evaluate(b.view(), reader.view(), &Default::default(), now).bias,
        Bias::Hold
    );
    assert_eq!(
        evaluate(reader.view(), b.view(), &Default::default(), now).bias,
        Bias::Hold
    );
}

#[test]
fn latest_recovery_cannot_skip_hold_after_offline() {
    for fault_on_binance in [false, true] {
        let now = Instant::now();
        let binance = book(now, 100_000, SCALE, 16);
        let aster = book(now, 99_000, SCALE, 16);
        let (mut bw, br) = {
            static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            book::channel(16, &EPOCH)
        }
        .unwrap();
        let (mut aw, ar) = {
            static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            book::channel(16, &EPOCH)
        }
        .unwrap();
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
