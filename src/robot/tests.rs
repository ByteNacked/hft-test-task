use crate::test_support::*;
use crate::{
    book::{self, BookStatus},
    config::RobotConfig,
    numeric::SCALE,
    robot::{Bias, Robot, evaluate},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

#[test]
fn directions_threshold_equality_and_exact_explanation() {
    let now = Instant::now();
    let b = view(&book(now, 20_000, 200_000_000, 16));
    for (mid, bias) in [
        (19_989, Bias::LongBias),
        (19_990, Bias::Hold),
        (20_000, Bias::Hold),
        (20_010, Bias::Hold),
        (20_011, Bias::ShortBias),
    ] {
        let a = view(&book(now, mid, 200_000_000, 16));
        let d = evaluate(b.view(), a.view(), &Default::default(), now);
        assert_eq!(d.bias, bias);
        assert_eq!(d.binance.mid2_raw, Some(40_000));
        assert_eq!(
            d.edge_bps.unwrap().numerator,
            10_000 * i128::from(2 * mid - 40_000)
        );
        assert_eq!(d.edge_bps.unwrap().denominator, 40_000);
        assert_eq!(d.binance.bid_qty_raw, Some(i128::from(SCALE)));
        assert_eq!(d.binance.exchange_time, Some(1_700_000_000_000));
        assert_eq!(d.binance.platform_time, Some(1_700_000_000_010));
    }
}
#[test]
fn freshness_skew_and_volume_boundaries() {
    let now = Instant::now();
    let config = RobotConfig::default();
    let b = view(&book(now, 100_000, SCALE, 16));
    let a = view(&book(now, 100_000, SCALE, 16));
    assert!(
        evaluate(b.view(), a.view(), &config, now + config.max_age())
            .edge_bps
            .is_some()
    );
    let stale = evaluate(
        b.view(),
        a.view(),
        &config,
        now + config.max_age() + Duration::from_nanos(1),
    );
    assert_eq!(stale.bias, Bias::Hold);
    assert_eq!(stale.edge_bps, None);
    let a = view(&book(now + config.max_skew(), 100_000, SCALE, 16));
    assert!(
        evaluate(b.view(), a.view(), &config, now + config.max_skew())
            .edge_bps
            .is_some()
    );
    let a = view(&book(
        now + config.max_skew() + Duration::from_nanos(1),
        100_000,
        SCALE,
        16,
    ));
    assert!(
        evaluate(b.view(), a.view(), &config, now + config.max_age())
            .edge_bps
            .is_none()
    );
    let a = view(&book(now, 100_000, 199_999_999, 16));
    assert!(
        evaluate(b.view(), a.view(), &config, now)
            .edge_bps
            .is_none()
    );
    // Quantities can exceed i64 in aggregate; i128 avoids false overflow.
    let a = view(&book(now, 100_000, i64::MAX, 16));
    assert_eq!(
        evaluate(b.view(), a.view(), &config, now).aster.bid_qty_raw,
        Some(5 * i128::from(i64::MAX))
    );
}
#[test]
fn offline_prices_remain_diagnostics_only() {
    let now = Instant::now();
    let book = book(now, 100, SCALE, 16);
    let b = view(&book);
    let (mut writer, mut reader) = book::channel(16).unwrap();
    writer.publish(&book, BookStatus::Offline).unwrap();
    reader.update();
    let d = evaluate(b.view(), reader.view(), &Default::default(), now);
    assert_eq!(d.bias, Bias::Hold);
    assert_eq!(d.aster.status, BookStatus::Offline);
    assert!(d.aster.bid.is_some());
    assert_eq!(d.edge_bps, None);
}
#[test]
fn clock_steps_do_not_affect_monotonic_age_or_skew() {
    let now = Instant::now();
    let mut a = book(now, 100, SCALE, 16);
    let b = view(&a);
    let mut change = diff(now);
    change.time =
        crate::md::EventTime::new(1, crate::md::ReceiveTime::new(200, now).unwrap()).unwrap();
    a.apply_diff(&mut change).unwrap();
    let a = view(&a);
    let d = evaluate(
        b.view(),
        a.view(),
        &Default::default(),
        now + Duration::from_millis(100),
    );
    assert!(d.edge_bps.is_some());
    assert_eq!(d.skew, Some(Duration::ZERO));
    assert_eq!(d.aster.age, Some(Duration::from_millis(100)));
    assert_eq!(d.aster.exchange_time, Some(1));
    assert_eq!(d.aster.platform_time, Some(200));
}
#[test]
fn polling_checks_both_buffers_and_handles_stale_without_messages() {
    let now = Instant::now();
    let aster_book = book(now, 99_000, SCALE, 16);
    let book = book(now, 100_000, SCALE, 16);
    let (mut bw, br) = book::channel(16).unwrap();
    let (mut aw, ar) = book::channel(16).unwrap();
    let mut robot = Robot::new(br, ar, Default::default());
    assert_eq!(robot.poll(now).unwrap().aster.status, BookStatus::Offline);
    bw.publish(&book, BookStatus::Online).unwrap();
    aw.publish(&aster_book, BookStatus::Online).unwrap();
    assert!(robot.poll(now).unwrap().edge_bps.is_some());
    assert_eq!(robot.last_decision().unwrap().binance.sequence, 1);
    assert_eq!(robot.last_decision().unwrap().aster.sequence, 1);
    for _ in 0..100 {
        assert_eq!(robot.poll(now), None);
    }
    assert_eq!(robot.evaluations(), 2);
    assert_eq!(robot.poll(now + Duration::from_millis(500)), None);
    assert_eq!(
        robot
            .poll(now + Duration::from_millis(500) + Duration::from_nanos(1))
            .unwrap()
            .bias,
        Bias::Hold
    );
    for _ in 0..100 {
        assert_eq!(robot.poll(now + Duration::from_millis(700)), None);
    }
    assert_eq!(robot.evaluations(), 3);
    // A status-only update is visible without a new diff and cannot refresh time.
    aw.publish(&aster_book, BookStatus::Offline).unwrap();
    assert_eq!(
        robot
            .poll(now + Duration::from_millis(800))
            .unwrap()
            .aster
            .status,
        BookStatus::Offline
    );
    assert_eq!(
        robot.last_decision().unwrap().aster.age,
        Some(Duration::from_millis(800))
    );
    assert_eq!(robot.poll(now + Duration::from_millis(1799)), None);
    assert_eq!(
        robot
            .poll(now + Duration::from_millis(1800))
            .unwrap()
            .aster
            .status,
        BookStatus::Offline
    );
    // A late output emits once, without catching up every missed reporting deadline.
    assert!(robot.poll(now + Duration::from_secs(10)).is_some());
    assert_eq!(robot.poll(now + Duration::from_secs(10)), None);
}
#[test]
fn updates_arriving_during_callback_are_seen_next_iteration() {
    let now = Instant::now();
    let book = book(now, 100, SCALE, 16);
    let (mut bw, br) = book::channel(16).unwrap();
    let (mut aw, ar) = book::channel(16).unwrap();
    let robot = Robot::new(br, ar, Default::default());
    let stop = Arc::new(AtomicBool::new(false));
    let callback_stop = stop.clone();
    let (send, recv) = mpsc::channel();
    // Callback executes after the calculation while the worker cannot poll.
    let handle = robot.spawn(stop, move |decision| {
        send.send(decision.aster.status).unwrap();
        if decision.aster.status == BookStatus::Offline {
            bw.publish(&book, BookStatus::Online).unwrap();
            aw.publish(&book, BookStatus::Online).unwrap();
        } else {
            callback_stop.store(true, Ordering::Relaxed);
        }
    });
    assert_eq!(
        recv.recv_timeout(Duration::from_secs(2)).unwrap(),
        BookStatus::Offline
    );
    assert_eq!(
        recv.recv_timeout(Duration::from_secs(2)).unwrap(),
        BookStatus::Online
    );
    handle.join().unwrap();
}
#[test]
fn empty_loop_observes_stop_without_publications() {
    let (_, br) = book::channel(16).unwrap();
    let (_, ar) = book::channel(16).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (send, recv) = mpsc::channel();
    let handle = Robot::new(br, ar, Default::default()).spawn(stop.clone(), move |_| {
        let _ = send.send(());
    });
    recv.recv_timeout(Duration::from_secs(2)).unwrap();
    stop.store(true, Ordering::Relaxed);
    let (done, finished) = mpsc::channel();
    let joiner = std::thread::spawn(move || {
        handle.join().unwrap();
        done.send(()).unwrap();
    });
    finished.recv_timeout(Duration::from_secs(2)).unwrap();
    joiner.join().unwrap();
}
