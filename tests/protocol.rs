use hft_test_task::{
    aster, binance,
    book::OrderBook,
    book::{self, BookStatus},
    config,
    md::{DepthDiff, ProtocolError, ReceiveTime, Snapshot},
    numeric::InstrumentRules,
    robot::{self, Bias},
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

struct Protocol {
    diff: fn(&[u8], ReceiveTime, InstrumentRules) -> Result<DepthDiff, ProtocolError>,
    snapshot: fn(&[u8], InstrumentRules) -> Result<Snapshot, ProtocolError>,
    metadata: fn(&[u8]) -> Result<InstrumentRules, ProtocolError>,
    diff_fixture: &'static [u8],
    snapshot_fixture: &'static [u8],
    metadata_fixture: &'static [u8],
    invalid_fixture: &'static [u8],
}
fn protocols() -> [Protocol; 2] {
    [
        Protocol {
            diff: binance::decode_diff,
            snapshot: binance::decode_snapshot,
            metadata: binance::decode_metadata,
            diff_fixture: include_bytes!("fixtures/binance/diff.json"),
            snapshot_fixture: include_bytes!("fixtures/binance/snapshot.json"),
            metadata_fixture: include_bytes!("fixtures/binance/metadata.json"),
            invalid_fixture: include_bytes!("fixtures/binance/invalid_precision.json"),
        },
        Protocol {
            diff: aster::decode_diff,
            snapshot: aster::decode_snapshot,
            metadata: aster::decode_metadata,
            diff_fixture: include_bytes!("fixtures/aster/diff.json"),
            snapshot_fixture: include_bytes!("fixtures/aster/snapshot.json"),
            metadata_fixture: include_bytes!("fixtures/aster/metadata.json"),
            invalid_fixture: include_bytes!("fixtures/aster/invalid_precision.json"),
        },
    ]
}
#[test]
fn fixtures_flow_through_book_store_and_robot() {
    let now = Instant::now();
    let receive = ReceiveTime::new(1_700_000_000_010, now).unwrap();
    let mut readers = Vec::new();
    for protocol in protocols() {
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        assert_eq!(rules.tick_size().raw(), 50_000_000);
        assert_eq!(rules.lot_size().raw(), 1_000_000);
        let mut snapshot = (protocol.snapshot)(protocol.snapshot_fixture, rules).unwrap();
        let mut book = OrderBook::new(rules, config::BOOK_LEVELS).unwrap();
        book.load_snapshot(&mut snapshot).unwrap();
        assert_eq!(book.event_time(), None);
        let mut diff = (protocol.diff)(protocol.diff_fixture, receive, rules).unwrap();
        assert_eq!(diff.time.exchange_time(), 1_700_000_000_002);
        assert_eq!(diff.time.lag_ms(), 8);
        assert_eq!(
            (diff.first_id, diff.final_id, diff.previous_id),
            (100, 102, 99)
        );
        book.apply_diff(&mut diff).unwrap();
        let (mut writer, mut reader) = book::channel(config::BOOK_LEVELS).unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
        reader.update();
        assert_eq!(reader.view().received_at(), Some(now));
        readers.push(reader);
    }
    let decision = robot::evaluate(
        readers[0].view(),
        readers[1].view(),
        &Default::default(),
        now + Duration::from_millis(1),
    );
    assert_eq!(decision.bias, Bias::LongBias);
    assert!(decision.edge_bps.unwrap().numerator < 0);
}
#[test]
fn corrupted_diff_fields_and_bounds_are_rejected() {
    let receive = ReceiveTime::new(1_700_000_000_010, Instant::now()).unwrap();
    for protocol in protocols() {
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        assert!((protocol.diff)(protocol.invalid_fixture, receive, rules).is_err());
        let base: Value = serde_json::from_slice(protocol.diff_fixture).unwrap();
        for (field, value) in [
            ("s", json!("BTCUSDT")),
            ("e", json!("bookTicker")),
            ("U", json!(103)),
            ("pu", json!(102)),
            ("u", json!(-1)),
            ("E", json!("1700000000002")),
            ("E", json!(u64::MAX)),
            ("T", json!(-1)),
            ("b", json!([["3500.01", "1"]])),
            ("b", json!([["3500", "0.0001"]])),
            ("b", json!([["3500", "1"], ["3500", "2"]])),
            ("a", json!([["3501", "-1"]])),
            ("b", json!([[3500, "1"]])),
            ("b", json!([["3500", "1", "extra"]])),
        ] {
            let mut malformed = base.clone();
            malformed["data"][field] = value;
            assert!(
                (protocol.diff)(&serde_json::to_vec(&malformed).unwrap(), receive, rules).is_err(),
                "field={field}"
            );
        }
        for field in ["E", "T", "U", "u", "pu", "b", "a", "e", "s"] {
            let mut malformed = base.clone();
            malformed["data"].as_object_mut().unwrap().remove(field);
            assert!(
                (protocol.diff)(&serde_json::to_vec(&malformed).unwrap(), receive, rules).is_err(),
                "missing={field}"
            );
        }
        let mut malformed = base.clone();
        malformed["stream"] = json!("btcusdt@depth@100ms");
        assert!((protocol.diff)(&serde_json::to_vec(&malformed).unwrap(), receive, rules).is_err());
        assert!(matches!(
            (protocol.diff)(&vec![b' '; config::MESSAGE_BYTES + 1], receive, rules),
            Err(ProtocolError::BodyTooLarge)
        ));
        let mut oversized = base.clone();
        oversized["data"]["b"] = (0..config::DIFF_LEVELS + 1)
            .map(|i| json!([(i + 1).to_string(), "1"]))
            .collect();
        assert!((protocol.diff)(&serde_json::to_vec(&oversized).unwrap(), receive, rules).is_err());
        oversized["data"]["b"].as_array_mut().unwrap().pop();
        assert_eq!(
            (protocol.diff)(&serde_json::to_vec(&oversized).unwrap(), receive, rules)
                .unwrap()
                .bids
                .len(),
            config::DIFF_LEVELS
        );
        let mut trailing = protocol.diff_fixture.to_vec();
        trailing.extend_from_slice(b"{}");
        assert!((protocol.diff)(&trailing, receive, rules).is_err());
    }
}
#[test]
fn snapshot_limits_and_metadata_validation() {
    for protocol in protocols() {
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        let base: Value = serde_json::from_slice(protocol.snapshot_fixture).unwrap();
        for (field, value) in [
            ("lastUpdateId", json!(-1)),
            ("E", json!(u64::MAX)),
            ("asks", json!([["4000", "0"]])),
            ("bids", json!([["3500", "1"], ["3500", "1"]])),
        ] {
            let mut bad = base.clone();
            bad[field] = value;
            assert!((protocol.snapshot)(&serde_json::to_vec(&bad).unwrap(), rules).is_err());
        }
        let mut huge = base;
        huge["bids"] = (0..config::SNAPSHOT_LEVELS + 1)
            .map(|i| json!([(i + 1).to_string(), "1"]))
            .collect();
        assert!((protocol.snapshot)(&serde_json::to_vec(&huge).unwrap(), rules).is_err());
        huge["bids"].as_array_mut().unwrap().pop();
        assert_eq!(
            (protocol.snapshot)(&serde_json::to_vec(&huge).unwrap(), rules)
                .unwrap()
                .bids
                .len(),
            config::SNAPSHOT_LEVELS
        );
        assert!(matches!(
            (protocol.snapshot)(&vec![b' '; config::MESSAGE_BYTES + 1], rules),
            Err(ProtocolError::BodyTooLarge)
        ));
        let metadata: Value = serde_json::from_slice(protocol.metadata_fixture).unwrap();
        for (field, value) in [
            ("symbol", json!("BTCUSDT")),
            ("status", json!("HALT")),
            ("contractType", json!("CURRENT_QUARTER")),
            ("baseAsset", json!("BTC")),
            ("quoteAsset", json!("USDC")),
            ("marginAsset", json!("ETH")),
            ("contractSize", json!("2")),
            ("filters", json!([])),
            (
                "filters",
                json!([{"filterType":"PRICE_FILTER","tickSize":"0.0000000001"}]),
            ),
        ] {
            let mut bad = metadata.clone();
            bad["symbols"][0][field] = value;
            assert!(
                (protocol.metadata)(&serde_json::to_vec(&bad).unwrap()).is_err(),
                "field={field}"
            );
        }
        let mut duplicate = metadata.clone();
        duplicate["symbols"]
            .as_array_mut()
            .unwrap()
            .push(metadata["symbols"][0].clone());
        assert!((protocol.metadata)(&serde_json::to_vec(&duplicate).unwrap()).is_err());
        let mut bad = metadata.clone();
        bad["symbols"][0]["filters"][1]["stepSize"] = json!("0");
        assert!((protocol.metadata)(&serde_json::to_vec(&bad).unwrap()).is_err());
        assert!(matches!(
            (protocol.metadata)(&vec![b' '; config::METADATA_BYTES + 1]),
            Err(ProtocolError::BodyTooLarge)
        ));
    }
}
#[test]
fn binance_rejects_coin_margin_and_timestamps_keep_signed_lag() {
    let protocol = protocols().into_iter().next().unwrap();
    let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
    let now = Instant::now();
    let receive = ReceiveTime::new(1_700_000_000_000, now).unwrap();
    assert_eq!(
        (protocol.diff)(protocol.diff_fixture, receive, rules)
            .unwrap()
            .time
            .lag_ms(),
        -2
    );
    for (field, value) in [("st", json!(2)), ("ps", json!("ETHUSD"))] {
        let mut bad: Value = serde_json::from_slice(protocol.diff_fixture).unwrap();
        bad["data"][field] = value;
        assert!(matches!(
            (protocol.diff)(&serde_json::to_vec(&bad).unwrap(), receive, rules),
            Err(ProtocolError::WrongStream)
        ));
    }
    assert!(ReceiveTime::new(u64::MAX, now).is_err());
}

#[test]
fn unrelated_instruments_do_not_change_units_and_metadata_arrays_are_bounded() {
    for protocol in protocols() {
        let base: Value = serde_json::from_slice(protocol.metadata_fixture).unwrap();
        let mut mixed = base.clone();
        mixed["symbols"].as_array_mut().unwrap().push(json!({"symbol":"BTCUSD_PERP","contractSize":100,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0"}]}));
        assert!((protocol.metadata)(&serde_json::to_vec(&mixed).unwrap()).is_ok());
        mixed["futuresType"] = json!("COIN_MARGINED");
        assert!((protocol.metadata)(&serde_json::to_vec(&mixed).unwrap()).is_err());
        let mut too_many = base.clone();
        too_many["symbols"] = (0..4097).map(|_| json!({"symbol":"OTHER"})).collect();
        assert!((protocol.metadata)(&serde_json::to_vec(&too_many).unwrap()).is_err());
        let mut filters = base.clone();
        filters["symbols"][0]["filters"] = (0..33).map(|_| json!({"filterType":"OTHER"})).collect();
        assert!((protocol.metadata)(&serde_json::to_vec(&filters).unwrap()).is_err());
        let mut duplicate = base.clone();
        duplicate["symbols"][0]["filters"]
            .as_array_mut()
            .unwrap()
            .push(base["symbols"][0]["filters"][0].clone());
        assert!((protocol.metadata)(&serde_json::to_vec(&duplicate).unwrap()).is_err());
    }
}
