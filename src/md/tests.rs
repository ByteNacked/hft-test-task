use crate::{
    aster::AsterAdapter,
    binance::BinanceAdapter,
    book::OrderBook,
    book::{self, BookStatus},
    config,
    md::{DepthDiff, MDAdapter, ProtocolError, ReceiveTime, Snapshot},
    numeric::InstrumentRules,
    robot::{self, Bias},
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

struct Protocol {
    diff: fn(&[u8], ReceiveTime, InstrumentRules, &mut DepthDiff) -> Result<(), ProtocolError>,
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
            diff: BinanceAdapter::decode_diff_into,
            snapshot: BinanceAdapter::decode_snapshot,
            metadata: BinanceAdapter::decode_metadata,
            diff_fixture: include_bytes!("../../tests/fixtures/binance/diff.json"),
            snapshot_fixture: include_bytes!("../../tests/fixtures/binance/snapshot.json"),
            metadata_fixture: include_bytes!("../../tests/fixtures/binance/metadata.json"),
            invalid_fixture: include_bytes!("../../tests/fixtures/binance/invalid_precision.json"),
        },
        Protocol {
            diff: AsterAdapter::decode_diff_into,
            snapshot: AsterAdapter::decode_snapshot,
            metadata: AsterAdapter::decode_metadata,
            diff_fixture: include_bytes!("../../tests/fixtures/aster/diff.json"),
            snapshot_fixture: include_bytes!("../../tests/fixtures/aster/snapshot.json"),
            metadata_fixture: include_bytes!("../../tests/fixtures/aster/metadata.json"),
            invalid_fixture: include_bytes!("../../tests/fixtures/aster/invalid_precision.json"),
        },
    ]
}
#[test]
fn fixtures_flow_through_book_and_robot() {
    let now = Instant::now();
    let receive = now;
    let mut readers = Vec::new();
    for protocol in protocols() {
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        let snapshot = (protocol.snapshot)(protocol.snapshot_fixture, rules).unwrap();
        let mut book = OrderBook::new(config::BOOK_LEVELS).unwrap();
        book.load_snapshot(&snapshot).unwrap();
        assert_eq!(book.event_time(), None);
        let mut diff = Box::<DepthDiff>::default();
        (protocol.diff)(protocol.diff_fixture, receive, rules, &mut diff).unwrap();
        assert_eq!(diff.time.exchange_time(), 1_700_000_000_002);
        assert_eq!(
            (diff.first_id, diff.final_id, diff.previous_id),
            (100, 102, 99)
        );
        book.apply_diff(&diff).unwrap();
        let (mut writer, mut reader) = {
            static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            book::channel(config::PUBLISHED_LEVELS, &EPOCH)
        }
        .unwrap();
        writer.publish(&book, BookStatus::Online).unwrap();
        reader.update();
        assert_eq!(reader.view().platform_time, Some(now));
        readers.push(reader);
    }
    let decision = robot::evaluate(
        readers[0].view(),
        readers[1].view(),
        &Default::default(),
        now + Duration::from_millis(1),
    );
    assert_eq!(decision.bias, Bias::LongBiasAster);
}
#[test]
fn corrupted_diff_fields_and_bounds_are_rejected() {
    let receive = Instant::now();
    for protocol in protocols() {
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        let mut diff = Box::<DepthDiff>::default();
        assert!((protocol.diff)(protocol.invalid_fixture, receive, rules, &mut diff).is_err());
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
                (protocol.diff)(
                    &serde_json::to_vec(&malformed).unwrap(),
                    receive,
                    rules,
                    &mut diff
                )
                .is_err(),
                "field={field}"
            );
        }
        for field in ["E", "T", "U", "u", "pu", "b", "a", "e", "s"] {
            let mut malformed = base.clone();
            malformed["data"].as_object_mut().unwrap().remove(field);
            assert!(
                (protocol.diff)(
                    &serde_json::to_vec(&malformed).unwrap(),
                    receive,
                    rules,
                    &mut diff
                )
                .is_err(),
                "missing={field}"
            );
        }
        let mut malformed = base.clone();
        malformed["stream"] = json!("btcusdt@depth@100ms");
        assert!(
            (protocol.diff)(
                &serde_json::to_vec(&malformed).unwrap(),
                receive,
                rules,
                &mut diff
            )
            .is_err()
        );
        assert!(matches!(
            (protocol.diff)(
                &vec![b' '; config::MESSAGE_BYTES + 1],
                receive,
                rules,
                &mut diff
            ),
            Err(ProtocolError::BodyTooLarge)
        ));
        let mut oversized = base.clone();
        oversized["data"]["b"] = (0..config::DIFF_LEVELS + 1)
            .map(|i| json!([(i + 1).to_string(), "1"]))
            .collect();
        assert!(
            (protocol.diff)(
                &serde_json::to_vec(&oversized).unwrap(),
                receive,
                rules,
                &mut diff
            )
            .is_err()
        );
        oversized["data"]["b"].as_array_mut().unwrap().pop();
        (protocol.diff)(
            &serde_json::to_vec(&oversized).unwrap(),
            receive,
            rules,
            &mut diff,
        )
        .unwrap();
        assert_eq!(diff.bids.len(), config::DIFF_LEVELS);
        let mut trailing = protocol.diff_fixture.to_vec();
        trailing.extend_from_slice(b"{}");
        assert!((protocol.diff)(&trailing, receive, rules, &mut diff).is_err());
        for field in ["u", "b", "data"] {
            let value = if field == "data" {
                &base[field]
            } else {
                &base["data"][field]
            };
            let member = format!("\"{field}\":{value}");
            let duplicate = base
                .to_string()
                .replacen(&member, &format!("{member},{member}"), 1);
            assert!((protocol.diff)(duplicate.as_bytes(), receive, rules, &mut diff).is_err());
        }
    }
}

#[test]
fn reused_diff_buffers_preserve_last_update_on_errors_and_clear_old_levels() {
    use crate::md::sync::{Applied, SyncedBook};
    for protocol in protocols() {
        let now = Instant::now();
        let rules = (protocol.metadata)(protocol.metadata_fixture).unwrap();
        let mut synced = SyncedBook::new().unwrap();
        synced
            .load(&(protocol.snapshot)(protocol.snapshot_fixture, rules).unwrap())
            .unwrap();
        let mut incoming = Box::<DepthDiff>::default();
        (protocol.diff)(protocol.diff_fixture, now, rules, &mut incoming).unwrap();
        assert_eq!(synced.apply(&mut incoming).unwrap(), Applied::Update);

        let mut next: Value = serde_json::from_slice(protocol.diff_fixture).unwrap();
        for (id, side, quantity) in [(103, "a", "0.600"), (104, "b", "0.900")] {
            let mut bad = next.clone();
            bad["data"]["a"] = json!([["3501", "invalid"]]);
            assert!(
                (protocol.diff)(
                    &serde_json::to_vec(&bad).unwrap(),
                    now,
                    rules,
                    &mut incoming
                )
                .is_err()
            );
            // Partial decode must not overwrite the accepted diff used for duplicates.
            (protocol.diff)(
                &serde_json::to_vec(&next).unwrap(),
                now,
                rules,
                &mut incoming,
            )
            .unwrap();
            assert_eq!(synced.apply(&mut incoming).unwrap(), Applied::Duplicate);

            let mut update: Value = serde_json::from_slice(protocol.diff_fixture).unwrap();
            update["data"]["U"] = json!(id);
            update["data"]["u"] = json!(id);
            update["data"]["pu"] = json!(id - 1);
            update["data"][side][0][1] = json!(quantity);
            update["data"][if side == "a" { "b" } else { "a" }] = json!([]);
            (protocol.diff)(
                &serde_json::to_vec(&update).unwrap(),
                now,
                rules,
                &mut incoming,
            )
            .unwrap();
            assert_eq!(synced.apply(&mut incoming).unwrap(), Applied::Update);
            next = update;
        }
        // The empty side of each update must not replay levels from a reused buffer.
        assert_eq!(synced.book.bids()[0].qty.raw(), 900_000_000);
        assert_eq!(synced.book.asks()[0].qty.raw(), 600_000_000);
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
