use hft_test_task::numeric::*;

#[test]
fn exact_ascii_parser_and_domain() {
    for invalid in [
        "", "-1", "+1", " 1", "1 ", "1e2", ".1", "1.", "1.2.3", "١", "NaN", "1\n",
    ] {
        assert!(invalid.parse::<Qty>().is_err(), "{invalid:?}");
    }
    assert!("1".repeat(65).parse::<Qty>().is_err());
    assert_eq!("0".parse::<Price>(), Err(NumericError::Domain));
    assert_eq!("0".parse::<Qty>().unwrap(), Qty::ZERO);
    assert_eq!(
        "1.230000000000000000".parse::<Price>().unwrap().raw(),
        1_230_000_000
    );
    assert_eq!("1.000000000000".parse::<Price>().unwrap().raw(), SCALE);
    assert_eq!(
        "0.0000000001".parse::<Qty>(),
        Err(NumericError::PrecisionLoss)
    );
    assert_eq!(
        "9223372036.854775807".parse::<Qty>().unwrap().raw(),
        i64::MAX
    );
    assert_eq!(
        "9223372036.854775808".parse::<Qty>(),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        serde_json::from_str::<Qty>("\"0.001\"").unwrap().raw(),
        1_000_000
    );
    assert!(serde_json::from_str::<Qty>("0.001").is_err());
    assert!(Qty::from_raw(-1).is_err());
}
#[test]
fn instrument_steps_are_exact() {
    let rules = InstrumentRules::new("0.05".parse().unwrap(), "0.001".parse().unwrap()).unwrap();
    assert!(
        rules
            .validate("2500.15".parse().unwrap(), "0.003".parse().unwrap())
            .is_ok()
    );
    assert!(
        rules
            .validate("2500.16".parse().unwrap(), Qty::ONE)
            .is_err()
    );
    assert!(
        rules
            .validate("2500.15".parse().unwrap(), "0.0001".parse().unwrap())
            .is_err()
    );
    assert!(InstrumentRules::new("0.05".parse().unwrap(), Qty::ZERO).is_err());
}
