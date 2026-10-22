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
    assert_eq!(
        format!("0.{}", "0".repeat(62)).parse::<Qty>(),
        Ok(Qty::ZERO)
    );
    assert_eq!("9".repeat(64).parse::<Qty>(), Err(NumericError::Overflow));
    assert_eq!(
        "0009223372036.854775807000".parse::<Qty>().unwrap().raw(),
        i64::MAX
    );
    assert_eq!(
        "1.230000000000000001".parse::<Qty>(),
        Err(NumericError::PrecisionLoss)
    );
    assert_eq!("9223372037".parse::<Qty>(), Err(NumericError::Overflow));
}
proptest::proptest! {
    #[test]
    fn exact_decimal_preserves_raw_values_and_rejects_overflow(
        raw in proptest::prelude::any::<u64>(), leading in 0_usize..16, trailing in 0_usize..16
    ) {
        let text = format!("{}{}.{:09}{}", "0".repeat(leading), raw / SCALE as u64,
            raw % SCALE as u64, "0".repeat(trailing));
        let expected = i64::try_from(raw).map_err(|_| NumericError::Overflow);
        proptest::prop_assert_eq!(text.parse::<Qty>().map(Qty::raw), expected);
    }
}
