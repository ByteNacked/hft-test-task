use hft_test_task::numeric::*;
use proptest::prelude::*;
use rust_decimal::Decimal;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

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
        "9223372036.854775807".parse::<Amount>().unwrap().raw(),
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
fn exact_operations_and_steps() {
    let price: Price = "0.05".parse().unwrap();
    assert!("0.15".parse::<Price>().unwrap().check_step(price).is_ok());
    assert_eq!(
        "0.16".parse::<Price>().unwrap().check_step(price),
        Err(NumericError::InvalidStep)
    );
    assert!(InstrumentRules::new(price, Qty::ZERO).is_err());
    let amount = "3500.10"
        .parse::<Price>()
        .unwrap()
        .checked_mul_qty("0.002".parse().unwrap())
        .unwrap();
    assert_eq!(amount, "7.0002".parse().unwrap());
    assert_eq!(
        amount
            .checked_div_price("3500.10".parse().unwrap())
            .unwrap(),
        "0.002".parse().unwrap()
    );
    assert_eq!(
        Price::from_raw(1)
            .unwrap()
            .checked_mul_qty(Qty::from_raw(1).unwrap()),
        Err(NumericError::PrecisionLoss)
    );
    assert_eq!(
        Amount::ONE.checked_div_price("3".parse().unwrap()),
        Err(NumericError::PrecisionLoss)
    );
    assert_eq!(
        Qty::from_raw(i64::MAX).unwrap().checked_add(Qty::ONE),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        Amount::ZERO.checked_sub(Amount::ONE),
        Err(NumericError::Domain)
    );
    assert_eq!(
        Amount::from_raw(i64::MAX).unwrap().checked_add(Amount::ONE),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        Price::from_raw(i64::MAX)
            .unwrap()
            .checked_mul_qty(Qty::from_raw(2 * SCALE).unwrap()),
        Err(NumericError::Overflow)
    );
    assert_eq!(Qty::ONE.checked_mul_integer(-1), Err(NumericError::Domain));
    assert_eq!(
        exact_quotient(i128::MAX, 1, "test"),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        exact_quotient(1, 0, "test"),
        Err(NumericError::DivisionByZero)
    );
}

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[test]
fn precision_loss_is_logged_for_input_and_arithmetic() {
    let captured = Capture(Arc::default());
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        assert!("0.0000000001".parse::<Price>().is_err());
        assert!(
            Price::from_raw(1)
                .unwrap()
                .checked_mul_qty(Qty::from_raw(1).unwrap())
                .is_err()
        );
        assert!(Amount::ONE.checked_div_price("3".parse().unwrap()).is_err());
    });
    let bytes = captured.0.lock().unwrap();
    let log = std::str::from_utf8(&bytes).unwrap();
    assert_eq!(log.matches("ERROR").count(), 3);
    for operation in ["parse", "price * qty", "amount / price"] {
        assert!(log.contains(operation));
    }
}
proptest! {
    #[test]
    fn agrees_with_decimal(raw in 0..=i64::MAX) {
        let reference = Decimal::from_i128_with_scale(i128::from(raw), 9);
        let parsed: Qty = reference.to_string().parse().unwrap();
        prop_assert_eq!(parsed.raw(), raw);
        prop_assert_eq!(Decimal::from_str(&parsed.to_string()).unwrap(), reference);
    }
    #[test]
    fn exact_product_matches_reference(p in 1_i64..1_000_000, q in 0_i64..1_000_000) {
        let price = Price::from_raw(p * SCALE).unwrap();
        let qty = Qty::from_raw(q).unwrap();
        let result = price.checked_mul_qty(qty).unwrap();
        prop_assert_eq!(Decimal::from_i128_with_scale(i128::from(result.raw()), 9), Decimal::from(p) * Decimal::from_i128_with_scale(i128::from(q), 9));
    }
}
