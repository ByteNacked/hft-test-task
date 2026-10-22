//! Decimal fixed point: raw / 10^9. No operation silently rounds.
use std::{fmt, str::FromStr};

use fixnum::{
    FixedPoint,
    ops::{One, Zero},
    typenum::U9,
};
use serde::{Deserialize, Deserializer, de};
use thiserror::Error;

pub type Fp = FixedPoint<i64, U9>;
pub const SCALE: i64 = *Fp::ONE.as_bits();
pub const MAX_NUMBER_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NumericError {
    #[error("expected unsigned ASCII decimal, at most 64 bytes")]
    InvalidDecimal,
    #[error("exact fixed-point representation would lose precision")]
    PrecisionLoss,
    #[error("numeric overflow")]
    Overflow,
    #[error("value outside the domain")]
    Domain,
    #[error("value is not a multiple of the instrument step")]
    InvalidStep,
}

#[cold]
fn precision_loss(operation: &'static str) -> NumericError {
    tracing::error!(operation, "fixed-point precision loss; value rejected");
    NumericError::PrecisionLoss
}

fn parse_exact(value: &str) -> Result<Fp, NumericError> {
    if value.is_empty() || value.len() > MAX_NUMBER_LEN {
        return Err(NumericError::InvalidDecimal);
    }
    let mut raw = 0_i64;
    let mut decimal = false;
    let mut fractional_digits = 0;
    let mut lost_precision = false;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'0'..=b'9' => {
                if decimal && fractional_digits == Fp::PRECISION as u32 {
                    lost_precision |= byte != b'0';
                } else {
                    raw = raw
                        .checked_mul(10)
                        .and_then(|n| n.checked_add(i64::from(byte - b'0')))
                        .ok_or(NumericError::Overflow)?;
                    fractional_digits += u32::from(decimal);
                }
            }
            b'.' if !decimal && index > 0 && index + 1 < value.len() => decimal = true,
            _ => return Err(NumericError::InvalidDecimal),
        }
    }
    if lost_precision {
        return Err(precision_loss("parse"));
    }
    raw.checked_mul(10_i64.pow(Fp::PRECISION as u32 - fractional_digits))
        .map(Fp::from_bits)
        .ok_or(NumericError::Overflow)
}

macro_rules! quantity {
    ($name:ident, $positive:expr) => {
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(Fp);

        impl $name {
            pub fn from_raw(raw: i64) -> Result<Self, NumericError> {
                if raw < 0 || ($positive && raw == 0) {
                    return Err(NumericError::Domain);
                }
                Ok(Self(Fp::from_bits(raw)))
            }
            pub fn raw(self) -> i64 {
                self.0.into_bits()
            }
            pub fn check_step(self, step: Self) -> Result<(), NumericError> {
                if step.raw() <= 0 || self.raw() % step.raw() != 0 {
                    Err(NumericError::InvalidStep)
                } else {
                    Ok(())
                }
            }
        }
        impl FromStr for $name {
            type Err = NumericError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::from_raw(parse_exact(value)?.into_bits())
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct Visitor;
                impl de::Visitor<'_> for Visitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("an unsigned exact decimal string")
                    }
                    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        value.parse().map_err(E::custom)
                    }
                }
                d.deserialize_str(Visitor)
            }
        }
    };
}
quantity!(Price, true);
quantity!(Qty, false);
impl Qty {
    pub const ZERO: Self = Self(Fp::ZERO);
    pub const ONE: Self = Self(Fp::ONE);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentRules {
    tick_size: Price,
    lot_size: Qty,
}
impl InstrumentRules {
    pub fn new(tick_size: Price, lot_size: Qty) -> Result<Self, NumericError> {
        if lot_size == Qty::ZERO {
            return Err(NumericError::InvalidStep);
        }
        Ok(Self {
            tick_size,
            lot_size,
        })
    }
    pub fn tick_size(self) -> Price {
        self.tick_size
    }
    pub fn validate(self, price: Price, qty: Qty) -> Result<(), NumericError> {
        price.check_step(self.tick_size)?;
        self.validate_qty(qty)
    }
    pub(crate) fn validate_qty(self, qty: Qty) -> Result<(), NumericError> {
        qty.check_step(self.lot_size)
    }
}
