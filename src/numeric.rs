//! Decimal fixed point: raw / 10^9. No operation silently rounds.
use std::{fmt, str::FromStr};

use fixnum::{FixedPoint, typenum::U9};
use serde::{Deserialize, Deserializer, de};
use thiserror::Error;

pub type Fp = FixedPoint<i64, U9>;
pub const SCALE: i64 = 1_000_000_000;
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

fn precision_loss(operation: &'static str) -> NumericError {
    tracing::error!(operation, "fixed-point precision loss; value rejected");
    NumericError::PrecisionLoss
}

fn parse_exact(value: &str) -> Result<Fp, NumericError> {
    if value.is_empty() || value.len() > MAX_NUMBER_LEN {
        return Err(NumericError::InvalidDecimal);
    }
    let mut dot = None;
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'.' && dot.is_none() && index > 0 && index + 1 < value.len() {
            dot = Some(index);
        } else if !byte.is_ascii_digit() {
            return Err(NumericError::InvalidDecimal);
        }
    }
    let trimmed = if let Some(index) = dot {
        let end = value.trim_end_matches('0').len();
        if end - index - 1 > 9 {
            return Err(precision_loss("parse"));
        }
        &value[..if end == index + 1 { index } else { end }]
    } else {
        value
    };
    Fp::from_str_exact(trimmed).map_err(|_| NumericError::Overflow)
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
    pub const ZERO: Self = Self(Fp::from_bits(0));
    pub const ONE: Self = Self(Fp::from_bits(SCALE));
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
    pub fn validate(self, price: Price, qty: Qty) -> Result<(), NumericError> {
        price.check_step(self.tick_size)?;
        qty.check_step(self.lot_size)
    }
}
