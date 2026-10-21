//! Shared combined-stream schema. Seeds write levels into reusable diff storage;
//! RawValue would allocate serde_json scratch for nested arrays on every message.
use super::{DepthDiff, EventTime, LevelsSeed, ProtocolError, ReceiveTime, normalize_diff};
use crate::{config, numeric::InstrumentRules};
use serde::{
    Deserialize,
    de::{DeserializeSeed, Error, IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

#[derive(Deserialize)]
#[serde(field_identifier)]
enum Field {
    #[serde(rename = "stream")]
    Stream,
    #[serde(rename = "data")]
    Data,
    #[serde(rename = "e")]
    Event,
    #[serde(rename = "s")]
    Symbol,
    #[serde(rename = "E")]
    ExchangeTime,
    #[serde(rename = "T")]
    TransactionTime,
    #[serde(rename = "U")]
    First,
    #[serde(rename = "u")]
    Final,
    #[serde(rename = "pu")]
    Previous,
    #[serde(rename = "b")]
    Bids,
    #[serde(rename = "a")]
    Asks,
    #[serde(rename = "st")]
    SymbolType,
    #[serde(rename = "ps")]
    Pair,
    #[serde(other)]
    Ignore,
}

struct Header<'a> {
    event: &'a str,
    symbol: &'a str,
    exchange_time: u64,
    transaction_time: u64,
    symbol_type: Option<u8>,
    pair: Option<&'a str>,
}

// Keep duplicate-field checks identical to derived Deserialize, including null options.
macro_rules! read_field {
    ($map:ident, $field:ident, $name:literal) => {{
        if $field.is_some() {
            return Err(A::Error::duplicate_field($name));
        }
        $field = Some($map.next_value()?);
    }};
}

struct DataSeed<'a, const BINANCE: bool>(&'a mut DepthDiff);
impl<'de, const BINANCE: bool> DeserializeSeed<'de> for DataSeed<'_, BINANCE> {
    type Value = Header<'de>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de, const BINANCE: bool> Visitor<'de> for DataSeed<'_, BINANCE> {
    type Value = Header<'de>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("depth update")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (mut event, mut symbol, mut exchange_time, mut transaction_time) =
            (None, None, None, None);
        let (mut first, mut final_id, mut previous) = (None, None, None);
        let (mut bids, mut asks) = (false, false);
        let mut symbol_type: Option<Option<u8>> = None;
        let mut pair: Option<Option<&str>> = None;
        while let Some(field) = map.next_key()? {
            match field {
                Field::Event => read_field!(map, event, "e"),
                Field::Symbol => read_field!(map, symbol, "s"),
                Field::ExchangeTime => read_field!(map, exchange_time, "E"),
                Field::TransactionTime => read_field!(map, transaction_time, "T"),
                Field::First => read_field!(map, first, "U"),
                Field::Final => read_field!(map, final_id, "u"),
                Field::Previous => read_field!(map, previous, "pu"),
                Field::SymbolType if BINANCE => read_field!(map, symbol_type, "st"),
                Field::Pair if BINANCE => read_field!(map, pair, "ps"),
                Field::Bids => {
                    if bids {
                        return Err(A::Error::duplicate_field("b"));
                    }
                    map.next_value_seed(LevelsSeed(&mut self.0.bids))?;
                    bids = true;
                }
                Field::Asks => {
                    if asks {
                        return Err(A::Error::duplicate_field("a"));
                    }
                    map.next_value_seed(LevelsSeed(&mut self.0.asks))?;
                    asks = true;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !bids {
            return Err(A::Error::missing_field("b"));
        }
        if !asks {
            return Err(A::Error::missing_field("a"));
        }
        self.0.first_id = first.ok_or_else(|| A::Error::missing_field("U"))?;
        self.0.final_id = final_id.ok_or_else(|| A::Error::missing_field("u"))?;
        self.0.previous_id = previous.ok_or_else(|| A::Error::missing_field("pu"))?;
        Ok(Header {
            event: event.ok_or_else(|| A::Error::missing_field("e"))?,
            symbol: symbol.ok_or_else(|| A::Error::missing_field("s"))?,
            exchange_time: exchange_time.ok_or_else(|| A::Error::missing_field("E"))?,
            transaction_time: transaction_time.ok_or_else(|| A::Error::missing_field("T"))?,
            symbol_type: symbol_type.flatten(),
            pair: pair.flatten(),
        })
    }
}

struct WrapperSeed<'a, const BINANCE: bool>(&'a mut DepthDiff);
impl<'de, const BINANCE: bool> DeserializeSeed<'de> for WrapperSeed<'_, BINANCE> {
    type Value = (&'de str, Header<'de>);
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de, const BINANCE: bool> Visitor<'de> for WrapperSeed<'_, BINANCE> {
    type Value = (&'de str, Header<'de>);
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("combined stream message")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (mut stream, mut data) = (None, None);
        while let Some(field) = map.next_key()? {
            match field {
                Field::Stream => read_field!(map, stream, "stream"),
                Field::Data => {
                    if data.is_some() {
                        return Err(A::Error::duplicate_field("data"));
                    }
                    data = Some(map.next_value_seed(DataSeed::<BINANCE>(self.0))?);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok((
            stream.ok_or_else(|| A::Error::missing_field("stream"))?,
            data.ok_or_else(|| A::Error::missing_field("data"))?,
        ))
    }
}

pub(crate) fn decode_into<const BINANCE: bool>(
    body: &[u8],
    received: ReceiveTime,
    rules: InstrumentRules,
    out: &mut DepthDiff,
) -> Result<(), ProtocolError> {
    out.bids.clear();
    out.asks.clear();
    super::check_body(body, config::MESSAGE_BYTES)?;
    let mut decoder = serde_json::Deserializer::from_slice(body);
    let (stream, header) = WrapperSeed::<BINANCE>(out).deserialize(&mut decoder)?;
    decoder.end()?;
    if stream != config::STREAM
        || header.event != "depthUpdate"
        || header.symbol != config::SYMBOL
        || header.symbol_type.is_some_and(|st| st != 1)
        || header.pair.is_some_and(|ps| ps != config::SYMBOL)
    {
        return Err(ProtocolError::WrongStream);
    }
    super::validate_timestamp(header.transaction_time)?;
    out.time = EventTime::new(header.exchange_time, received)?;
    normalize_diff(out, rules)
}
