//! Sequence validation is independent of transport and never refreshes duplicate data.
use crate::{
    book::OrderBook,
    md::{AdapterError, DepthDiff, Snapshot},
    numeric::InstrumentRules,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Applied {
    Update,
    Duplicate,
    BeforeSnapshot,
}

pub(crate) struct SyncedBook {
    pub(crate) book: OrderBook,
    snapshot_id: Option<u64>,
    last: Option<DepthDiff>,
}
impl SyncedBook {
    pub(crate) fn new(rules: InstrumentRules) -> Result<Self, AdapterError> {
        Ok(Self {
            book: OrderBook::new(rules, crate::config::BOOK_LEVELS)?,
            snapshot_id: None,
            last: None,
        })
    }
    pub(crate) fn reset(&mut self) {
        self.snapshot_id = None;
        self.last = None;
    }
    pub(crate) fn load(&mut self, snapshot: &mut Snapshot) -> Result<(), AdapterError> {
        self.book.load_snapshot(snapshot)?;
        self.snapshot_id = Some(snapshot.last_update_id);
        self.last = None;
        Ok(())
    }
    pub(crate) fn ready(&self) -> bool {
        self.last.is_some()
    }
    pub(crate) fn has_snapshot(&self) -> bool {
        self.snapshot_id.is_some()
    }
    pub(crate) fn apply(&mut self, mut diff: DepthDiff) -> Result<Applied, AdapterError> {
        if let Some(last) = &self.last {
            if diff.final_id == last.final_id {
                // Local receive timestamps differ on retransmission. Exchange time and
                // normalized levels/IDs must match exactly; otherwise this is a conflict.
                if diff.first_id == last.first_id
                    && diff.previous_id == last.previous_id
                    && diff.time.exchange_time() == last.time.exchange_time()
                    && diff.bids == last.bids
                    && diff.asks == last.asks
                {
                    return Ok(Applied::Duplicate);
                }
                return Err(AdapterError::Sequence("conflicting duplicate"));
            }
            if diff.final_id < last.final_id {
                return Err(AdapterError::Sequence("regression"));
            }
            if diff.previous_id != last.final_id {
                return Err(AdapterError::Sequence("pu does not match previous u"));
            }
        } else {
            let snapshot = self
                .snapshot_id
                .ok_or(AdapterError::Sequence("snapshot required"))?;
            if diff.final_id < snapshot {
                return Ok(Applied::BeforeSnapshot);
            }
            if diff.first_id > snapshot {
                return Err(AdapterError::Sequence("snapshot bridge missing"));
            }
        }
        self.book.apply_diff(&mut diff)?;
        self.last = Some(diff);
        Ok(Applied::Update)
    }
}

#[cfg(test)]
mod tests;
