//! Sequence validation is independent of transport and never refreshes duplicate data.
use crate::{
    book::OrderBook,
    md::{AdapterError, DepthDiff, Snapshot},
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
    last: Box<DepthDiff>,
    ready: bool,
}
impl SyncedBook {
    pub(crate) fn new() -> Result<Self, AdapterError> {
        Ok(Self {
            book: OrderBook::new(crate::config::BOOK_LEVELS)?,
            snapshot_id: None,
            last: Box::default(),
            ready: false,
        })
    }
    pub(crate) fn reset(&mut self) {
        self.snapshot_id = None;
        self.ready = false;
    }
    pub(crate) fn load(&mut self, snapshot: &Snapshot) -> Result<(), AdapterError> {
        self.book.load_snapshot(snapshot)?;
        self.snapshot_id = Some(snapshot.last_update_id);
        self.ready = false;
        Ok(())
    }
    pub(crate) fn ready(&self) -> bool {
        self.ready
    }
    pub(crate) fn has_snapshot(&self) -> bool {
        self.snapshot_id.is_some()
    }
    pub(crate) fn apply(&mut self, diff: &mut Box<DepthDiff>) -> Result<Applied, AdapterError> {
        if self.ready {
            let last = &self.last;
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
        self.book.apply_diff(diff)?;
        // Swap owners, not the 128 KiB payloads. The previous diff becomes the
        // next decode workspace; errors and duplicates retain the accepted diff.
        std::mem::swap(&mut self.last, diff);
        self.ready = true;
        Ok(Applied::Update)
    }
}

#[cfg(test)]
mod tests;
