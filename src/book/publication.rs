//! SPSC latest-state publication. Only a validated writer can populate a view.
use crate::{
    book::{BookError, Coverage, Level, OrderBook},
    config::{BOOK_LEVELS, ROBOT_LEVELS},
};
use std::time::Instant;
use triple_buffer::{Input, Output, TripleBuffer};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum BookStatus {
    #[default]
    Offline,
    Online,
}

#[derive(Debug)]
pub struct BookView {
    pub(crate) bids: Vec<Level>,
    pub(crate) asks: Vec<Level>,
    pub(crate) coverage: Option<Coverage>,
    pub(crate) sequence: u64,
    pub(crate) status: BookStatus,
    pub(crate) exchange_time: Option<u64>,
    pub(crate) platform_time: Option<u64>,
    pub(crate) received_at: Option<Instant>,
    capacity: usize,
}
impl BookView {
    fn empty(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
            coverage: None,
            sequence: 0,
            status: BookStatus::Offline,
            exchange_time: None,
            platform_time: None,
            received_at: None,
            capacity,
        }
    }
    pub fn bids(&self) -> &[Level] {
        &self.bids
    }
    pub fn asks(&self) -> &[Level] {
        &self.asks
    }
    pub fn coverage(&self) -> Option<Coverage> {
        self.coverage
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn status(&self) -> BookStatus {
        self.status
    }
    pub fn exchange_time(&self) -> Option<u64> {
        self.exchange_time
    }
    pub fn platform_time(&self) -> Option<u64> {
        self.platform_time
    }
    pub fn received_at(&self) -> Option<Instant> {
        self.received_at
    }

    #[cfg(test)]
    pub(crate) fn allocated_capacities(&self) -> [usize; 2] {
        [self.bids.capacity(), self.asks.capacity()]
    }
}
impl Clone for BookView {
    fn clone(&self) -> Self {
        // Construction only. Vec::clone on an empty Vec would discard spare capacity.
        let mut cloned = Self::empty(self.capacity);
        cloned.bids.extend_from_slice(&self.bids);
        cloned.asks.extend_from_slice(&self.asks);
        cloned.coverage = self.coverage;
        cloned.sequence = self.sequence;
        cloned.status = self.status;
        cloned.exchange_time = self.exchange_time;
        cloned.platform_time = self.platform_time;
        cloned.received_at = self.received_at;
        cloned
    }
}
pub struct BookPublisher {
    input: Input<BookView>,
    sequence: u64,
    capacity: usize,
}
pub struct BookReader {
    output: Output<BookView>,
    skipped_publications: u64,
}
pub fn channel(capacity: usize) -> Result<(BookPublisher, BookReader), BookError> {
    if !(ROBOT_LEVELS..=BOOK_LEVELS).contains(&capacity) {
        return Err(BookError::Capacity);
    }
    let (input, output) = TripleBuffer::new(&BookView::empty(capacity)).split();
    Ok((
        BookPublisher {
            input,
            sequence: 0,
            capacity,
        },
        BookReader {
            output,
            skipped_publications: 0,
        },
    ))
}
impl BookPublisher {
    /// Copies the complete state, including status-only changes; timestamps come from
    /// the last committed diff, never from publication time. No allocations or awaits.
    pub fn publish(&mut self, book: &OrderBook, status: BookStatus) -> Result<u64, BookError> {
        if book.bids().len() > self.capacity || book.asks().len() > self.capacity {
            return Err(BookError::Capacity);
        }
        if status == BookStatus::Online
            && (book.event_time().is_none()
                || !book
                    .coverage()
                    .is_some_and(|c| c.is_sufficient(book.bids(), book.asks())))
        {
            return Err(BookError::NotReady);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(BookError::SequenceOverflow)?;
        let slot = self.input.input_buffer_mut();
        slot.bids.clear();
        slot.asks.clear();
        slot.bids.extend_from_slice(book.bids());
        slot.asks.extend_from_slice(book.asks());
        slot.coverage = book.coverage();
        slot.sequence = sequence;
        slot.status = status;
        slot.exchange_time = book.event_time().map(|t| t.exchange_time());
        slot.platform_time = book.event_time().map(|t| t.platform_time());
        slot.received_at = book.event_time().map(|t| t.received_at());
        self.input.publish();
        self.sequence = sequence;
        Ok(sequence)
    }
}
impl BookReader {
    pub fn update(&mut self) -> bool {
        let previous = self.view().sequence();
        let changed = self.output.update();
        if changed {
            self.skipped_publications = self.skipped_publications.saturating_add(
                self.view()
                    .sequence()
                    .saturating_sub(previous)
                    .saturating_sub(1),
            );
        }
        changed
    }
    pub fn skipped_publications(&self) -> u64 {
        self.skipped_publications
    }
    /// Stable until the next explicit update; this call never implicitly consumes a slot.
    pub fn view(&self) -> &BookView {
        self.output.output_buffer()
    }
}

#[cfg(test)]
mod tests;
