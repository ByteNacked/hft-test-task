//! Latest book view plus an independent, persistent generation of feed gaps.
use crate::{
    book::{BookError, Level, OrderBook},
    config::{BOOK_LEVELS, ROBOT_LEVELS},
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
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
    pub(crate) status: BookStatus,
    pub(crate) exchange_time: Option<u64>,
    pub(crate) platform_time: Option<Instant>,
    epoch: Option<u64>,
}
impl BookView {
    fn empty(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
            status: BookStatus::Offline,
            exchange_time: None,
            platform_time: None,
            epoch: None,
        }
    }
}
impl Clone for BookView {
    fn clone(&self) -> Self {
        // TripleBuffer clones at construction; preserve the empty Vecs' capacity.
        let mut cloned = Self::empty(self.bids.capacity());
        cloned.bids.extend_from_slice(&self.bids);
        cloned.asks.extend_from_slice(&self.asks);
        cloned.status = self.status;
        cloned.exchange_time = self.exchange_time;
        cloned.platform_time = self.platform_time;
        cloned.epoch = self.epoch;
        cloned
    }
}
pub struct BookPublisher {
    input: Input<BookView>,
    capacity: usize,
    offline_epoch: &'static AtomicU64,
}
pub struct BookReader {
    output: Output<BookView>,
    offline_epoch: &'static AtomicU64,
    seen_epoch: u64,
}
/// Capacity limits published levels per side, independently of the source book.
/// One channel per counter; its publisher is the only writer to that counter.
pub fn channel(
    capacity: usize,
    offline_epoch: &'static AtomicU64,
) -> Result<(BookPublisher, BookReader), BookError> {
    if !(ROBOT_LEVELS..=BOOK_LEVELS).contains(&capacity) {
        return Err(BookError::Capacity);
    }
    // Preserve the generation across sequential service runs; never reset a
    // counter that may still be observed by a reader.
    let seen_epoch = offline_epoch.load(Ordering::Acquire);
    let (input, output) = TripleBuffer::new(&BookView::empty(capacity)).split();
    Ok((
        BookPublisher {
            input,
            capacity,
            offline_epoch,
        },
        BookReader {
            output,
            offline_epoch,
            seen_epoch,
        },
    ))
}
impl BookPublisher {
    /// Offline invalidates all older views independently of book publication.
    /// Online copies the best levels, preserving diff timestamps. No allocations or awaits.
    pub fn publish(&mut self, book: &OrderBook, status: BookStatus) -> Result<(), BookError> {
        if status == BookStatus::Offline {
            // One writer. Release publishes invalidation independently of the triple buffer;
            // the reader acquires it before accepting a view from this epoch.
            let epoch = self
                .offline_epoch
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or(BookError::PublicationEpochOverflow)?;
            self.offline_epoch.store(epoch, Ordering::Release);
            return Ok(());
        }
        if book.event_time().is_none()
            || !book
                .coverage()
                .is_some_and(|c| c.is_sufficient(book.bids(), book.asks()))
        {
            return Err(BookError::NotReady);
        }
        let slot = self.input.input_buffer_mut();
        slot.bids.clear();
        slot.asks.clear();
        slot.bids
            .extend_from_slice(&book.bids()[..book.bids().len().min(self.capacity)]);
        slot.asks
            .extend_from_slice(&book.asks()[..book.asks().len().min(self.capacity)]);
        slot.status = status;
        // Only this producer increments the epoch; book data travels through the triple buffer.
        slot.epoch = Some(self.offline_epoch.load(Ordering::Relaxed));
        slot.exchange_time = book.event_time().map(|t| t.exchange_time());
        slot.platform_time = book.event_time().map(|t| t.platform_time());
        self.input.publish();
        Ok(())
    }
}
impl BookReader {
    pub fn update(&mut self) -> bool {
        let previous_status = self.output.output_buffer().status;
        let updated = self.output.update();
        // Acquire the book BEFORE the epoch. A recovered publication synchronizes
        // with its preceding gap, so Online cannot conceal that gap from the reader.
        let epoch = self.offline_epoch.load(Ordering::Acquire);
        let interrupted = epoch != self.seen_epoch;
        self.seen_epoch = epoch;
        let view = self.output.output_buffer_mut();
        view.status = if !interrupted && view.epoch == Some(epoch) {
            BookStatus::Online
        } else {
            BookStatus::Offline
        };
        // A recovered book acquired with a gap stays held, but masked Offline for
        // this pass. Next pass acquires any newer book or unmasks this one.
        updated || interrupted || view.status != previous_status
    }
    /// Stable until the next explicit update; this call never implicitly consumes a slot.
    pub fn view(&self) -> &BookView {
        self.output.output_buffer()
    }
}

#[cfg(test)]
mod tests;
