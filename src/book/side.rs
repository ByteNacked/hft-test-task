//! Fixed price slots and two bitmap levels; all storage is allocated at startup.
use super::{IndexedLevel, Level, PriceWindow};
use crate::{
    config::{BOOK_PRICE_SLOTS, PUBLISHED_LEVELS},
    numeric::Qty,
};
use arrayvec::ArrayVec;

#[derive(Debug)]
pub(super) struct Side {
    quantities: Box<[Qty]>,
    occupied: Box<[u64]>,
    nonempty_words: Box<[u64]>,
    descending: bool,
    // First nonempty summary word in this side's price order; None iff empty.
    best_group: Option<usize>,
    pub(super) top: ArrayVec<Level, PUBLISHED_LEVELS>,
    worst_top_index: usize,
    top_dirty: bool,
}
impl Side {
    pub(super) fn new(descending: bool) -> Self {
        let words = BOOK_PRICE_SLOTS.div_ceil(u64::BITS as usize);
        Self {
            quantities: vec![Qty::ZERO; BOOK_PRICE_SLOTS].into_boxed_slice(),
            occupied: vec![0; words].into_boxed_slice(),
            nonempty_words: vec![0; words.div_ceil(u64::BITS as usize)].into_boxed_slice(),
            descending,
            best_group: None,
            top: ArrayVec::new(),
            worst_top_index: 0,
            top_dirty: false,
        }
    }
    pub(super) fn levels(&self, window: PriceWindow) -> impl Iterator<Item = Level> + '_ {
        let descending = self.descending;
        let start = self.best_group.unwrap_or(0);
        let count = self.best_group.map_or(0, |best| {
            if descending {
                best + 1
            } else {
                self.nonempty_words.len() - best
            }
        });
        (0..count).flat_map(move |i| {
            let group = if descending { start - i } else { start + i };
            bits(self.nonempty_words[group], descending).flat_map(move |bit| {
                let word = group * u64::BITS as usize + bit;
                bits(self.occupied[word], descending).map(move |bit| {
                    let index = word * u64::BITS as usize + bit;
                    Level {
                        price: window.price(index),
                        qty: self.quantities[index],
                    }
                })
            })
        })
    }
    fn set(&mut self, index: usize, qty: Qty) -> Qty {
        let old = std::mem::replace(&mut self.quantities[index], qty);
        if (old == Qty::ZERO) == (qty == Qty::ZERO) {
            return old; // Quantity-only updates do not change bitmap occupancy.
        }
        let word = index / u64::BITS as usize;
        let group = word / u64::BITS as usize;
        let bit = 1_u64 << (index % u64::BITS as usize);
        if qty == Qty::ZERO {
            self.occupied[word] &= !bit;
            if self.occupied[word] == 0 {
                self.nonempty_words[group] &= !(1_u64 << (word % u64::BITS as usize));
                if self.nonempty_words[group] == 0 && self.best_group == Some(group) {
                    self.best_group = if self.descending {
                        (0..group).rev().find(|&i| self.nonempty_words[i] != 0)
                    } else {
                        (group + 1..self.nonempty_words.len())
                            .find(|&i| self.nonempty_words[i] != 0)
                    };
                }
            }
        } else {
            self.occupied[word] |= bit;
            self.nonempty_words[group] |= 1_u64 << (word % u64::BITS as usize);
            if self.best_group.is_none_or(|best| {
                if self.descending {
                    group > best
                } else {
                    group < best
                }
            }) {
                self.best_group = Some(group);
            }
        }
        old
    }
    pub(super) fn load(&mut self, levels: &[Level], window: PriceWindow) {
        // Prices were checked against this window before loading either side.
        // Full reset is confined to snapshot recovery, never the diff hot path.
        self.quantities.fill(Qty::ZERO);
        self.occupied.fill(0);
        self.nonempty_words.fill(0);
        self.best_group = None;
        for level in levels {
            self.set(window.index(level.price).unwrap(), level.qty);
        }
        self.top_dirty = true;
        self.refresh_top(window);
    }
    pub(super) fn apply(&mut self, levels: &[IndexedLevel]) {
        for level in levels {
            let old = self.set(level.index, level.qty);
            // Compare against the committed boundary for the entire diff. Deletions
            // inside it trigger a rebuild that also picks up changes deeper in the book.
            self.top_dirty |= old != level.qty
                && (self.top.len() < PUBLISHED_LEVELS
                    || if self.descending {
                        level.index >= self.worst_top_index
                    } else {
                        level.index <= self.worst_top_index
                    });
        }
    }
    pub(super) fn refresh_top(&mut self, window: PriceWindow) {
        if !self.top_dirty {
            return;
        }
        self.top = self.levels(window).take(PUBLISHED_LEVELS).collect();
        self.worst_top_index = self
            .top
            .last()
            .map_or(0, |level| window.index(level.price).unwrap());
        self.top_dirty = false;
    }
}
fn bits(mut word: u64, descending: bool) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        if word == 0 {
            return None;
        }
        let bit = if descending {
            u64::BITS - 1 - word.leading_zeros()
        } else {
            word.trailing_zeros()
        };
        word &= !(1_u64 << bit);
        Some(bit as usize)
    })
}
