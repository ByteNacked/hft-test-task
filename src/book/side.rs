//! Fixed price slots and two bitmap levels; all storage is allocated at startup.
use super::Level;
use crate::{
    config::{BOOK_PRICE_SLOTS, DIFF_LEVELS},
    numeric::{Price, Qty},
};

#[derive(Debug)]
pub(super) struct Side {
    quantities: Box<[Qty]>,
    occupied: Box<[u64]>,
    nonempty_words: Box<[u64]>,
    undo: Box<[Qty]>,
    descending: bool,
    // First nonempty summary word in this side's price order; None iff empty.
    best_group: Option<usize>,
}
impl Side {
    pub(super) fn new(descending: bool) -> Self {
        let words = BOOK_PRICE_SLOTS.div_ceil(64);
        Self {
            quantities: vec![Qty::ZERO; BOOK_PRICE_SLOTS].into_boxed_slice(),
            occupied: vec![0; words].into_boxed_slice(),
            nonempty_words: vec![0; words.div_ceil(64)].into_boxed_slice(),
            undo: vec![Qty::ZERO; DIFF_LEVELS].into_boxed_slice(),
            descending,
            best_group: None,
        }
    }
    pub(super) fn levels(&self, tick: Price) -> impl Iterator<Item = Level> + '_ {
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
                let word = group * 64 + bit;
                bits(self.occupied[word], descending).map(move |bit| {
                    let index = word * 64 + bit;
                    Level {
                        // Only checked positive prices set bits; constructor bounds this product.
                        price: Price::from_raw(index as i64 * tick.raw()).unwrap(),
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
        let word = index / 64;
        let group = word / 64;
        let bit = 1_u64 << (index % 64);
        if qty == Qty::ZERO {
            self.occupied[word] &= !bit;
            if self.occupied[word] == 0 {
                self.nonempty_words[group] &= !(1_u64 << (word % 64));
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
            self.nonempty_words[group] |= 1_u64 << (word % 64);
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
    pub(super) fn load(&mut self, levels: &[Level], tick: Price) {
        // Full reset is confined to snapshot recovery, never the diff hot path.
        self.quantities.fill(Qty::ZERO);
        self.occupied.fill(0);
        self.nonempty_words.fill(0);
        self.best_group = None;
        for level in levels {
            self.set((level.price.raw() / tick.raw()) as usize, level.qty);
        }
    }
    pub(super) fn apply(&mut self, levels: &[Level], tick: Price) {
        for (i, level) in levels.iter().enumerate() {
            self.undo[i] = self.set((level.price.raw() / tick.raw()) as usize, level.qty);
        }
    }
    pub(super) fn rollback(&mut self, levels: &[Level], tick: Price) {
        for (i, level) in levels.iter().enumerate() {
            self.set((level.price.raw() / tick.raw()) as usize, self.undo[i]);
        }
    }
}
fn bits(mut word: u64, descending: bool) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        if word == 0 {
            return None;
        }
        let bit = if descending {
            63 - word.leading_zeros()
        } else {
            word.trailing_zeros()
        };
        word &= !(1_u64 << bit);
        Some(bit as usize)
    })
}
