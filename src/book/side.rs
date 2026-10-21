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
}
impl Side {
    pub(super) fn new() -> Self {
        let words = BOOK_PRICE_SLOTS.div_ceil(64);
        Self {
            quantities: vec![Qty::ZERO; BOOK_PRICE_SLOTS].into_boxed_slice(),
            occupied: vec![0; words].into_boxed_slice(),
            nonempty_words: vec![0; words.div_ceil(64)].into_boxed_slice(),
            undo: vec![Qty::ZERO; DIFF_LEVELS].into_boxed_slice(),
        }
    }
    pub(super) fn levels(&self, tick: Price, descending: bool) -> impl Iterator<Item = Level> + '_ {
        (0..self.nonempty_words.len()).flat_map(move |i| {
            let group = if descending {
                self.nonempty_words.len() - 1 - i
            } else {
                i
            };
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
        let word = index / 64;
        let bit = 1_u64 << (index % 64);
        if qty == Qty::ZERO {
            self.occupied[word] &= !bit;
            if self.occupied[word] == 0 {
                self.nonempty_words[word / 64] &= !(1_u64 << (word % 64));
            }
        } else {
            self.occupied[word] |= bit;
            self.nonempty_words[word / 64] |= 1_u64 << (word % 64);
        }
        old
    }
    pub(super) fn load(&mut self, levels: &[Level], tick: Price) {
        // Full reset is confined to snapshot recovery, never the diff hot path.
        self.quantities.fill(Qty::ZERO);
        self.occupied.fill(0);
        self.nonempty_words.fill(0);
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
