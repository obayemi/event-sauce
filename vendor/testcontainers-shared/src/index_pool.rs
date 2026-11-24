//! Hands out the indices of a fixed set of slots, each to one holder at a
//! time.

/// The indices `0..capacity`, each held by at most one holder at a time.
#[derive(Debug)]
pub(crate) struct IndexPool {
    capacity: u16,
    next: u16,
    released: Vec<Index>,
}

/// An index held from an [`IndexPool`]. It is neither `Clone` nor `Copy`, so
/// only its holder can give it back.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Index(u16);

impl Index {
    pub(crate) fn get(&self) -> u16 {
        self.0
    }
}

impl IndexPool {
    pub(crate) const fn new(capacity: u16) -> Self {
        Self {
            capacity,
            next: 0,
            released: Vec::new(),
        }
    }

    /// A free index, preferring one released earlier, or `None` once every
    /// index is held.
    pub(crate) fn acquire(&mut self) -> Option<Index> {
        self.released.pop().or_else(|| {
            (self.next < self.capacity).then(|| {
                self.next += 1;
                Index(self.next - 1)
            })
        })
    }

    /// Makes `index` free for the next [`IndexPool::acquire`].
    pub(crate) fn release(&mut self, index: Index) {
        self.released.push(index);
    }
}

#[cfg(test)]
#[path = "tests/index_pool_tests.rs"]
mod tests;
