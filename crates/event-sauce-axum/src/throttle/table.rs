use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use super::Backoff;

#[cfg(test)]
#[path = "tests/table_tests.rs"]
mod tests;

const DEFAULT_CAPACITY: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    failures: u32,
    last_failure: Instant,
    blocked_until: Instant,
}

/// `Err` with the time left while the key of `entry` must wait at `now`.
fn waiting(entry: Option<&Entry>, now: Instant) -> Result<(), Duration> {
    match entry {
        Some(entry) if now < entry.blocked_until => Err(entry.blocked_until - now),
        _ => Ok(()),
    }
}

/// What counting one attempt changed on its key, so that an attempt which
/// turns out to be no failure can take it back ([`Throttle::refund`]).
#[derive(Debug, Clone, Copy)]
pub struct Charge {
    before: Option<Entry>,
    after: Entry,
}

/// The failures of every key, and the delays they owe, in process.
///
/// A table remembers at most its capacity of keys, 10 000 unless
/// [`with_capacity`](Self::with_capacity) says otherwise: when it is full, the
/// keys past their [`forget_after`](Backoff::forget_after) go first, then the
/// one whose last failure is the oldest.
#[derive(Debug)]
pub struct Throttle {
    backoff: Backoff,
    capacity: usize,
    entries: Mutex<HashMap<String, Entry>>,
}

impl Throttle {
    /// A throttle applying `backoff` to every key.
    #[must_use]
    pub fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            capacity: DEFAULT_CAPACITY,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Remembers at most `capacity` keys.
    #[must_use]
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn forgotten(&self, entry: &Entry, now: Instant) -> bool {
        now.saturating_duration_since(entry.last_failure) >= self.backoff.forget_after
    }

    /// Whether `key` may run at `now`.
    ///
    /// # Errors
    ///
    /// The time left while `key` must wait.
    pub fn check(&self, key: &str, now: Instant) -> Result<(), Duration> {
        waiting(self.entries().get(key), now)
    }

    /// Counts an attempt of `key` at `now` as a failure, unless `key` must
    /// wait: checking and counting at once, so that attempts running together
    /// each see the others counted. The [`Charge`] takes it back
    /// ([`refund`](Self::refund)).
    ///
    /// # Errors
    ///
    /// The time left while `key` must wait; nothing is counted then.
    pub fn charge(&self, key: &str, now: Instant) -> Result<Charge, Duration> {
        let mut entries = self.entries();
        waiting(entries.get(key), now)?;
        let before = entries.get(key).copied();
        let after = self.count(&mut entries, key, now);
        Ok(Charge { before, after })
    }

    #[cfg(test)]
    fn fail(&self, key: &str, now: Instant) {
        self.count(&mut self.entries(), key, now);
    }

    fn count(&self, entries: &mut HashMap<String, Entry>, key: &str, now: Instant) -> Entry {
        if !entries.contains_key(key) && entries.len() >= self.capacity {
            self.evict(entries, now);
        }
        let entry = entries.entry(key.to_owned()).or_insert(Entry {
            failures: 0,
            last_failure: now,
            blocked_until: now,
        });
        if self.forgotten(entry, now) {
            entry.failures = 0;
        }
        entry.failures = entry.failures.saturating_add(1);
        entry.last_failure = now;
        entry.blocked_until = now + self.backoff.delay(entry.failures);
        *entry
    }

    /// Forgets the failures of `key`, after it succeeded.
    pub fn clear(&self, key: &str) {
        self.entries().remove(key);
    }

    /// Takes back the failure `charge` counted on `key`, for an attempt that
    /// turned out to be none: the key is left as the charge found it when
    /// nothing counted on it since, and else one failure lighter, its wait no
    /// longer than the failures left owe.
    pub fn refund(&self, key: &str, charge: Charge) {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(key) else {
            return;
        };
        if *entry != charge.after {
            entry.failures = entry.failures.saturating_sub(1);
            entry.blocked_until = entry
                .blocked_until
                .min(entry.last_failure + self.backoff.delay(entry.failures));
            return;
        }
        match charge.before {
            Some(before) => *entry = before,
            None => {
                entries.remove(key);
            }
        }
    }

    fn evict(&self, entries: &mut HashMap<String, Entry>, now: Instant) {
        entries.retain(|_, entry| !self.forgotten(entry, now));
        if entries.len() < self.capacity {
            return;
        }
        let oldest = entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_failure)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            entries.remove(&oldest);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries().len()
    }
}
