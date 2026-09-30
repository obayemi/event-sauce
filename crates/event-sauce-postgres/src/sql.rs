//! Schema-qualified table names and the advisory-lock key derived from them,
//! shared by every store and by [`migrations::with_migration_lock`](crate::migrations::with_migration_lock).

/// Joins a schema and table name into a fully-qualified identifier.
pub(crate) fn qualify(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// Computes a stable 64-bit advisory-lock key from `seed`, for keying
/// `pg_advisory_lock` / `pg_advisory_xact_lock` calls.
///
/// The hash is a 64-bit [FNV-1a] over the UTF-8 bytes of `seed`, reinterpreted
/// as the signed `bigint` these functions expect. FNV-1a is used deliberately
/// rather than [`std::hash::DefaultHasher`]: the latter seeds `SipHash`
/// randomly per process, so two processes would compute *different* keys and
/// any cross-process serialization guarantee keyed on it would silently not
/// hold. The algorithm is therefore part of callers' wire contract and must
/// remain stable.
///
/// [FNV-1a]: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
pub(crate) fn advisory_lock_key(seed: &str) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash.cast_signed()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins [`advisory_lock_key`]'s wire contract: it must be deterministic
    /// across calls (and, having no per-process seed, across processes — the
    /// precondition for cross-process locking), it must match a known-answer
    /// FNV-1a hash of `"public.events"` so the algorithm cannot drift
    /// silently, and distinct seeds must take distinct keys so independent
    /// locks never serialize against each other.
    #[test]
    fn advisory_lock_key_is_stable_and_input_distinct() {
        assert_eq!(
            advisory_lock_key("public.events"),
            advisory_lock_key("public.events"),
            "advisory_lock_key must be stable for a given seed"
        );

        assert_eq!(
            advisory_lock_key("public.events"),
            -146_897_220_888_487_505,
            "advisory_lock_key must be 64-bit FNV-1a of the seed"
        );

        assert_ne!(
            advisory_lock_key("public.events"),
            advisory_lock_key("other.events"),
            "different seeds must take distinct advisory keys"
        );
    }
}
