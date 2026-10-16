//! The version-expectation rule shared by the in-memory stores.

use event_sauce_core::{AggregateVersion, Error, Result, StreamId};

/// Checks every `(stream, expected)` expectation against the version the
/// stream was at when the batch was prepared.
///
/// `version_at_load` returns that load-time version. For a stream the batch
/// also writes it is the version its first write was staged from, not the
/// version later writes leave it at, so a write and an expectation for the
/// same stream agree only when they were prepared from the same read.
///
/// # Errors
///
/// Returns `Error::ConcurrencyConflict` for the first expectation whose
/// expected version differs from its load-time version.
pub(crate) fn check_expectations(
    expectations: &[(StreamId, AggregateVersion)],
    version_at_load: impl Fn(&StreamId) -> AggregateVersion,
) -> Result<()> {
    for (stream_id, expected) in expectations {
        let actual = version_at_load(stream_id);
        if actual != *expected {
            return Err(Error::concurrency_conflict(*expected, actual));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
