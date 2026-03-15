//! Aggregate claims for cross-aggregate uniqueness constraints.
//!
//! Claims enable aggregates to declare ownership of unique values (e.g., email addresses).
//! The event store enforces these claims transactionally during event append, preventing
//! two aggregates from claiming the same value.
//!
//! Claim keys are stored as SHA-256 hashes in the database, which:
//! - Avoids JSONB indexing limitations
//! - Prevents data leakage for encrypted aggregates (crypto-shredding safe)
//! - Produces compact, fixed-size (32 bytes) keys for UNIQUE constraints

/// A uniqueness claim declared by an aggregate.
///
/// Each claim has a type (namespace) and a key (the value being claimed).
/// The claim type acts as a namespace to prevent collisions between different
/// kinds of claims (e.g., "Profile.email" vs "Profile.username").
///
/// # Examples
///
/// ```
/// use event_sauce_core::AggregateClaim;
/// use serde_json::json;
///
/// let claim = AggregateClaim::new("Profile.email", json!("user@example.com"));
/// assert_eq!(claim.claim_type, "Profile.email");
/// ```
#[derive(Debug, Clone)]
pub struct AggregateClaim {
    /// Namespace for the claim (e.g., "Profile.email").
    pub claim_type: &'static str,
    /// The value being claimed, serialized as JSON for type transparency.
    pub claim_key: serde_json::Value,
}

impl AggregateClaim {
    /// Creates a new aggregate claim.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateClaim;
    /// use serde_json::json;
    ///
    /// let claim = AggregateClaim::new("Profile.email", json!("user@example.com"));
    /// ```
    #[must_use]
    pub fn new(claim_type: &'static str, claim_key: serde_json::Value) -> Self {
        Self {
            claim_type,
            claim_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_aggregate_claim_new() {
        let claim = AggregateClaim::new("Profile.email", json!("user@example.com"));
        assert_eq!(claim.claim_type, "Profile.email");
        assert_eq!(claim.claim_key, json!("user@example.com"));
    }

    #[test]
    fn test_aggregate_claim_with_composite_key() {
        let claim = AggregateClaim::new(
            "Membership.unique",
            json!({"group_id": "abc", "user_id": "xyz"}),
        );
        assert_eq!(claim.claim_type, "Membership.unique");
        assert!(claim.claim_key.is_object());
    }

    #[test]
    fn test_aggregate_claim_clone() {
        let claim = AggregateClaim::new("Profile.email", json!("test@example.com"));
        let cloned = claim.clone();
        assert_eq!(cloned.claim_type, claim.claim_type);
        assert_eq!(cloned.claim_key, claim.claim_key);
    }
}
