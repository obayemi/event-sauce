//! Cross-aggregate uniqueness claims shared by the in-memory stores.

use event_sauce_core::{AggregateClaim, Error, Result};
use std::collections::HashMap;
use uuid::Uuid;

/// Index of uniqueness claims: which aggregate holds each `(type, key)`.
///
/// Mirrors the Postgres `aggregate_claims` table: re-claiming a value by
/// its holder is fine, changing a claim releases the old value, and a
/// claim type no longer present is dropped.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClaimsData {
    holders: HashMap<(String, String), Uuid>,
    by_aggregate: HashMap<Uuid, HashMap<String, String>>,
}

impl ClaimsData {
    pub(crate) fn check(&self, aggregate_id: Uuid, claims: &[AggregateClaim]) -> Result<()> {
        for claim in claims {
            let key = (claim.claim_type.to_string(), claim.claim_key.to_string());
            if let Some(holder) = self.holders.get(&key) {
                if *holder != aggregate_id {
                    return Err(Error::claim_conflict(
                        claim.claim_type,
                        claim.claim_key.clone(),
                        Some(*holder),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn upsert(&mut self, aggregate_id: Uuid, claims: &[AggregateClaim]) {
        let held = self.by_aggregate.entry(aggregate_id).or_default();
        let stale: Vec<String> = held
            .keys()
            .filter(|claim_type| !claims.iter().any(|c| c.claim_type == claim_type.as_str()))
            .cloned()
            .collect();
        for claim_type in stale {
            if let Some(old_key) = held.remove(&claim_type) {
                self.holders.remove(&(claim_type, old_key));
            }
        }
        for claim in claims {
            let claim_type = claim.claim_type.to_string();
            let claim_key = claim.claim_key.to_string();
            if let Some(old_key) = held.insert(claim_type.clone(), claim_key.clone()) {
                if old_key != claim_key {
                    self.holders.remove(&(claim_type.clone(), old_key));
                }
            }
            self.holders.insert((claim_type, claim_key), aggregate_id);
        }
    }

    pub(crate) fn clear(&mut self, aggregate_id: Uuid) {
        if let Some(held) = self.by_aggregate.remove(&aggregate_id) {
            for entry in held {
                self.holders.remove(&entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Model-based property test for [`ClaimsData`]: a naive reference that
    //! just tracks each aggregate's current claims list, scanning it linearly
    //! to find a key's holder, must agree with the two-index structure
    //! `ClaimsData` actually uses on every check, every upsert, and the full
    //! final state over a finite universe of claim types and keys.
    use super::ClaimsData;
    use event_sauce_core::AggregateClaim;
    use proptest::prelude::*;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    const CLAIM_TYPES: [&str; 2] = ["Acct.email", "Acct.username"];
    const CLAIM_KEYS: [&str; 2] = ["alice", "bob"];

    #[derive(Debug, Clone, Default)]
    struct ReferenceClaims {
        by_aggregate: HashMap<Uuid, Vec<(String, String)>>,
    }

    impl ReferenceClaims {
        fn holder_of(&self, claim_type: &str, claim_key: &str) -> Option<Uuid> {
            self.by_aggregate.iter().find_map(|(aggregate_id, claims)| {
                claims
                    .iter()
                    .any(|(t, k)| t == claim_type && k == claim_key)
                    .then_some(*aggregate_id)
            })
        }

        fn check(&self, aggregate_id: Uuid, claims: &[(String, String)]) -> bool {
            claims.iter().all(|(t, k)| {
                self.holder_of(t, k)
                    .is_none_or(|holder| holder == aggregate_id)
            })
        }

        fn upsert(&mut self, aggregate_id: Uuid, claims: Vec<(String, String)>) {
            self.by_aggregate.insert(aggregate_id, claims);
        }

        fn clear(&mut self, aggregate_id: Uuid) {
            self.by_aggregate.remove(&aggregate_id);
        }
    }

    fn arb_aggregate() -> impl Strategy<Value = Uuid> {
        prop::sample::select(&[0u128, 1, 2][..]).prop_map(Uuid::from_u128)
    }

    fn arb_claims() -> impl Strategy<Value = Vec<(&'static str, &'static str)>> {
        prop::collection::hash_map(
            prop::sample::select(&CLAIM_TYPES[..]),
            prop::sample::select(&CLAIM_KEYS[..]),
            0..=2,
        )
        .prop_map(|map| map.into_iter().collect())
    }

    #[derive(Debug, Clone)]
    enum Op {
        Upsert(Uuid, Vec<(&'static str, &'static str)>),
        Clear(Uuid),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (arb_aggregate(), arb_claims()).prop_map(|(a, c)| Op::Upsert(a, c)),
            arb_aggregate().prop_map(Op::Clear),
        ]
    }

    proptest! {
        #[test]
        fn matches_reference_model(ops in prop::collection::vec(op_strategy(), 1..30)) {
            let mut sut = ClaimsData::default();
            let mut reference = ReferenceClaims::default();

            for op in ops {
                match op {
                    Op::Upsert(aggregate_id, claims) => {
                        let aggregate_claims: Vec<AggregateClaim> = claims
                            .iter()
                            .map(|(t, k)| AggregateClaim::new(t, json!(k)))
                            .collect();
                        let pairs: Vec<(String, String)> = claims
                            .iter()
                            .map(|(t, k)| ((*t).to_string(), (*k).to_string()))
                            .collect();

                        let sut_ok = sut.check(aggregate_id, &aggregate_claims).is_ok();
                        let model_ok = reference.check(aggregate_id, &pairs);
                        prop_assert_eq!(sut_ok, model_ok);

                        if model_ok {
                            sut.upsert(aggregate_id, &aggregate_claims);
                            reference.upsert(aggregate_id, pairs);
                        }
                    }
                    Op::Clear(aggregate_id) => {
                        sut.clear(aggregate_id);
                        reference.clear(aggregate_id);
                    }
                }
            }

            for claim_type in CLAIM_TYPES {
                for claim_key in CLAIM_KEYS {
                    let expected_holder = reference.holder_of(claim_type, claim_key);
                    for aggregate_id in [
                        Uuid::from_u128(0),
                        Uuid::from_u128(1),
                        Uuid::from_u128(2),
                    ] {
                        let claim = [AggregateClaim::new(claim_type, json!(claim_key))];
                        let sut_ok = sut.check(aggregate_id, &claim).is_ok();
                        let model_ok = expected_holder.is_none_or(|holder| holder == aggregate_id);
                        prop_assert_eq!(sut_ok, model_ok);
                    }
                }
            }
        }
    }
}
