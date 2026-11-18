use crate::test_support::secret;
use argon2::Params;

use super::*;

fn cheap() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

#[tokio::test]
async fn a_hashed_password_verifies() {
    let hasher = cheap();
    let hash = hasher.hash(&secret("correct horse")).await.unwrap();
    assert!(hasher
        .verify(&secret("correct horse"), &hash)
        .await
        .unwrap());
}

#[tokio::test]
async fn another_password_does_not_verify() {
    let hasher = cheap();
    let hash = hasher.hash(&secret("correct horse")).await.unwrap();
    assert!(!hasher
        .verify(&secret("battery staple"), &hash)
        .await
        .unwrap());
}

#[tokio::test]
async fn the_same_password_hashes_differently_each_time() {
    let hasher = cheap();
    let first = hasher.hash(&secret("correct horse")).await.unwrap();
    let second = hasher.hash(&secret("correct horse")).await.unwrap();
    assert_ne!(first, second);
}

#[tokio::test]
async fn the_hash_records_the_configured_cost() {
    let hash = cheap().hash(&secret("correct horse")).await.unwrap();
    assert!(hash.as_str().starts_with("$argon2id$v=19$m=8,t=1,p=1$"));
}

#[tokio::test]
async fn the_default_cost_follows_the_recommended_argon2_parameters() {
    let hash = Argon2Hasher::default()
        .hash(&secret("correct horse"))
        .await
        .unwrap();
    assert!(hash.as_str().contains("m=19456,t=2,p=1"));
}

#[tokio::test]
async fn a_hash_made_at_another_cost_still_verifies() {
    let hash = cheap().hash(&secret("correct horse")).await.unwrap();
    assert!(Argon2Hasher::default()
        .verify(&secret("correct horse"), &hash)
        .await
        .unwrap());
}

#[tokio::test]
async fn a_hash_of_another_algorithm_is_an_error_not_a_mismatch() {
    let foreign =
        PasswordHash::new("$scrypt$ln=16,r=8,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2g")
            .unwrap();
    assert!(cheap()
        .verify(&secret("correct horse"), &foreign)
        .await
        .is_err());
}

#[tokio::test]
async fn a_panicked_blocking_task_is_a_hash_error() {
    let joined = tokio::task::spawn_blocking(|| -> Result<bool, HashError> {
        panic!("worker died");
    })
    .await;

    let error = flatten(joined).unwrap_err();

    assert!(error.0.contains("panic"));
}

#[tokio::test]
async fn a_finished_blocking_task_keeps_its_own_result() {
    let joined = tokio::task::spawn_blocking(|| Ok::<_, HashError>(true)).await;

    assert!(flatten(joined).unwrap());
}
