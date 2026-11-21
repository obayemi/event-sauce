use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    test_support::{ada, credentials, login_in, restored_user, started_at, FakeUsers, TestUser},
    Done, Entered,
};

#[derive(Debug, Serialize, Deserialize)]
struct Ask {
    expected: String,
}

impl LoginState for Ask {
    const TAG: &'static str = "test.ask";
}

#[async_trait]
impl Step<TestUser> for Ask {
    type Proof = String;
    type Verifier = ();
    type Next = Login<TestUser, Done>;

    async fn verify(
        advance: Advance<TestUser, Self>,
        (): &(),
        proof: String,
    ) -> Result<Login<TestUser, Done>, Rejected> {
        if proof == advance.login().state().expected {
            Ok(advance.to(Done))
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

fn asking(user: &Arc<AggregateRoot<TestUser>>, users: &FakeUsers) -> Login<TestUser, Ask> {
    login_in(
        user,
        users,
        Ask {
            expected: "open".into(),
        },
    )
}

#[test]
fn a_login_exposes_its_user_state_and_start_time() {
    let user = ada();
    let login = asking(&user, &FakeUsers::holding(&user));

    assert_eq!(login.user().entity_id(), user.entity_id());
    assert_eq!(login.state().expected, "open");
    assert_eq!(login.started_at(), started_at());
}

#[test]
fn a_login_pins_the_fingerprint_of_its_user() {
    let user = ada();
    let login = asking(&user, &FakeUsers::holding(&user));

    assert_eq!(login.fingerprint(), user.identity().session_fingerprint());
}

#[tokio::test]
async fn the_right_proof_moves_the_login_to_the_next_state() {
    let user = ada();
    let users = FakeUsers::holding(&user);

    let next = asking(&user, &users)
        .prove(&(), "open".into())
        .await
        .unwrap();

    assert_eq!(next.user().entity_id(), user.entity_id());
    assert_eq!(next.started_at(), started_at());
    assert_eq!(next.fingerprint(), user.identity().session_fingerprint());
}

#[tokio::test]
async fn a_wrong_proof_is_rejected() {
    let user = ada();
    let users = FakeUsers::holding(&user);

    let rejected = asking(&user, &users)
        .prove(&(), "nope".into())
        .await
        .unwrap_err();

    assert!(matches!(rejected, Rejected::Mismatch));
}

#[tokio::test]
async fn a_complete_login_finishes_into_an_authenticated_user() {
    let user = ada();
    let users = FakeUsers::holding(&user);

    let authenticated = login_in(&user, &users, Done).finish().await.unwrap();

    assert_eq!(authenticated.user().entity_id(), user.entity_id());
    assert_eq!(
        authenticated.session_fingerprint(),
        user.identity().session_fingerprint()
    );
    assert_eq!(authenticated.into_user().entity_id(), user.entity_id());
}

#[tokio::test]
async fn a_user_deactivated_mid_login_cannot_finish() {
    let user = ada();
    let users = FakeUsers::holding(&user);
    let login = login_in(&user, &users, Done);
    users.replace(restored_user(
        user.entity_id(),
        credentials("ada@example.com", "pw"),
        false,
    ));

    assert!(matches!(login.finish().await, Err(FinishError::Inactive)));
}

#[tokio::test]
async fn a_user_removed_mid_login_cannot_finish() {
    let user = ada();
    let users = FakeUsers::holding(&user);
    let login = login_in(&user, &users, Done);
    users.remove(user.entity_id());

    assert!(matches!(login.finish().await, Err(FinishError::Inactive)));
}

#[tokio::test]
async fn credentials_changed_mid_login_cannot_finish() {
    let user = ada();
    let users = FakeUsers::holding(&user);
    let login = login_in(&user, &users, Done);
    users.replace(restored_user(
        user.entity_id(),
        credentials("ada@example.com", "new-password"),
        true,
    ));

    assert!(matches!(
        login.finish().await,
        Err(FinishError::CredentialsChanged)
    ));
}

#[test]
fn a_login_debug_shows_its_state_but_no_secret() {
    let user = ada();
    let login = asking(&user, &FakeUsers::holding(&user));

    let shown = format!("{login:?}");

    assert!(shown.contains("Ask") && !shown.contains("fingerprint"));
}

#[test]
fn an_advance_moves_a_login_out_of_the_entered_state() {
    let user = ada();
    let entered = login_in(&user, &FakeUsers::holding(&user), Entered);

    let login = Advance(entered).to(Done);

    assert_eq!(login.user().entity_id(), user.entity_id());
}
