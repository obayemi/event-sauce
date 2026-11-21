use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    test_support::{
        ada, credentials, email, secret, started_at, stored_user, FakeHasher, FakeLoginStore,
        FakeUsers, TestUser,
    },
    AlwaysDone, FixedClock, LoginState, PasswordAttempt, PasswordLogin,
};

fn attempt(login: &str, password: &str) -> PasswordAttempt<crate::Email> {
    PasswordAttempt::new(email(login), secret(password))
}

fn password_factor(hasher: &FakeHasher) -> FirstFactor<PasswordLogin<FakeHasher>, AlwaysDone> {
    FirstFactor::new(PasswordLogin::new(hasher.clone()), AlwaysDone)
}

fn logins(users: &FakeUsers) -> Logins<TestUser> {
    Logins::over(Arc::new(users.clone())).with_clock(Arc::new(FixedClock(started_at())))
}

#[tokio::test]
async fn the_right_password_begins_a_login_for_the_user() {
    let user = ada();
    let hasher = FakeHasher::default();

    let login = logins(&FakeUsers::holding(&user))
        .begin(&password_factor(&hasher), attempt("ADA@example.com", "pw"))
        .await
        .unwrap();

    assert_eq!(login.user().entity_id(), user.entity_id());
    assert_eq!(login.started_at(), started_at());
    assert_eq!(hasher.verified(), 1);
}

#[tokio::test]
async fn the_wrong_password_is_invalid_credentials() {
    let user = ada();

    let error = logins(&FakeUsers::holding(&user))
        .begin(
            &password_factor(&FakeHasher::default()),
            attempt("ada@example.com", "nope"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::InvalidCredentials));
}

#[tokio::test]
async fn an_unknown_login_hands_the_attempt_back_and_equalises_timing() {
    let user = ada();
    let hasher = FakeHasher::default();

    let error = logins(&FakeUsers::holding(&user))
        .begin(
            &password_factor(&hasher),
            attempt("nobody@example.com", "pw"),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, AuthError::Unknown { attempt } if attempt.login == email("nobody@example.com"))
    );
    assert_eq!(hasher.verified(), 1);
}

#[tokio::test]
async fn a_located_user_that_is_gone_is_unknown() {
    let user = ada();
    let users = FakeUsers::holding(&user);
    users.remove(user.entity_id());

    let error = logins(&users)
        .begin(
            &password_factor(&FakeHasher::default()),
            attempt("ada@example.com", "pw"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::Unknown { .. }));
}

#[tokio::test]
async fn an_inactive_user_is_refused_before_the_password_is_checked() {
    let user = stored_user(credentials("ada@example.com", "pw"), false);
    let hasher = FakeHasher::default();

    let error = logins(&FakeUsers::holding(&user))
        .begin(&password_factor(&hasher), attempt("ada@example.com", "pw"))
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::Inactive));
    assert_eq!(hasher.verified(), 0);
}

#[tokio::test]
async fn a_hasher_failure_is_a_hash_error() {
    let user = ada();

    let error = logins(&FakeUsers::holding(&user))
        .begin(
            &password_factor(&FakeHasher::broken()),
            attempt("ada@example.com", "pw"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, AuthError::Hash(_)));
}

#[derive(Debug, Serialize, Deserialize)]
struct Review;

impl LoginState for Review {
    const TAG: &'static str = "test.review";
}

struct ToReview;

impl Route<TestUser> for ToReview {
    type Next = Login<TestUser, Review>;

    fn route(&self, entered: Advance<TestUser, Entered>) -> Login<TestUser, Review> {
        entered.to(Review)
    }
}

#[tokio::test]
async fn the_route_picks_the_state_the_login_goes_to() {
    let user = ada();
    let factor = FirstFactor::new(PasswordLogin::new(FakeHasher::default()), ToReview);

    let login = logins(&FakeUsers::holding(&user))
        .begin(&factor, attempt("ada@example.com", "pw"))
        .await
        .unwrap();

    assert_eq!(format!("{:?}", login.state()), "Review");
}

#[tokio::test]
async fn a_new_user_signs_up_straight_into_done() {
    let users = FakeUsers::default();
    let user = TestUser::register(credentials("eve@example.com", "pw")).unwrap();
    let id = user.entity_id();

    let login = logins(&users).sign_up(user).await.unwrap();

    assert_eq!(login.user().entity_id(), id);
    assert_eq!(login.started_at(), started_at());
}

#[tokio::test]
async fn an_aggregate_with_nothing_to_commit_is_not_new() {
    let users = FakeUsers::default();
    let mut user = TestUser::register(credentials("eve@example.com", "pw")).unwrap();
    user.clear_pending_events();

    let error = logins(&users).sign_up(user).await.unwrap_err();

    assert!(matches!(error, SignUpError::NotNew));
}

#[test]
fn pending_logins_share_the_user_source() {
    let users = FakeUsers::default();

    let pending = logins(&users).pending(Arc::new(FakeLoginStore::default()));

    assert_eq!(format!("{pending:?}"), "PendingLogins { .. }");
}

#[test]
fn logins_clone_and_debug_without_naming_the_store() {
    let logins = logins(&FakeUsers::default());

    assert_eq!(format!("{:?}", logins.clone()), "Logins { .. }");
}
