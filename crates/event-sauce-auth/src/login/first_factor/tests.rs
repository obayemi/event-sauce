use super::*;
use crate::{
    test_support::{credentials, stored_user, FakeUsers},
    Login,
};

#[test]
fn a_first_factor_hands_out_its_method_and_route() {
    let first = FirstFactor::new("method", "route");

    assert_eq!((*first.method(), *first.route()), ("method", "route"));
}

#[test]
fn always_done_routes_a_single_factor_login_to_done() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);
    let users = FakeUsers::holding(&user);
    let entered = Login::new(
        user.clone(),
        chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        std::sync::Arc::new(users),
        Entered,
    );

    let login = AlwaysDone.route(Advance::new(entered));

    assert_eq!(login.user().entity_id(), user.entity_id());
}

fn assert_single_factor<C: SingleFactor>() {}

#[test]
fn the_ready_made_credentials_are_single_factor() {
    assert_single_factor::<Password<crate::Email>>();
    assert_single_factor::<Federated>();
    assert_single_factor::<PasswordOrFederated<crate::Email>>();
}
