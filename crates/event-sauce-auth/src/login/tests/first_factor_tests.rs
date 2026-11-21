use super::*;
use crate::test_support::{ada, login_in, FakeUsers};

#[test]
fn a_first_factor_hands_out_its_method_and_route() {
    let first = FirstFactor::new("method", "route");

    assert_eq!((*first.method(), *first.route()), ("method", "route"));
}

#[test]
fn always_done_routes_a_single_factor_login_to_done() {
    let user = ada();
    let entered = login_in(&user, &FakeUsers::holding(&user), Entered);

    let login = AlwaysDone.route(Advance(entered));

    assert_eq!(login.user().entity_id(), user.entity_id());
}

fn assert_single_factor<C: SingleFactor>() {}

#[test]
fn the_ready_made_credentials_are_single_factor() {
    assert_single_factor::<Password<crate::Email>>();
    assert_single_factor::<Federated>();
    assert_single_factor::<PasswordOrFederated<crate::Email>>();
}
