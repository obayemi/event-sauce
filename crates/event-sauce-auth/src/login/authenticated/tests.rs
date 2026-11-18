use super::*;
use crate::test_support::{credentials, stored_user};

#[test]
fn an_authenticated_user_exposes_its_user_and_fingerprint() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);
    let fingerprint = user.identity().session_fingerprint();

    let authenticated = Authenticated::new(user.clone(), fingerprint);

    assert_eq!(authenticated.user().entity_id(), user.entity_id());
    assert_eq!(authenticated.session_fingerprint(), fingerprint);
    assert_eq!(authenticated.into_user().entity_id(), user.entity_id());
}

#[test]
fn debug_names_the_user_and_hides_the_fingerprint() {
    let user = stored_user(credentials("ada@example.com", "pw"), true);

    let shown = format!("{:?}", Authenticated::new(user.clone(), [7; 32]));

    assert!(shown.contains(&user.entity_id().to_string()));
    assert!(!shown.contains("fingerprint"));
}
