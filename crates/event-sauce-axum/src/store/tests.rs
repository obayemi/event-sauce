use std::sync::Arc;

use super::Store;

struct NotClone(u8);

#[test]
fn cloning_a_store_shares_the_underlying_store() {
    let store = Store::new(NotClone(7));
    let clone = store.clone();
    assert!(Arc::ptr_eq(store.arc(), clone.arc()));
}

#[test]
fn a_store_wraps_an_existing_arc_without_copying_it() {
    let shared = Arc::new(NotClone(7));
    let store = Store::from(Arc::clone(&shared));
    assert!(Arc::ptr_eq(&shared, store.arc()));
}

#[test]
fn a_store_derefs_to_the_wrapped_store() {
    let store = Store::new(NotClone(7));
    assert_eq!((*store).0, 7);
}
