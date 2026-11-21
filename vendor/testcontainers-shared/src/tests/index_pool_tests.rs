use super::IndexPool;

#[test]
fn acquire_hands_out_distinct_indices_up_to_the_capacity() {
    let mut pool = IndexPool::new(3);

    let indices: Vec<u16> = (0..3)
        .map(|_| pool.acquire().expect("free index").get())
        .collect();

    assert_eq!(indices, [0, 1, 2]);
}

#[test]
fn acquire_is_none_once_every_index_is_held() {
    let mut pool = IndexPool::new(1);
    let _held = pool.acquire().expect("free index");

    assert!(pool.acquire().is_none());
}

#[test]
fn a_released_index_is_handed_out_again() {
    let mut pool = IndexPool::new(1);
    let held = pool.acquire().expect("free index");

    pool.release(held);

    assert_eq!(pool.acquire().map(|index| index.get()), Some(0));
}

#[test]
fn an_empty_pool_never_hands_out_an_index() {
    assert!(IndexPool::new(0).acquire().is_none());
}
