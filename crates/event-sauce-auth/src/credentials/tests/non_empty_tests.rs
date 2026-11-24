use super::*;

fn set(items: &[u32]) -> NonEmpty<u32> {
    NonEmpty::try_from(items.to_vec()).unwrap()
}

#[test]
fn new_holds_exactly_its_first_item() {
    assert_eq!(NonEmpty::new(7).as_slice(), &[7]);
}

#[test]
fn push_appends_in_order_and_reports_insertion() {
    let mut items = NonEmpty::new(1);
    assert!(items.push(2));
    assert!(items.push(3));
    assert_eq!(items.as_slice(), &[1, 2, 3]);
}

#[test]
fn push_refuses_a_duplicate() {
    let mut items = NonEmpty::new(1);
    assert!(!items.push(1));
    assert_eq!(items.as_slice(), &[1]);
}

#[test]
fn try_from_vec_rejects_an_empty_vec() {
    assert_eq!(NonEmpty::<u32>::try_from(Vec::new()), Err(EmptyError));
}

#[test]
fn try_from_vec_deduplicates_keeping_the_first_occurrence() {
    assert_eq!(set(&[3, 1, 3, 2, 1]).as_slice(), &[3, 1, 2]);
}

#[test]
fn contains_looks_through_every_item() {
    let items = set(&[1, 2]);
    assert!(items.contains(&2));
    assert!(!items.contains(&3));
}

#[test]
fn iter_yields_items_in_order() {
    assert_eq!(set(&[2, 1]).iter().copied().collect::<Vec<_>>(), [2, 1]);
}

#[test]
fn retained_keeps_the_matching_items() {
    assert_eq!(set(&[1, 2, 3]).retained(|n| n % 2 == 1), Some(set(&[1, 3])));
}

#[test]
fn retained_is_none_when_nothing_is_left() {
    assert_eq!(set(&[1, 2]).retained(|_| false), None);
}

#[test]
fn serializes_as_a_plain_sequence() {
    assert_eq!(serde_json::to_string(&set(&[1, 2])).unwrap(), "[1,2]");
}

#[test]
fn deserializing_an_empty_sequence_fails() {
    assert!(serde_json::from_str::<NonEmpty<u32>>("[]").is_err());
}

#[test]
fn deserializing_deduplicates() {
    let items: NonEmpty<u32> = serde_json::from_str("[1,1,2]").unwrap();
    assert_eq!(items, set(&[1, 2]));
}
