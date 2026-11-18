use super::*;
use serde_json::json;
use uuid::Uuid;

#[test]
fn display_renders_through_to_string() {
    assert_eq!(
        Value::from(display(Uuid::nil())),
        json!(Uuid::nil().to_string())
    );
    assert_eq!(Value::from(display(12)), json!("12"));
}
