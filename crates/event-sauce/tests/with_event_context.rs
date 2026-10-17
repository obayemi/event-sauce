//! `#[with_event_context]` runs the annotated function inside an event
//! context: events applied in its body carry the attribute's values.

use async_trait::async_trait;
use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use event_sauce::{with_event_context, EventMetadata};
use serde_json::json;

mod account_fixture;
use account_fixture::{Account, AccountCommands};

type Root = AggregateRoot<Account>;

async fn save_and_load(mut account: Root) -> Vec<Option<EventMetadata>> {
    let store = std::sync::Arc::new(InMemoryEventStore::builder().build());
    store
        .repository::<Account>()
        .save(&mut account)
        .await
        .unwrap();
    account_fixture::stored_metadata(&store, account.entity_id()).await
}

async fn only_metadata(account: Root) -> EventMetadata {
    let mut all = save_and_load(account).await;
    assert_eq!(all.len(), 1);
    all.remove(0).expect("event carries context metadata")
}

#[with_event_context(correlation_id = "sync")]
fn sync_open() -> Root {
    Account::open().unwrap()
}

#[with_event_context(root, tenant = "acme", correlation_id = "early")]
fn early_return(skip: bool) -> Root {
    let mut account = Account::open().unwrap();
    if skip {
        return account;
    }
    account.deposit(1).unwrap();
    account
}

#[with_event_context(correlation_id = "fallible")]
fn fallible(input: &str) -> std::result::Result<Root, std::num::ParseIntError> {
    let amount = input.parse::<i64>()?;
    let mut account = Account::open().unwrap();
    account.deposit(amount).unwrap();
    Ok(account)
}

#[with_event_context(correlation_id = "unit")]
fn unit_return(sink: &mut Option<Root>) {
    *sink = Some(Account::open().unwrap());
}

#[with_event_context(correlation_id = "opaque")]
fn opaque_return() -> impl Into<Root> {
    Account::open().unwrap()
}

#[with_event_context(user = %name, attempt)]
fn arguments_see_parameters(name: &str, attempt: u32) -> Root {
    Account::open().unwrap()
}

#[with_event_context(%user, attempt)]
fn owned_parameters_stay_usable(user: String, attempt: String) -> usize {
    user.len() + attempt.len()
}

#[with_event_context(correlation_id = "async")]
async fn async_open(text: &str) -> (usize, Root) {
    tokio::task::yield_now().await;
    (text.len(), Account::open().unwrap())
}

#[with_event_context(correlation_id = "async-fallible")]
async fn async_fallible(input: &str) -> std::result::Result<Root, std::num::ParseIntError> {
    let amount = input.parse::<i64>()?;
    tokio::task::yield_now().await;
    let mut account = Account::open().unwrap();
    account.deposit(amount).unwrap();
    Ok(account)
}

#[async_trait]
trait Service {
    async fn handle(&self, text: &str) -> (usize, Root);
    async fn owned(&self, user: String) -> usize;
}

struct Handler;

#[async_trait]
impl Service for Handler {
    #[with_event_context(root, tenant = "acme", correlation_id = "trait")]
    async fn handle(&self, text: &str) -> (usize, Root) {
        tokio::task::yield_now().await;
        (text.len(), Account::open().unwrap())
    }

    #[with_event_context(%user)]
    async fn owned(&self, user: String) -> usize {
        user.len()
    }
}

#[tokio::test]
async fn sync_fn_tags_events_applied_in_its_body() {
    let metadata = only_metadata(sync_open()).await;
    assert_eq!(metadata.correlation_id.as_deref(), Some("sync"));
    assert_eq!(metadata.tenant, None);
}

#[tokio::test]
async fn events_applied_outside_the_fn_have_no_metadata() {
    sync_open();
    let account = Account::open().unwrap();
    assert_eq!(save_and_load(account).await, [None]);
}

#[tokio::test]
async fn early_return_keeps_the_context_and_the_value() {
    let skipped = save_and_load(early_return(true)).await;
    assert_eq!(skipped.len(), 1);
    let full = save_and_load(early_return(false)).await;
    assert_eq!(full.len(), 2);
    for metadata in skipped.into_iter().chain(full) {
        let metadata = metadata.unwrap();
        assert_eq!(metadata.tenant.as_deref(), Some("acme"));
        assert_eq!(metadata.correlation_id.as_deref(), Some("early"));
    }
}

#[tokio::test]
async fn question_mark_works_in_a_sync_fn() {
    let account = fallible("41").unwrap();
    assert_eq!(account.balance, 41);
    let metadata = save_and_load(account).await;
    assert!(metadata
        .iter()
        .all(|m| m.as_ref().unwrap().correlation_id.as_deref() == Some("fallible")));
    assert!(fallible("x").is_err());
}

#[tokio::test]
async fn unit_return_fn_tags_events() {
    let mut sink = None;
    unit_return(&mut sink);
    let metadata = only_metadata(sink.unwrap()).await;
    assert_eq!(metadata.correlation_id.as_deref(), Some("unit"));
}

#[tokio::test]
async fn impl_trait_return_fn_tags_events() {
    let metadata = only_metadata(opaque_return().into()).await;
    assert_eq!(metadata.correlation_id.as_deref(), Some("opaque"));
}

#[tokio::test]
async fn arguments_may_use_parameters() {
    let metadata = only_metadata(arguments_see_parameters("ada", 2)).await;
    assert_eq!(
        metadata.additional.map(serde_json::Value::Object),
        Some(json!({"user": "ada", "attempt": 2}))
    );
}

#[tokio::test]
async fn async_fn_borrows_its_parameters_and_keeps_the_context_across_awaits() {
    let text = String::from("hello");
    let (len, account) = async_open(&text).await;
    assert_eq!(len, 5);
    let metadata = only_metadata(account).await;
    assert_eq!(metadata.correlation_id.as_deref(), Some("async"));
}

#[tokio::test]
async fn question_mark_works_in_an_async_fn() {
    let account = async_fallible("7").await.unwrap();
    assert_eq!(account.balance, 7);
    assert!(save_and_load(account).await.iter().all(|m| m
        .as_ref()
        .unwrap()
        .correlation_id
        .as_deref()
        == Some("async-fallible")));
    assert!(async_fallible("x").await.is_err());
}

#[tokio::test]
async fn async_trait_method_runs_inside_the_context() {
    let (len, account) = Handler.handle("abc").await;
    assert_eq!(len, 3);
    let metadata = only_metadata(account).await;
    assert_eq!(metadata.tenant.as_deref(), Some("acme"));
    assert_eq!(metadata.correlation_id.as_deref(), Some("trait"));
}

#[test]
fn sync_fn_keeps_owned_parameters_usable_in_its_body() {
    assert_eq!(owned_parameters_stay_usable("ada".into(), "42".into()), 5);
}

#[tokio::test]
async fn async_trait_method_keeps_owned_parameters_usable_in_its_body() {
    assert_eq!(Handler.owned("ada".into()).await, 3);
}
