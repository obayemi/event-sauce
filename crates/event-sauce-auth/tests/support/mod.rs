//! Helpers and the backend-independent contract shared by the login store
//! integration tests.

use std::time::Duration;

use testcontainers::{runners::AsyncRunner, ContainerRequest, Image, TestcontainersError};
use testcontainers_modules::testcontainers::ContainerAsync;

const START_ATTEMPTS: u32 = 5;
const START_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Starts a container built by `build`, retrying transient startup failures
/// such as a host port that another parallel test grabbed first.
pub async fn start_with_retry<I: Image>(
    mut build: impl FnMut() -> ContainerRequest<I>,
) -> Result<ContainerAsync<I>, TestcontainersError> {
    let mut attempt = 1;
    loop {
        match build().start().await {
            Ok(container) => return Ok(container),
            Err(_) if attempt < START_ATTEMPTS => {
                tokio::time::sleep(START_RETRY_DELAY * attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Generates the tests every `LoginStore` backend must pass.
///
/// `$start` is an async function returning `(guard, Arc<dyn LoginStore>)`,
/// where `guard` keeps the backing container alive for the test.
#[macro_export]
macro_rules! login_store_contract {
    ($start:ident) => {
        mod contract {
            use std::sync::Arc;
            use std::time::Duration;

            use event_sauce_auth::{LoginKey, LoginStore, LoginStoreError};

            const LONG: Duration = Duration::from_secs(60);
            const SHORT: Duration = Duration::from_millis(300);
            const CONTENDERS: usize = 16;

            fn key(name: &str) -> LoginKey {
                LoginKey::new("test", name)
            }

            #[tokio::test]
            async fn put_then_take_returns_the_value_once() {
                let (_guard, store) = super::$start().await;

                store.put(&key("a"), b"payload", LONG).await.unwrap();

                assert_eq!(
                    store.take(&key("a")).await.unwrap().as_deref(),
                    Some(&b"payload"[..])
                );
                assert_eq!(store.take(&key("a")).await.unwrap(), None);
            }

            #[tokio::test]
            async fn take_of_an_unknown_key_is_none() {
                let (_guard, store) = super::$start().await;

                assert_eq!(store.take(&key("missing")).await.unwrap(), None);
            }

            #[tokio::test]
            async fn put_overwrites_an_existing_entry() {
                let (_guard, store) = super::$start().await;

                store.put(&key("a"), b"first", LONG).await.unwrap();
                store.put(&key("a"), b"second", LONG).await.unwrap();

                assert_eq!(
                    store.take(&key("a")).await.unwrap().as_deref(),
                    Some(&b"second"[..])
                );
            }

            #[tokio::test]
            async fn values_are_arbitrary_bytes() {
                let (_guard, store) = super::$start().await;
                let bytes: Vec<u8> = (0..=255).collect();

                store.put(&key("bin"), &bytes, LONG).await.unwrap();

                assert_eq!(store.take(&key("bin")).await.unwrap(), Some(bytes));
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn take_is_single_use_under_concurrent_callers() {
                let (_guard, store) = super::$start().await;
                store.put(&key("race"), b"once", LONG).await.unwrap();

                let winners = race(&store, |store| async move {
                    store.take(&key("race")).await.unwrap().is_some()
                })
                .await;

                assert_eq!(winners, 1);
            }

            #[tokio::test]
            async fn mark_once_reports_only_the_first_recording() {
                let (_guard, store) = super::$start().await;

                assert!(store.mark_once(&key("step"), LONG).await.unwrap());
                assert!(!store.mark_once(&key("step"), LONG).await.unwrap());
                assert!(store.mark_once(&key("other"), LONG).await.unwrap());
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn mark_once_holds_under_concurrent_callers() {
                let (_guard, store) = super::$start().await;

                let winners = race(&store, |store| async move {
                    store.mark_once(&key("race"), LONG).await.unwrap()
                })
                .await;

                assert_eq!(winners, 1);
            }

            #[tokio::test]
            async fn an_expired_entry_cannot_be_taken() {
                let (_guard, store) = super::$start().await;
                store.put(&key("short"), b"v", SHORT).await.unwrap();

                tokio::time::sleep(SHORT * 3).await;

                assert_eq!(store.take(&key("short")).await.unwrap(), None);
            }

            #[tokio::test]
            async fn an_expired_mark_can_be_recorded_again() {
                let (_guard, store) = super::$start().await;
                assert!(store.mark_once(&key("step"), SHORT).await.unwrap());

                tokio::time::sleep(SHORT * 3).await;

                assert!(store.mark_once(&key("step"), LONG).await.unwrap());
            }

            #[tokio::test]
            async fn a_zero_ttl_is_refused_by_every_operation() {
                let (_guard, store) = super::$start().await;

                assert!(matches!(
                    store.put(&key("a"), b"v", Duration::ZERO).await,
                    Err(LoginStoreError::InvalidTtl(_))
                ));
                assert!(matches!(
                    store.mark_once(&key("a"), Duration::ZERO).await,
                    Err(LoginStoreError::InvalidTtl(_))
                ));
            }

            async fn race<F, Fut>(store: &Arc<dyn LoginStore>, attempt: F) -> usize
            where
                F: Fn(Arc<dyn LoginStore>) -> Fut,
                Fut: std::future::Future<Output = bool> + Send + 'static,
            {
                let handles: Vec<_> = (0..CONTENDERS)
                    .map(|_| tokio::spawn(attempt(Arc::clone(store))))
                    .collect();
                let mut winners = 0;
                for handle in handles {
                    winners += usize::from(handle.await.unwrap());
                }
                winners
            }
        }
    };
}
