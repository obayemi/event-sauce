//! The backend-independent contract shared by the login store integration
//! tests.

/// Generates the tests every `LoginStore` backend must pass.
///
/// `$start` is an async function returning `(guard, Arc<dyn LoginStore>)`,
/// where `guard` keeps the test's slice of the backing server for the test.
#[macro_export]
macro_rules! login_store_contract {
    ($start:ident) => {
        mod contract {
            use std::num::NonZeroU32;
            use std::sync::Arc;

            use event_sauce_auth::{LoginKey, LoginStore, LoginTtl};

            const LONG: LoginTtl = LoginTtl::ONE_MINUTE;
            const SHORT: LoginTtl = LoginTtl::from_millis(NonZeroU32::new(300).unwrap());
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

                tokio::time::sleep(SHORT.as_duration() * 3).await;

                assert_eq!(store.take(&key("short")).await.unwrap(), None);
            }

            #[tokio::test]
            async fn an_expired_mark_can_be_recorded_again() {
                let (_guard, store) = super::$start().await;
                assert!(store.mark_once(&key("step"), SHORT).await.unwrap());

                tokio::time::sleep(SHORT.as_duration() * 3).await;

                assert!(store.mark_once(&key("step"), LONG).await.unwrap());
            }

            #[tokio::test]
            async fn the_longest_ttl_is_honoured_by_every_operation() {
                let (_guard, store) = super::$start().await;

                store.put(&key("a"), b"v", LoginTtl::MAX).await.unwrap();
                assert!(store.mark_once(&key("b"), LoginTtl::MAX).await.unwrap());

                assert_eq!(store.take(&key("a")).await.unwrap(), Some(b"v".to_vec()));
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
