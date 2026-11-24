use std::sync::Arc;

use super::*;
use crate::{FederatedId, Provider};

struct Scripted;

#[async_trait::async_trait]
impl OAuthProvider for Scripted {
    type Profile = String;
    type Error = std::convert::Infallible;

    fn authorize(&self) -> Authorization {
        Authorization {
            url: "https://provider.test/consent".to_owned(),
            state: OAuthState::new("state"),
            pkce_verifier: PkceVerifier::new("verifier"),
        }
    }

    async fn identify(
        &self,
        code: AuthorizationCode,
        verifier: PkceVerifier,
    ) -> Result<FederatedAccount<String>, Self::Error> {
        Ok(FederatedAccount {
            id: FederatedId::new(Provider::GOOGLE, code.as_str()),
            profile: verifier.as_str().to_owned(),
        })
    }
}

#[test]
fn a_state_equals_only_the_same_value() {
    assert_eq!(OAuthState::new("a"), OAuthState::new("a"));
    assert_ne!(OAuthState::new("a"), OAuthState::new("b"));
    assert_ne!(OAuthState::new("a"), OAuthState::new("ab"));
}

#[test]
fn a_state_exposes_its_value() {
    assert_eq!(OAuthState::new("abc").as_str(), "abc");
}

#[test]
fn secrets_serialize_as_the_bare_string() {
    let json = serde_json::to_string(&OAuthState::new("abc")).unwrap();
    assert_eq!(json, "\"abc\"");
    let verifier: PkceVerifier = serde_json::from_str("\"v\"").unwrap();
    assert_eq!(verifier, PkceVerifier::new("v"));
    let code: AuthorizationCode = serde_json::from_str("\"c\"").unwrap();
    assert_eq!(code.as_str(), "c");
}

#[test]
fn debug_never_prints_a_secret() {
    for printed in [
        format!("{:?}", OAuthState::new("hunter2")),
        format!("{:?}", PkceVerifier::new("hunter2")),
        format!("{:?}", AuthorizationCode::new("hunter2")),
    ] {
        assert!(!printed.contains("hunter2"), "{printed}");
    }
}

#[tokio::test]
async fn a_provider_is_used_through_a_shared_pointer() {
    let provider: Arc<dyn OAuthProvider<Profile = String, Error = std::convert::Infallible>> =
        Arc::new(Scripted);

    let authorization = provider.authorize();
    let account = provider
        .identify(AuthorizationCode::new("sub-1"), authorization.pkce_verifier)
        .await
        .unwrap();

    assert_eq!(authorization.state, OAuthState::new("state"));
    assert_eq!(account.id, FederatedId::new(Provider::GOOGLE, "sub-1"));
    assert_eq!(account.profile, "verifier");
}
