use axum_login::tower_sessions::Session;
use event_sauce_auth::{Authorization, OAuthState, PkceVerifier};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::pages::CallbackError;
use crate::diagnostics;
use crate::return_to::ReturnTo;

pub(super) const STATE: &str = "event_sauce.oauth.state";
pub(super) const PKCE: &str = "event_sauce.oauth.pkce";
pub(super) const RETURN_TO: &str = "event_sauce.oauth.return_to";
pub(super) const LINK: &str = "event_sauce.oauth.link";
pub(super) const CLAIM: &str = "event_sauce.oauth.claim";

pub(super) async fn put<T: Serialize>(session: &Session, key: &str, value: T) {
    if let Err(error) = session.insert(key, value).await {
        diagnostics::internal(&error);
    }
}

pub(super) async fn take<T: DeserializeOwned>(session: &Session, key: &str) -> Option<T> {
    match session.remove(key).await {
        Ok(value) => value,
        Err(error) => {
            diagnostics::internal(&error);
            None
        }
    }
}

pub(super) async fn start(
    session: &Session,
    authorization: &Authorization,
    return_to: Option<&ReturnTo>,
) {
    put(session, STATE, &authorization.state).await;
    put(session, PKCE, &authorization.pkce_verifier).await;
    match return_to {
        Some(return_to) => put(session, RETURN_TO, return_to.as_str()).await,
        None => drop(take::<String>(session, RETURN_TO).await),
    }
}

pub(super) struct Flow {
    pub(super) pkce_verifier: PkceVerifier,
    pub(super) return_to: Option<ReturnTo>,
}

pub(super) async fn take_flow<E, F>(
    session: &Session,
    returned: &OAuthState,
) -> Result<Flow, CallbackError<E, F>> {
    if take::<OAuthState>(session, STATE).await.as_ref() != Some(returned) {
        return Err(CallbackError::State);
    }
    let pkce_verifier = take(session, PKCE).await.ok_or(CallbackError::Expired)?;
    let return_to = take::<String>(session, RETURN_TO)
        .await
        .and_then(|raw| ReturnTo::parse(&raw));
    Ok(Flow {
        pkce_verifier,
        return_to,
    })
}
