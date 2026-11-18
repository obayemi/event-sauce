//! Registration, login, logout, password change and the profile.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use axum_login::AuthSession;
use event_sauce::auth::{
    Argon2Hasher, Email, LoginId, Logins, Password, PasswordAttempt, PasswordHash, PasswordHasher,
};
use event_sauce::axum::{CurrentUser, Repo, SessionUser};
use event_sauce::postgres::PostgresEventStore;
use event_sauce::prelude::*;
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{json, Value};
use std::result::Result;

use crate::app::{Backend, Factor};
use crate::error::ApiError;
use crate::member::{check_password, Member, MemberCommands};

type Members = Repo<Member, PostgresEventStore>;

#[derive(Deserialize)]
pub struct Registration {
    nickname: String,
    email: String,
    password: String,
}

#[derive(Deserialize)]
pub struct LoginForm {
    email: String,
    password: String,
}

#[derive(Deserialize)]
pub struct NewPassword {
    password: String,
}

pub async fn register(
    Repo(members): Members,
    State(hasher): State<Argon2Hasher>,
    Json(form): Json<Registration>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let email = Email::parse(&form.email)?;
    let hash = hash_password(&hasher, form.password).await?;
    let mut member = Member::join(form.nickname, Password::new(email, hash))?;
    members.save(&mut member).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": member.entity_id() })),
    ))
}

pub async fn login(
    mut auth: AuthSession<Backend>,
    State(logins): State<Logins<Member>>,
    State(first): State<Arc<Factor>>,
    Json(form): Json<LoginForm>,
) -> Result<StatusCode, ApiError> {
    let email = Email::parse(&form.email).map_err(|_| ApiError::Unauthorized)?;
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    let authenticated = logins
        .begin(&first, attempt)
        .await?
        .finish()
        .await
        .map_err(|_| ApiError::Unauthorized)?;
    auth.login(&SessionUser::from(authenticated)).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn logout(mut auth: AuthSession<Backend>) -> Result<StatusCode, ApiError> {
    auth.logout().await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn change_password(
    Repo(members): Members,
    State(hasher): State<Argon2Hasher>,
    member: CurrentUser<Backend>,
    Json(form): Json<NewPassword>,
) -> Result<StatusCode, ApiError> {
    let hash = hash_password(&hasher, form.password).await?;
    members
        .modify(member.entity_id(), |member| member.change_password(hash))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn me(member: CurrentUser<Backend>) -> Json<Value> {
    Json(json!({
        "id": member.entity_id(),
        "nickname": member.nickname,
        "email": member.email().map(Email::as_str),
    }))
}

async fn hash_password(hasher: &Argon2Hasher, password: String) -> Result<PasswordHash, ApiError> {
    check_password(&password)?;
    Ok(hasher.hash(&SecretString::from(password)).await?)
}
