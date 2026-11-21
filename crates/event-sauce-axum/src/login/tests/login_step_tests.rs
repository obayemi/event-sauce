use axum::response::IntoResponse;
use event_sauce_auth::{Done, LoginState, LoginTtl, PendingLoginId};
use http::{header::SET_COOKIE, Response, StatusCode};
use serde::{Deserialize, Serialize};

use super::LoginStep;
use crate::login::PendingLogin;

#[derive(Serialize, Deserialize)]
struct Brief;

impl LoginState for Brief {
    const TAG: &'static str = "test.brief";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

fn cookies<B>(response: &Response<B>) -> Vec<String> {
    response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect()
}

#[test]
fn an_opened_step_answers_no_content_and_clears_the_pending_cookie() {
    let response = LoginStep::opened().into_response();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(cookies(&response), [PendingLogin::clear_cookie()]);
}

#[test]
fn a_pending_step_answers_accepted_with_the_cookie_of_its_id() {
    let id = PendingLoginId::new();

    let response = LoginStep::waiting::<Done>(id).into_response();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let [cookie] = cookies(&response).try_into().unwrap();
    assert!(cookie.starts_with(&format!("{}={id};", PendingLogin::COOKIE)));
}

#[test]
fn the_pending_cookie_lives_as_long_as_the_state_the_login_waits_in() {
    let five_minutes = LoginStep::waiting::<Done>(PendingLoginId::new()).into_response();
    let one_hour = LoginStep::waiting::<Brief>(PendingLoginId::new()).into_response();

    assert!(cookies(&five_minutes)[0].contains("Max-Age=300"));
    assert!(cookies(&one_hour)[0].contains("Max-Age=3600"));
}

#[test]
fn a_step_reports_whether_it_opened_and_which_login_waits() {
    let id = PendingLoginId::new();

    assert!(LoginStep::opened().is_opened());
    assert_eq!(LoginStep::opened().pending(), None);
    assert!(!LoginStep::waiting::<Done>(id).is_opened());
    assert_eq!(LoginStep::waiting::<Done>(id).pending(), Some(id));
}

fn parts_of(step: LoginStep) -> axum::response::Response {
    (step, StatusCode::IM_A_TEAPOT).into_response()
}

#[test]
fn as_response_parts_a_step_adds_its_cookie_and_nothing_else() {
    let pending = parts_of(LoginStep::waiting::<Done>(PendingLoginId::new()));
    let opened = parts_of(LoginStep::opened());

    assert_eq!(pending.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(cookies(&pending).len(), 1);
    assert!(cookies(&pending)[0].starts_with(PendingLogin::COOKIE));
    assert_eq!(cookies(&opened), [PendingLogin::clear_cookie()]);
}
