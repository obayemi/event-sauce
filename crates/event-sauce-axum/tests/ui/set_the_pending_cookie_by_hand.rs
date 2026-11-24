use event_sauce_auth::Done;
use event_sauce_axum::PendingLogin;

fn main() {
    let _ = PendingLogin::set_cookie::<Done>(event_sauce_auth::PendingLoginId::new());
}
