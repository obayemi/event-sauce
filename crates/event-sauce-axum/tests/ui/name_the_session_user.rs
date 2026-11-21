use event_sauce_axum::SessionUser;

fn main() {
    let _ = std::any::type_name::<SessionUser<()>>();
}
