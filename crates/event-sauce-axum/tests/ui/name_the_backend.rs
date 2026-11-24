use event_sauce_axum::EventSauceBackend;

fn main() {
    let _ = std::any::type_name::<EventSauceBackend<(), ()>>();
}
