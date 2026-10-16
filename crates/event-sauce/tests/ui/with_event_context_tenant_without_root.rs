use event_sauce::with_event_context;

#[with_event_context(tenant = "acme")]
fn handler() {}

fn main() {
    handler();
}
