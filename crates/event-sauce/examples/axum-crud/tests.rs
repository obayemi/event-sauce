//! The domain rules of the example, then its whole HTTP surface driven
//! through the router against a real `PostgreSQL` container.

use argon2::Params;
use event_sauce::auth::Argon2Hasher;

mod domain;
mod harness;
mod http;

const EMAIL: &str = "ada@example.com";
const PASSWORD: &str = "correct-horse";

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}
