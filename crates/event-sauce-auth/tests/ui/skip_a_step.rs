use async_trait::async_trait;
use event_sauce_auth::{Advance, Authenticated, FinishError, Login, LoginState, Rejected, Step, UserAggregate};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct First;

#[derive(Serialize, Deserialize)]
struct Second;

impl LoginState for First {
    const TAG: &'static str = "ui.first";
}

impl LoginState for Second {
    const TAG: &'static str = "ui.second";
}

#[async_trait]
impl<U: UserAggregate> Step<U> for First {
    type Proof = ();
    type Verifier = ();
    type Next = Login<U, Second>;

    async fn verify(advance: Advance<U, Self>, _: &(), _: ()) -> Result<Login<U, Second>, Rejected> {
        Ok(advance.to(Second))
    }
}

async fn skip<U: UserAggregate>(login: Login<U, First>) -> Result<Authenticated<U>, FinishError> {
    let second = login.prove(&(), ()).await.unwrap();
    second.finish().await
}

fn main() {}
