use event_sauce_auth::{FederatedAccount, FederatedId, Provider};

use super::*;

struct Bare;

#[async_trait]
impl OAuthAccounts for Bare {
    type Profile = ();
    type Refusal = ();

    async fn unknown(&self, _: &FederatedAccount<()>) -> Result<(), ()> {
        Err(())
    }

    async fn link(&self, _: EntityId, _: FederatedAccount<()>, _: LinkProof) -> LinkOutcome {
        LinkOutcome::Failed
    }

    async fn claim(&self, _: EntityId, _: FederatedAccount<()>) -> bool {
        false
    }
}

#[tokio::test]
async fn settling_an_unknown_account_does_nothing_by_default() {
    Bare.reconcile().await;

    let account = FederatedAccount {
        id: FederatedId::new(Provider::GOOGLE, "sub"),
        profile: (),
    };
    assert_eq!(Bare.unknown(&account).await, Err(()));
}
