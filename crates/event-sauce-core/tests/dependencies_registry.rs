//! The generated registry, exercised as a service would use it.
//!
//! `declare_dependencies!` has to be expanded somewhere outside the crate that
//! defines it, because an `inventory` collection needs a type local to the crate
//! collecting it. That is exactly what a service does, so this file plays the
//! part of one: a context, three nodes, two consumers, and the lookups.
//!
//! The graph modelled here is the fan-in/fan-out pair:
//!
//! ```text
//!   Product changed  -> every Building holding a device of it   (fan-out)
//!   Device  changed  -> the one Building holding it             (fan-in)
//!   Synoptic changed -> itself                                  (identity)
//! ```

#![cfg(feature = "dependencies")]
#![allow(clippy::expect_used)]

use std::collections::HashMap;

use async_trait::async_trait;
use event_sauce_core::dependencies::{
    erased_affected, Affected, Dependency, Node, NodeRef, Trigger,
};
use event_sauce_core::{declare_dependencies, EntityId, Result};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The service's ids and nodes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BuildingId(u8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DeviceId(u8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProductId(u8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SynopticId(u8);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LineageId(u8);

fn entity(id: u8) -> EntityId {
    EntityId::from(Uuid::from_u128(u128::from(id)))
}

#[derive(Debug, Clone)]
struct Building;
impl Node for Building {
    const NAME: &'static str = "Building";
    type Key = BuildingId;
}

#[derive(Debug, Clone)]
struct Device;
impl Node for Device {
    const NAME: &'static str = "Device";
    type Key = DeviceId;
}

#[derive(Debug, Clone)]
struct Product;
impl Node for Product {
    const NAME: &'static str = "Product";
    type Key = ProductId;
}

#[derive(Debug, Clone)]
struct Synoptic;
impl Node for Synoptic {
    const NAME: &'static str = "Synoptic";
    type Key = SynopticId;
}

/// Read while building, but a change to it rebuilds nothing. Not an aggregate
/// either — the case a blanket impl over `Aggregate` would have forbidden.
#[derive(Debug, Clone)]
struct ScopeLineage;
impl Node for ScopeLineage {
    const NAME: &'static str = "ScopeLineage";
    type Key = LineageId;
}

// ---------------------------------------------------------------------------
// The context: it carries the reverse lookups, as a service's ports would
// ---------------------------------------------------------------------------

struct SiteCtx {
    /// device -> its building
    building_of_device: HashMap<u8, u8>,
    /// product -> the buildings holding a device of it
    buildings_with_product: HashMap<u8, Vec<u8>>,
}

impl SiteCtx {
    fn estate() -> Self {
        Self {
            // Devices 1 and 2 are in building 10; device 3 is in building 20.
            building_of_device: HashMap::from([(1, 10), (2, 10), (3, 20)]),
            // Product 7 is on devices 1, 2 and 3 — so buildings 10 (twice) and 20.
            buildings_with_product: HashMap::from([(7, vec![10, 10, 20])]),
        }
    }
}

declare_dependencies!(SiteCtx);

// ---------------------------------------------------------------------------
// Consumer 1: the building view. Fan-in from Device, fan-out from Product.
// ---------------------------------------------------------------------------

struct BuildingView;

#[async_trait]
impl Affected<SiteCtx, Device> for BuildingView {
    async fn affected(ctx: &SiteCtx, key: &DeviceId) -> Result<Vec<EntityId>> {
        Ok(ctx
            .building_of_device
            .get(&key.0)
            .map(|building| vec![entity(*building)])
            .unwrap_or_default())
    }
}

#[async_trait]
impl Affected<SiteCtx, Product> for BuildingView {
    async fn affected(ctx: &SiteCtx, key: &ProductId) -> Result<Vec<EntityId>> {
        Ok(ctx
            .buildings_with_product
            .get(&key.0)
            .map(|buildings| buildings.iter().copied().map(entity).collect())
            .unwrap_or_default())
    }
}

static BUILDING_DEPENDS: &[NodeRef] = &[NodeRef::of::<Device>(), NodeRef::of::<Product>()];
static BUILDING_CONSULTS: &[NodeRef] = &[NodeRef::of::<ScopeLineage>()];
static BUILDING_TRIGGERS: &[Trigger<SiteCtx>] = &[
    Trigger {
        node: NodeRef::of::<Device>(),
        affected: erased_affected::<SiteCtx, BuildingView, Device>,
    },
    Trigger {
        node: NodeRef::of::<Product>(),
        affected: erased_affected::<SiteCtx, BuildingView, Product>,
    },
];

inventory::submit! {
    DependencyEntry(Dependency {
        name: "site.v1.Building",
        root: NodeRef::of::<Building>(),
        depends: BUILDING_DEPENDS,
        consults: BUILDING_CONSULTS,
        triggers: BUILDING_TRIGGERS,
    })
}

// ---------------------------------------------------------------------------
// Consumer 2: the synoptic view. Depends on itself only — the identity lookup.
// ---------------------------------------------------------------------------

struct SynopticView;

#[async_trait]
impl Affected<SiteCtx, Synoptic> for SynopticView {
    async fn affected(_ctx: &SiteCtx, key: &SynopticId) -> Result<Vec<EntityId>> {
        Ok(vec![entity(key.0)])
    }
}

static SYNOPTIC_DEPENDS: &[NodeRef] = &[NodeRef::of::<Synoptic>()];
static SYNOPTIC_TRIGGERS: &[Trigger<SiteCtx>] = &[Trigger {
    node: NodeRef::of::<Synoptic>(),
    affected: erased_affected::<SiteCtx, SynopticView, Synoptic>,
}];

inventory::submit! {
    DependencyEntry(Dependency {
        name: "site.v1.Synoptic",
        root: NodeRef::of::<Synoptic>(),
        depends: SYNOPTIC_DEPENDS,
        consults: &[],
        triggers: SYNOPTIC_TRIGGERS,
    })
}

// ---------------------------------------------------------------------------
// The lookups
// ---------------------------------------------------------------------------

fn names(mut found: Vec<&'static str>) -> Vec<&'static str> {
    // The linker's order is not a contract, so a test asserting a list sorts it.
    found.sort_unstable();
    found
}

#[test]
fn all_finds_every_submitted_consumer() {
    assert_eq!(
        names(all().map(|dependency| dependency.name).collect()),
        vec!["site.v1.Building", "site.v1.Synoptic"]
    );
}

#[test]
fn rooted_at_finds_the_consumers_about_a_node() {
    assert_eq!(
        names(rooted_at::<Building>().map(|d| d.name).collect()),
        vec!["site.v1.Building"]
    );
    // Nothing is rooted at Device: a device reaches consumers as a part.
    assert!(rooted_at::<Device>().next().is_none());
}

#[test]
fn depending_on_finds_the_consumers_a_change_makes_stale() {
    assert_eq!(
        names(depending_on::<Product>().map(|d| d.name).collect()),
        vec!["site.v1.Building"]
    );
    assert_eq!(
        names(depending_on::<Synoptic>().map(|d| d.name).collect()),
        vec!["site.v1.Synoptic"]
    );
    // The building view is about Building but does not depend on it here.
    assert!(depending_on::<Building>().next().is_none());
}

#[tokio::test]
async fn a_device_change_makes_one_building_stale() {
    let stale = stale_roots::<Device>(&SiteCtx::estate(), &DeviceId(3))
        .await
        .expect("the reverse lookup answers");

    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].consumer, "site.v1.Building");
    assert_eq!(stale[0].root, NodeRef::of::<Building>());
    assert_eq!(stale[0].root_id, entity(20));
}

#[tokio::test]
async fn a_product_change_fans_out_and_deduplicates() {
    // Product 7 is on three devices, two of which share building 10. That is
    // one rebuild of building 10, not two.
    let stale = stale_roots::<Product>(&SiteCtx::estate(), &ProductId(7))
        .await
        .expect("the reverse lookup answers");

    let mut roots: Vec<Uuid> = stale.iter().map(|entry| entry.root_id.as_uuid()).collect();
    roots.sort_unstable();

    assert_eq!(roots, vec![entity(10).as_uuid(), entity(20).as_uuid()]);
    assert!(stale.iter().all(|e| e.consumer == "site.v1.Building"));
}

#[tokio::test]
async fn the_root_id_is_the_consumers_own_and_not_the_triggers() {
    // A device changed, so the id that travels is a BUILDING id. Returning the
    // device id here is the mistake that files every message under the wrong
    // entity at every consumer.
    let stale = stale_roots::<Device>(&SiteCtx::estate(), &DeviceId(1))
        .await
        .expect("the reverse lookup answers");

    assert_eq!(stale[0].root_id, entity(10));
    assert_ne!(stale[0].root_id, entity(1));
}

#[tokio::test]
async fn a_node_nothing_depends_on_makes_nothing_stale() {
    let stale = stale_roots::<Building>(&SiteCtx::estate(), &BuildingId(10))
        .await
        .expect("an unwatched node is not an error");

    assert!(stale.is_empty());
}

#[tokio::test]
async fn an_unknown_key_makes_nothing_stale() {
    // Terminal, not an error: the reverse lookup simply names no root.
    let stale = stale_roots::<Device>(&SiteCtx::estate(), &DeviceId(99))
        .await
        .expect("an absent key is not an error");

    assert!(stale.is_empty());
}

#[tokio::test]
async fn one_consumer_does_not_answer_for_another() {
    // Synoptic and Building are separate graphs; a synoptic change must not
    // reach the building view.
    let stale = stale_roots::<Synoptic>(&SiteCtx::estate(), &SynopticId(4))
        .await
        .expect("the reverse lookup answers");

    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].consumer, "site.v1.Synoptic");
}

#[test]
fn no_two_dependencies_share_an_id_type() {
    // The rule "one aggregate, one id type" is not something the compiler can
    // impose. This line makes it a guard.
    assert_eq!(
        event_sauce_core::dependencies::conflicting_triggers(all()),
        Vec::new()
    );
}

#[tokio::test]
async fn a_consulted_node_makes_nothing_stale() {
    // `ScopeLineage` is declared in `consults`: the building view reads it while
    // building and claims a change to it cannot alter what is published. So it
    // gets no trigger, and a change to it must produce no rebuild — which is the
    // whole of the difference between the two lists.
    let stale = stale_roots::<ScopeLineage>(&SiteCtx::estate(), &LineageId(10))
        .await
        .expect("a consulted node is not an error");

    assert!(stale.is_empty());
}

#[test]
fn a_consulted_node_is_still_visible_in_the_declaration() {
    // Declared, and therefore reviewable: the silence is a statement someone
    // wrote rather than a node nobody remembered to list.
    let building = all()
        .find(|dependency| dependency.name == "site.v1.Building")
        .expect("the building view is registered");

    assert!(building.consults::<ScopeLineage>());
    assert!(!building.depends_on::<ScopeLineage>());
    assert!(building.touches::<ScopeLineage>());
}
