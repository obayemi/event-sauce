//! The dependency graph between published payloads and the aggregates they read.
//!
//! A service publishes payloads that join several aggregates together. When one
//! of those aggregates changes, the payloads that read it are stale — including
//! payloads that are *about* something else entirely. Answering "what is now
//! stale, and which roots do I rebuild?" needs a declared graph and a reverse
//! lookup, and that is all this module is.
//!
//! # What this module deliberately does not know
//!
//! It knows [`Node`]s, edges between them, and keys. It does **not** know what a
//! payload is: there is no envelope here, no serde tag, no schema version and no
//! transport. Those belong to whatever publishes; the moment this module learns
//! them, the seam has leaked and the graph stops being reusable for in-transaction
//! projections or reactions.
//!
//! # The shape
//!
//! ```text
//!   Node          a vertex: an aggregate, a projection row, an entity received
//!                 over a bus. Anything with a name and a key.
//!   Source<N>     how a context READS one node. Implemented on the CONTEXT.
//!   Affected<_,N> the reverse edge: this node changed -> which roots are stale?
//!   Dependency    one declared consumer: its root, what it depends on, and the
//!                 erased bridge to each reverse lookup.
//! ```
//!
//! [`declare_dependencies!`](crate::declare_dependencies) puts the registry in
//! the caller's module, because an `inventory` collection needs a type local to
//! the crate that collects it.

use std::any::{Any, TypeId};
use std::fmt;
use std::hash::Hash;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{EntityId, Result};

/// Re-exported so [`declare_dependencies!`](crate::declare_dependencies) can name
/// it without the caller taking a direct dependency.
#[doc(hidden)]
pub use inventory;

/// A vertex of the read graph.
///
/// # Implemented per type, never blanket
///
/// It is tempting to write `impl<A: Aggregate> Node for A` and be done. Doing so
/// makes every hand-written `impl Node for SomeProjectionRow` illegal, because
/// the compiler cannot prove that the row is *not* an `Aggregate` — Rust has no
/// negative reasoning. Since a projection row, a lineage view and an entity
/// received over a bus are all legitimate vertices, the impl is emitted **per
/// type** instead.
///
/// # Why `Clone`
///
/// One node is read by more than one payload of a single build. A memo hands the
/// second reader a clone. Put a node with a high cost of copy behind an `Arc`.
pub trait Node: Clone + Send + Sync + Sized + 'static {
    /// The name of this node in a trace, an error and the registry.
    ///
    /// For an aggregate this is its aggregate type name.
    const NAME: &'static str;

    /// What identifies one instance.
    ///
    /// `Hash + Eq` so a memo can index on it. `Debug` because an error saying
    /// "a device is absent" without saying *which* is an error nobody can act on.
    type Key: Clone + Eq + Hash + fmt::Debug + Send + Sync + 'static;
}

/// The designation of a [`Node`] inside a declaration.
///
/// It carries the name for display and the exact identity of the type for
/// comparison. Comparison uses the [`TypeId`] and never the name, because two
/// nodes in different modules can share a short name and a registry that mixes
/// them answers the wrong question.
#[derive(Clone, Copy)]
pub struct NodeRef {
    name: &'static str,
    type_id: fn() -> TypeId,
    key_type_id: fn() -> TypeId,
}

impl NodeRef {
    /// The designation of the node `N`.
    #[must_use]
    pub const fn of<N: Node>() -> Self {
        Self {
            name: N::NAME,
            type_id: TypeId::of::<N>,
            key_type_id: TypeId::of::<N::Key>,
        }
    }

    /// The name of the node.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The exact identity of the node type.
    #[must_use]
    pub fn type_id(&self) -> TypeId {
        (self.type_id)()
    }

    /// The exact identity of the node's key type.
    ///
    /// Used by [`conflicting_triggers`] to see two nodes that share an id type,
    /// which the compiler cannot refuse but which makes one reverse lookup serve
    /// two nodes in silence.
    #[must_use]
    pub fn key_type_id(&self) -> TypeId {
        (self.key_type_id)()
    }

    /// `true` if this designates `N`.
    #[must_use]
    pub fn is<N: Node>(&self) -> bool {
        self.type_id() == TypeId::of::<N>()
    }
}

impl fmt::Debug for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

impl fmt::Display for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

impl PartialEq for NodeRef {
    fn eq(&self, other: &Self) -> bool {
        self.type_id() == other.type_id()
    }
}

impl Eq for NodeRef {}

/// How a context reads one node.
///
/// # Implemented on the context, not on the node
///
/// `Source` is foreign to a service and so is an aggregate, but the context is
/// **local** to the crate writing the impl, so the orphan rule is satisfied by
/// the left-hand side. That also means no blanket impl is needed and no
/// coherence conflict can arise with a node the kit has never heard of.
///
/// A node backed by an aggregate reads the same repository the use cases read.
/// There is no repository for published payloads alone.
#[async_trait]
pub trait Source<N: Node>: Send + Sync {
    /// Reads one instance, or `None` when the key names nothing.
    ///
    /// The distinction matters to the caller: `None` is terminal — the key names
    /// nothing on the next pass either — while an `Err` is usually transient.
    /// Collapsing them is how one payload about a purged entity stops an outbox.
    ///
    /// # Errors
    ///
    /// Whatever the underlying source gives when it cannot answer.
    async fn fetch(&self, key: &N::Key) -> Result<Option<N>>;
}

/// The reverse edge: this node changed, so which roots are now stale?
///
/// One impl per (consumer, node) pair. Each receives the **typed** key, so a
/// service writes no downcast, no match on a marker and no chain of `if`.
///
/// An empty vector is a correct answer: the node changed and no root of this
/// consumer names it.
#[async_trait]
pub trait Affected<Ctx, N: Node> {
    /// The ids of the roots to rebuild.
    ///
    /// # Errors
    ///
    /// Whatever the reverse query gives when it cannot answer.
    async fn affected(ctx: &Ctx, key: &N::Key) -> Result<Vec<EntityId>>;
}

/// The erased signature a registration holds.
///
/// Never written in the code of a service: a declaration macro builds the entry
/// and [`erased_affected`] performs the single downcast of the mechanism, at the
/// one place where it cannot fail.
pub type ErasedAffected<Ctx> =
    for<'a> fn(&'a Ctx, &'a (dyn Any + Send + Sync)) -> BoxFuture<'a, Result<Vec<EntityId>>>;

/// The bridge from the typed [`Affected`] impl to an entry of the registry.
///
/// A declaration macro writes `erased_affected::<Ctx, D, N>` for each declared
/// dependency. The downcast is performed for the exact key type the entry was
/// built for, and [`Dependency`] lookups only ever call an entry with that type,
/// so it cannot fail in practice.
///
/// # Panics
///
/// Never. A key of the wrong type yields an empty result rather than aborting,
/// because a registry lookup is not the place to discover a wiring bug — the
/// declaration macro is, and it checks at compile time.
#[doc(hidden)]
pub fn erased_affected<'a, Ctx, D, N>(
    ctx: &'a Ctx,
    key: &'a (dyn Any + Send + Sync),
) -> BoxFuture<'a, Result<Vec<EntityId>>>
where
    Ctx: Sync,
    N: Node,
    D: Affected<Ctx, N>,
{
    Box::pin(async move {
        match key.downcast_ref::<N::Key>() {
            Some(typed) => D::affected(ctx, typed).await,
            None => Ok(Vec::new()),
        }
    })
}

/// One node that makes a consumer stale, and how to find the affected roots.
pub struct Trigger<Ctx: 'static> {
    /// The node whose change fires this trigger.
    pub node: NodeRef,
    /// The bridge to the [`Affected`] impl for that node.
    pub affected: ErasedAffected<Ctx>,
}

impl<Ctx: 'static> fmt::Debug for Trigger<Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `affected` is a fn pointer and has no useful Debug; the node it is
        // bridged for is the part worth seeing.
        f.debug_struct("Trigger")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// One declared consumer of the graph.
///
/// # `depends` and `consults` are not the same read
///
/// Both are read while building. Only `depends` makes this consumer stale, and
/// only `depends` needs an [`Affected`] impl. `consults` is a claim that a change
/// to that node cannot alter what is published — it is the claim to challenge in
/// review, because getting it wrong is the silent failure. When in doubt, depend:
/// the cost of being wrong that way is a redundant rebuild, and the cost of being
/// wrong the other way is a consumer stale for ever.
pub struct Dependency<Ctx: 'static> {
    /// A stable identifier for this consumer, unique within a registry.
    pub name: &'static str,
    /// The node this consumer is *about*. It supplies the root id.
    pub root: NodeRef,
    /// Nodes whose change rebuilds this consumer. Each has a [`Trigger`].
    pub depends: &'static [NodeRef],
    /// Nodes read while building whose change rebuilds nothing.
    pub consults: &'static [NodeRef],
    /// One bridge per entry of `depends`, in the same order.
    pub triggers: &'static [Trigger<Ctx>],
}

impl<Ctx: 'static> Dependency<Ctx> {
    /// `true` if this consumer is *about* `N`.
    #[must_use]
    pub fn rooted_at<N: Node>(&self) -> bool {
        self.root.is::<N>()
    }

    /// `true` if a change to `N` rebuilds this consumer.
    #[must_use]
    pub fn depends_on<N: Node>(&self) -> bool {
        let target = TypeId::of::<N>();
        self.depends.iter().any(|node| node.type_id() == target)
    }

    /// `true` if this consumer reads `N` without depending on it.
    #[must_use]
    pub fn consults<N: Node>(&self) -> bool {
        let target = TypeId::of::<N>();
        self.consults.iter().any(|node| node.type_id() == target)
    }

    /// `true` if this consumer reads `N` at all, either way.
    #[must_use]
    pub fn touches<N: Node>(&self) -> bool {
        self.depends_on::<N>() || self.consults::<N>()
    }
}

impl<Ctx: 'static> fmt::Debug for Dependency<Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dependency")
            .field("name", &self.name)
            .field("root", &self.root)
            .field("depends", &self.depends)
            .field("consults", &self.consults)
            // `triggers` mirrors `depends` and holds only fn pointers.
            .finish_non_exhaustive()
    }
}

/// One root to rebuild, and which consumer asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stale {
    /// The `name` of the [`Dependency`] that is now stale.
    pub consumer: &'static str,
    /// The node that consumer is about.
    pub root: NodeRef,
    /// The identity of the root instance to rebuild.
    pub root_id: EntityId,
}

/// Two dependencies of one consumer whose nodes share a key type.
///
/// The compiler cannot refuse this: `Affected<Ctx, A>` and `Affected<Ctx, B>`
/// are satisfied by one impl when `A::Key == B::Key`, so one reverse lookup
/// serves two nodes and the other is never called. [`conflicting_triggers`]
/// finds the pairs so a test can make "one aggregate, one id type" a guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyClash {
    /// The consumer whose declaration holds both.
    pub consumer: &'static str,
    /// The first of the two nodes.
    pub left: NodeRef,
    /// The second.
    pub right: NodeRef,
}

/// Every pair of declared dependencies within one consumer that shares a key type.
///
/// An empty result is the healthy answer. Assert it in a test:
///
/// ```ignore
/// #[test]
/// fn no_two_dependencies_share_an_id_type() {
///     assert_eq!(conflicting_triggers(all()), Vec::new());
/// }
/// ```
#[must_use]
pub fn conflicting_triggers<'a, Ctx: 'static>(
    dependencies: impl Iterator<Item = &'a Dependency<Ctx>>,
) -> Vec<KeyClash> {
    let mut clashes = Vec::new();
    for dependency in dependencies {
        for (index, left) in dependency.depends.iter().enumerate() {
            for right in dependency.depends.iter().skip(index + 1) {
                if left.key_type_id() == right.key_type_id() {
                    clashes.push(KeyClash {
                        consumer: dependency.name,
                        left: *left,
                        right: *right,
                    });
                }
            }
        }
    }
    clashes
}

/// Puts a dependency registry in the calling module.
///
/// An `inventory` collection needs a type local to the crate that collects it,
/// so the registry cannot live in this crate and the macro generates it where it
/// is used. `$ctx` is the service's context type — the one carrying the ports.
///
/// It declares `DependencyEntry`, its collection, and four lookups: `all`,
/// `rooted_at`, `depending_on` and `stale_roots`.
///
/// ```ignore
/// declare_dependencies!(SiteCtx);
///
/// inventory::submit! { DependencyEntry(Dependency { .. }) }
///
/// let stale = stale_roots::<Product>(&ctx, &product_id).await?;
/// ```
#[macro_export]
macro_rules! declare_dependencies {
    ($ctx:ty $(,)?) => {
        /// One registered consumer of the dependency graph.
        ///
        /// A new type and not an alias: an `inventory` collection puts an impl
        /// on the type it collects, and the orphan rule forbids that on a type
        /// of `event-sauce-core`.
        pub struct DependencyEntry(pub $crate::dependencies::Dependency<$ctx>);

        $crate::dependencies::inventory::collect!(DependencyEntry);

        /// Every declared consumer, in the order the linker used.
        ///
        /// That order is not a contract. A test asserting a list must sort first.
        pub fn all() -> impl Iterator<Item = &'static $crate::dependencies::Dependency<$ctx>> {
            $crate::dependencies::inventory::iter::<DependencyEntry>
                .into_iter()
                .map(|entry| &entry.0)
        }

        /// The consumers that are *about* the node `N`.
        pub fn rooted_at<N: $crate::dependencies::Node>(
        ) -> impl Iterator<Item = &'static $crate::dependencies::Dependency<$ctx>> {
            all().filter(|dependency| dependency.rooted_at::<N>())
        }

        /// The consumers a change to `N` makes stale.
        pub fn depending_on<N: $crate::dependencies::Node>(
        ) -> impl Iterator<Item = &'static $crate::dependencies::Dependency<$ctx>> {
            all().filter(|dependency| dependency.depends_on::<N>())
        }

        /// Every root to rebuild after `N` with this key changed.
        ///
        /// More than one path can reach one root — two devices of one building
        /// sharing a product — and the result is deduplicated, because that is
        /// one rebuild and not two.
        ///
        /// # Errors
        ///
        /// The first error any reverse lookup gives.
        pub async fn stale_roots<N>(
            ctx: &$ctx,
            key: &N::Key,
        ) -> $crate::Result<::std::vec::Vec<$crate::dependencies::Stale>>
        where
            N: $crate::dependencies::Node,
        {
            let changed = $crate::dependencies::NodeRef::of::<N>();
            let erased: &(dyn ::std::any::Any + Send + Sync) = key;
            let mut stale: ::std::vec::Vec<$crate::dependencies::Stale> = ::std::vec::Vec::new();

            for dependency in all() {
                for trigger in dependency.triggers {
                    if trigger.node != changed {
                        continue;
                    }
                    for root_id in (trigger.affected)(ctx, erased).await? {
                        let entry = $crate::dependencies::Stale {
                            consumer: dependency.name,
                            root: dependency.root,
                            root_id,
                        };
                        if !stale.contains(&entry) {
                            stale.push(entry);
                        }
                    }
                }
            }
            Ok(stale)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- nodes -------------------------------------------------------------

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct BuildingId(u32);
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct DeviceId(u32);
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct ProductId(u32);

    #[derive(Debug, Clone, PartialEq)]
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

    /// A node that is not an aggregate — the case a blanket impl would forbid.
    #[derive(Debug, Clone)]
    struct ScopeLineage;
    impl Node for ScopeLineage {
        const NAME: &'static str = "ScopeLineage";
        type Key = BuildingId;
    }

    // ---- NodeRef -----------------------------------------------------------

    #[test]
    fn node_ref_carries_the_node_name() {
        assert_eq!(NodeRef::of::<Building>().name(), "Building");
    }

    #[test]
    fn node_ref_of_the_same_node_is_equal() {
        assert_eq!(NodeRef::of::<Building>(), NodeRef::of::<Building>());
    }

    #[test]
    fn node_ref_of_different_nodes_is_not_equal() {
        assert_ne!(NodeRef::of::<Building>(), NodeRef::of::<Device>());
    }

    #[test]
    fn node_ref_compares_on_type_and_not_on_name() {
        // Two nodes with the same NAME must still be distinct designations,
        // because a registry that mixes them answers the wrong question.
        #[derive(Debug, Clone)]
        struct OtherBuilding;
        impl Node for OtherBuilding {
            const NAME: &'static str = "Building";
            type Key = BuildingId;
        }

        assert_eq!(NodeRef::of::<OtherBuilding>().name(), "Building");
        assert_ne!(NodeRef::of::<Building>(), NodeRef::of::<OtherBuilding>());
    }

    #[test]
    fn node_ref_is_identifies_its_own_node() {
        let building = NodeRef::of::<Building>();
        assert!(building.is::<Building>());
        assert!(!building.is::<Device>());
    }

    #[test]
    fn node_ref_exposes_the_key_type() {
        assert_eq!(
            NodeRef::of::<Building>().key_type_id(),
            TypeId::of::<BuildingId>()
        );
        assert_ne!(
            NodeRef::of::<Building>().key_type_id(),
            NodeRef::of::<Device>().key_type_id()
        );
    }

    #[test]
    fn node_ref_displays_as_its_name() {
        assert_eq!(NodeRef::of::<Device>().to_string(), "Device");
        assert_eq!(format!("{:?}", NodeRef::of::<Device>()), "Device");
    }

    // ---- Dependency --------------------------------------------------------

    struct Ctx;

    struct BuildingView;

    #[async_trait]
    impl Affected<Ctx, Device> for BuildingView {
        async fn affected(_ctx: &Ctx, key: &DeviceId) -> Result<Vec<EntityId>> {
            // One device sits in one building: fan-in.
            Ok(vec![EntityId::from(uuid::Uuid::from_u128(u128::from(
                key.0,
            )))])
        }
    }

    #[async_trait]
    impl Affected<Ctx, Product> for BuildingView {
        async fn affected(_ctx: &Ctx, key: &ProductId) -> Result<Vec<EntityId>> {
            // One product reaches several buildings: fan-out. The duplicate is
            // deliberate — two devices of one building share a product.
            let one = EntityId::from(uuid::Uuid::from_u128(u128::from(key.0)));
            let two = EntityId::from(uuid::Uuid::from_u128(u128::from(key.0) + 1));
            Ok(vec![one, two, one])
        }
    }

    static BUILDING_VIEW_DEPENDS: &[NodeRef] = &[NodeRef::of::<Device>(), NodeRef::of::<Product>()];
    static BUILDING_VIEW_CONSULTS: &[NodeRef] = &[NodeRef::of::<ScopeLineage>()];
    static BUILDING_VIEW_TRIGGERS: &[Trigger<Ctx>] = &[
        Trigger {
            node: NodeRef::of::<Device>(),
            affected: erased_affected::<Ctx, BuildingView, Device>,
        },
        Trigger {
            node: NodeRef::of::<Product>(),
            affected: erased_affected::<Ctx, BuildingView, Product>,
        },
    ];

    fn building_view() -> Dependency<Ctx> {
        Dependency {
            name: "site.v1.Building",
            root: NodeRef::of::<Building>(),
            depends: BUILDING_VIEW_DEPENDS,
            consults: BUILDING_VIEW_CONSULTS,
            triggers: BUILDING_VIEW_TRIGGERS,
        }
    }

    #[test]
    fn dependency_knows_its_root() {
        let dependency = building_view();
        assert!(dependency.rooted_at::<Building>());
        assert!(!dependency.rooted_at::<Device>());
    }

    #[test]
    fn dependency_separates_depends_from_consults() {
        let dependency = building_view();

        assert!(dependency.depends_on::<Device>());
        assert!(!dependency.consults::<Device>());

        assert!(dependency.consults::<ScopeLineage>());
        assert!(!dependency.depends_on::<ScopeLineage>());
    }

    #[test]
    fn touches_covers_both_kinds_of_read() {
        let dependency = building_view();
        assert!(dependency.touches::<Device>());
        assert!(dependency.touches::<ScopeLineage>());
        assert!(!dependency.touches::<Building>());
    }

    #[test]
    fn a_node_the_consumer_never_reads_is_neither() {
        let dependency = building_view();
        assert!(!dependency.depends_on::<Building>());
        assert!(!dependency.consults::<Building>());
    }

    // ---- the erased bridge -------------------------------------------------

    #[tokio::test]
    async fn erased_bridge_calls_through_with_the_typed_key() {
        let key = DeviceId(7);
        let erased: &(dyn Any + Send + Sync) = &key;

        let roots = erased_affected::<Ctx, BuildingView, Device>(&Ctx, erased)
            .await
            .expect("the reverse lookup answers");

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], EntityId::from(uuid::Uuid::from_u128(7)));
    }

    #[tokio::test]
    async fn erased_bridge_yields_nothing_for_a_key_of_another_type() {
        // Unreachable through a registry lookup, which only ever calls an entry
        // with the key type it was built for. Asserted so it stays harmless.
        let wrong = ProductId(7);
        let erased: &(dyn Any + Send + Sync) = &wrong;

        let roots = erased_affected::<Ctx, BuildingView, Device>(&Ctx, erased)
            .await
            .expect("a mismatched key is not an error");

        assert!(roots.is_empty());
    }

    // ---- the key-clash guard ------------------------------------------------

    #[test]
    fn conflicting_triggers_is_empty_when_every_key_type_differs() {
        let dependency = building_view();
        assert_eq!(
            conflicting_triggers(std::iter::once(&dependency)),
            Vec::new()
        );
    }

    #[test]
    fn conflicting_triggers_sees_two_nodes_sharing_a_key_type() {
        // ScopeLineage and Building are different nodes keyed by BuildingId.
        // The compiler accepts one Affected impl for both; this does not.
        static CLASHING: &[NodeRef] = &[NodeRef::of::<Building>(), NodeRef::of::<ScopeLineage>()];

        let dependency: Dependency<Ctx> = Dependency {
            name: "clashing",
            root: NodeRef::of::<Building>(),
            depends: CLASHING,
            consults: &[],
            triggers: &[],
        };

        let clashes = conflicting_triggers(std::iter::once(&dependency));

        assert_eq!(clashes.len(), 1);
        assert_eq!(clashes[0].consumer, "clashing");
        assert_eq!(clashes[0].left, NodeRef::of::<Building>());
        assert_eq!(clashes[0].right, NodeRef::of::<ScopeLineage>());
    }

    #[test]
    fn conflicting_triggers_ignores_consults() {
        // `consults` needs no Affected impl, so a shared key type there is
        // harmless: nothing is ever dispatched on it.
        static DEPENDS: &[NodeRef] = &[NodeRef::of::<Device>()];
        static CONSULTS: &[NodeRef] = &[NodeRef::of::<Building>(), NodeRef::of::<ScopeLineage>()];

        let dependency: Dependency<Ctx> = Dependency {
            name: "consults-only-clash",
            root: NodeRef::of::<Building>(),
            depends: DEPENDS,
            consults: CONSULTS,
            triggers: &[],
        };

        assert_eq!(
            conflicting_triggers(std::iter::once(&dependency)),
            Vec::new()
        );
    }
}
