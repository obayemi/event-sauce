//! [`define_events!`](crate::define_events): its TT-muncher parse phase, its
//! per-kind emit phase, and the tests for both.
//!
//! The macro runs in two phases — *parse* (TT muncher) and *emit* (per-kind
//! trait emitter). Both treat "kind" and "has an actor" as independent axes:
//! a variant's kind is exactly one of `regular` / `init` / `delete`, and
//! `@actor(T)` only ever adds an extra trait impl on top of that kind's
//! ordinary one — it never changes which kind the variant is, or how it
//! replays.
//!
//! 1. **Parse phase**, itself split in two steps so the hook grammar is
//!    written once instead of once per kind:
//!
//!    - **Classify** (the `@munch` arms): six tiny arms match only the
//!      variant's marker tokens — structural, not lookahead, so a marker
//!      that requires a literal `@init`/`@delete`/`@actor(Ty)` token must be
//!      tried before the marker-less catch-all — and forward to `@hooks`
//!      with the kind tag and actor type resolved. See the table above the
//!      `@munch` arms below for the marker-to-kind mapping.
//!
//!    - **Parse hooks** (the `@hooks` arms): the eight optional clauses
//!      (`@version`, `@occurred_at`, `@validate`, `@validate_spec`,
//!      `@post_validate`, `@post_validate_spec`, `@encrypted_fields`,
//!      `@aliases`) and the `=> apply` closure are parsed once, kind-agnostic,
//!      and pushed into the normalised metadata blob (variant name, fields,
//!      kind, optional actor type, version, `validate`/`post_validate`/spec
//!      hooks, encrypted fields, apply closure). Two arms exist here, not
//!      one, only because `macro_rules` can't default the `@occurred_at`
//!      field name to `timestamp` when the clause is absent.
//!
//! 2. **Emit phase** (`@emit_trait` arms): once all variants are parsed, each
//!    variant in the accumulator is dispatched to one of three emit arms (one
//!    per kind), which emits that kind's base trait impl
//!    (`ApplyEvent`/`InitEvent`/`DeleteEvent`) and then forwards to an actor
//!    helper with the (possibly empty) actor type: the regular and delete
//!    emitters both forward to `@emit_actor_impl` (with
//!    `[ActorEvent validate_actor]` or `[ActorDeleteEvent
//!    validate_delete_actor]`), and the init emitter forwards to
//!    `@emit_actor_init_event`. That helper expands to nothing when the
//!    variant has no actor, or to the matching
//!    `ActorEvent`/`ActorInitEvent`/`ActorDeleteEvent` impl when it
//!    does — the same validate closure feeding both the base impl's 2-arg (or
//!    1-arg, for init) dispatch and the actor impl's 3-arg (or 2-arg) one.

/// Declaratively define domain events for an aggregate.
///
/// This macro generates event structs, an event enum, and all the required trait
/// implementations (`DomainEvent`, `EventApplicator`, `ApplyEvent`/`InitEvent`,
/// `ActorEvent`/`ActorInitEvent`) from a concise, readable declaration.
///
/// Every crate path the expansion needs beyond `$crate` itself (`pastey`,
/// `uuid`, `serde_json`) is resolved through `$crate::__private`; `chrono`
/// resolves through the public `$crate::chrono` instead, since instants are
/// part of the public API rather than a macro-only detail. Either way a
/// caller depending only on `event-sauce` or `event-sauce-core` needs no
/// direct dependency of its own on any of them. The generated event structs
/// still derive `serde::Serialize` and `serde::Deserialize`, so `serde`
/// with the `derive` feature is the one companion dependency every caller
/// of this macro needs.
///
/// # Features
///
/// - `@init` marks a variant as an initialisation event (type-state pattern)
/// - `@actor(Type)` marks a variant as requiring an actor for permission checks
/// - `@validate |...|` inline validation with arity-based dispatch:
///   - `|agg, evt|` → `validate()` (runs on replay)
///   - `|agg, actor, evt|` → `validate_actor()` (command-time only)
///   - `|evt|` → `validate_init()` (init, runs on replay)
///   - `|actor, evt|` → `validate_init_actor()` (actor-init, command-time only)
/// - `@validate_spec(Expr)` / `@validate_spec(|..| Expr)` specification-based validation
/// - `@post_validate |agg, evt|` post-apply validation
/// - `@post_validate_spec(Expr)` post-apply specification validation
/// - `@version(n)` explicit event version
/// - `@encrypted_fields(f1, f2)` field-level encryption
/// - `@aliases("Old.Name", ...)` accept old wire `event_type` strings on read,
///   keeping historical events loadable after an aggregate/variant rename. The
///   canonical `EVENT_TYPE` (current name) is unchanged; aliases are only
///   additional accepted-on-read names. An unknown, un-aliased `event_type`
///   still fails fast (no silent skip).
/// - `=> |agg, evt|` apply logic (or `=> |id, evt|` for init events)
///
/// An optional **enum-level** clause may appear as the **first** item in the
/// enum body (before any variant):
///
/// - `@upcast |event_type, from_version, data| { ... }` — overrides
///   [`DomainEvent::upcast`](crate::DomainEvent::upcast). Runs on load, before
///   deserialization, with the stored `event_version`; mutate `data` (the
///   variant's flat JSON object) in place to migrate an older payload. Omit the
///   clause to keep the trait default (no migration).
///
/// # Benefits
///
/// - Reduces boilerplate by ~60%
/// - Type-safe validation logic
/// - Consistent event structure
/// - Automatic timestamp handling
/// - Clear, declarative syntax
/// - Full integration with `ApplyEvent` trait
///
/// # `@occurred_at`, and the fact that carries two clocks
///
/// By default each generated event gets a `timestamp` field, and
/// [`DomainEvent::occurred_at`](crate::DomainEvent::occurred_at) answers with it.
///
/// A producer whose facts carry more than one instant cannot use that. "When the
/// world did the thing" and "when this service found out" are different questions,
/// and a field that holds one of them under a name that says neither answers neither
/// — which is what happens when a scheduler writes its own `now()` and a reader
/// writes a producer's stamp into the same column.
///
/// `@occurred_at(field)` names the generated instant instead. The variant declares
/// its other clocks as ordinary fields:
///
/// ```text
/// define_events! {
///     enum SensorEvent for Sensor {
///         Measured {
///             received_at: DateTime<Utc>,   // DETECTION time — an ordinary field
///             celsius: i32,
///         }
///         @occurred_at(measured_at)         // DATA time — names the instant
///         => |sensor, event| { … },
///     }
/// }
/// ```
///
/// `occurred_at()` then answers with `measured_at`, and no field is called
/// `timestamp`. A variant with no marker is unchanged.
///
/// The commands over it take that instant as a parameter, which is
/// [`command_handler!`](crate::command_handler)'s default.
#[macro_export]
macro_rules! define_events {
    // =========================================================================
    // Single entry point — always uses TT muncher.
    // Supports both `@init` and regular variants without needing `@with_init`.
    // =========================================================================
    (
        $vis:vis enum $event_enum:ident for $aggregate:ty {
            @upcast |$upcast_type:ident, $upcast_ver:ident, $upcast_data:ident| $upcast_body:block
            $($rest:tt)*
        }
    ) => {
        $crate::define_events! {
            @munch
            [$vis] [$event_enum] [$aggregate]
            upcast: [[$upcast_type, $upcast_ver, $upcast_data, $upcast_body]]
            accumulated: []
            rest: [$($rest)*]
        }
    };

    (
        $vis:vis enum $event_enum:ident for $aggregate:ty {
            $($rest:tt)*
        }
    ) => {
        $crate::define_events! {
            @munch
            [$vis] [$event_enum] [$aggregate]
            upcast: []
            accumulated: []
            rest: [$($rest)*]
        }
    };

    // =========================================================================
    // TT muncher, phase 1 — classify the marker prefix.
    //
    // Six tiny arms match only the variant's marker tokens and forward to the
    // single `@hooks` arm below with the kind tag and actor type resolved.
    // Markers are matched most-specific-first: `(none)` is a catch-all (it
    // matches any tail), so it must come after every arm that requires a
    // literal `@init`/`@delete`/`@actor(..)` token, or it would shadow them.
    //
    // | Annotations          | Kind tag  | Actor type |
    // |----------------------|-----------|------------|
    // | `@actor(T)`          | `regular` | `T`        |
    // | `@init @actor(T)`    | `init`    | `T`        |
    // | `@delete @actor(T)`  | `delete`  | `T`        |
    // | `@delete`            | `delete`  | (none)     |
    // | `@init`              | `init`    | (none)     |
    // | (none)               | `regular` | (none)     |
    // =========================================================================
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            @actor($actor_type:ty)
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [regular]
            actor_type: [$actor_type]
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            @init
            @actor($actor_type:ty)
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [init]
            actor_type: [$actor_type]
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            @delete
            @actor($actor_type:ty)
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [delete]
            actor_type: [$actor_type]
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            @delete
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [delete]
            actor_type: []
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            @init
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [init]
            actor_type: []
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: [
            $variant:ident {
                $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* $(,)?
            }
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @hooks
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [$($acc)*]
            kind: [regular]
            actor_type: []
            variant: $variant
            fields: { $($(#[$field_attr])* $field: $field_ty),* }
            rest: [$($tail)*]
        }
    };

    // =========================================================================
    // TT muncher, phase 2 — the hook grammar, once, for every kind.
    //
    // Parses the eight optional clauses shared by every kind (`@version`,
    // `@occurred_at`, `@validate`, `@validate_spec`, `@post_validate`,
    // `@post_validate_spec`, `@encrypted_fields`, `@aliases`) plus the
    // mandatory `=> apply` closure, and pushes the normalised metadata blob.
    // Two arms, not one: macro_rules can't default a captured `@occurred_at`
    // field name to `timestamp` within a single arm, since there is no
    // conditional substitution between independently-optional fragments — so
    // the arm that requires `@occurred_at` is tried first, and the arm
    // without it (defaulting the clock field to `timestamp`) second.
    // =========================================================================
    (
        @hooks
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        kind: [$kind:ident]
        actor_type: [$($actor_type:ty)?]
        variant: $variant:ident
        fields: { $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* }
        rest: [
            $(@version($version:literal))?
            @occurred_at($ts_field:ident)
            $(@validate |$($val_args:ident),+| $val_body:block)?
            $(@validate_spec($($val_spec:tt)*))?
            $(@post_validate |$post_val_agg:ident, $post_val_evt:ident| $post_val_body:block)?
            $(@post_validate_spec($($post_val_spec:tt)*))?
            $(@encrypted_fields($($enc_field:ident),+ $(,)?))?
            $(@aliases($($alias:literal),+ $(,)?))?
            => $apply:expr,
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @munch
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [
                $($acc)*
                {
                    variant: $variant,
                    fields: { $($(#[$field_attr])* $field: $field_ty),* },
                    kind: $kind,
                    clock: [$ts_field],
                    actor_type: [$($actor_type)?],
                    version: [$([$version])?],
                    validate: [$([|$($val_args),+| $val_body])?],
                    validate_spec: [$([$($val_spec)*])?],
                    post_validate: [$([$post_val_agg, $post_val_evt, $post_val_body])?],
                    post_validate_spec: [$([$($post_val_spec)*])?],
                    encrypted_fields: [$([$($enc_field),+])?],
                    aliases: [$([$($alias),+])?],
                    apply: $apply,
                }
            ]
            rest: [$($tail)*]
        }
    };
    (
        @hooks
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        kind: [$kind:ident]
        actor_type: [$($actor_type:ty)?]
        variant: $variant:ident
        fields: { $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* }
        rest: [
            $(@version($version:literal))?
            $(@validate |$($val_args:ident),+| $val_body:block)?
            $(@validate_spec($($val_spec:tt)*))?
            $(@post_validate |$post_val_agg:ident, $post_val_evt:ident| $post_val_body:block)?
            $(@post_validate_spec($($post_val_spec:tt)*))?
            $(@encrypted_fields($($enc_field:ident),+ $(,)?))?
            $(@aliases($($alias:literal),+ $(,)?))?
            => $apply:expr,
            $($tail:tt)*
        ]
    ) => {
        $crate::define_events! {
            @munch
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            accumulated: [
                $($acc)*
                {
                    variant: $variant,
                    fields: { $($(#[$field_attr])* $field: $field_ty),* },
                    kind: $kind,
                    clock: [timestamp],
                    actor_type: [$($actor_type)?],
                    version: [$([$version])?],
                    validate: [$([|$($val_args),+| $val_body])?],
                    validate_spec: [$([$($val_spec)*])?],
                    post_validate: [$([$post_val_agg, $post_val_evt, $post_val_body])?],
                    post_validate_spec: [$([$($post_val_spec)*])?],
                    encrypted_fields: [$([$($enc_field),+])?],
                    aliases: [$([$($alias),+])?],
                    apply: $apply,
                }
            ]
            rest: [$($tail)*]
        }
    };

    // =========================================================================
    // TT muncher: done (rest is empty) — forward to @build
    // =========================================================================
    (
        @munch
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        accumulated: [$($acc:tt)*]
        rest: []
    ) => {
        $crate::define_events! {
            @build
            [$vis] [$event_enum] [$aggregate]
            upcast: [$($upcast)*]
            variants: [$($acc)*]
        }
    };

    // =========================================================================
    // @build: generate all code from classified variants
    // =========================================================================
    (
        @build
        [$vis:vis] [$event_enum:ident] [$aggregate:ty]
        upcast: [$($upcast:tt)*]
        variants: [
            $(
                {
                    variant: $variant:ident,
                    fields: { $($(#[$field_attr:meta])* $field:ident: $field_ty:ty),* },
                    kind: $kind:ident,
                    clock: [$clock:ident],
                    actor_type: [$($actor_type:ty)?],
                    version: [$($version:tt)*],
                    validate: [$($validate:tt)*],
                    validate_spec: [$($validate_spec:tt)*],
                    post_validate: [$($post_validate:tt)*],
                    post_validate_spec: [$($post_validate_spec:tt)*],
                    encrypted_fields: [$($encrypted_fields:tt)*],
                    aliases: [$($aliases:tt)*],
                    apply: $apply:expr,
                }
            )*
        ]
    ) => {
        // ---- Per-variant: struct, From, EventType, trait impl ----
        $(
            $crate::__private::paste! {
                #[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
                $vis struct [<$variant Event>] {
                    $($(#[$field_attr])* pub $field: $field_ty,)*
                    pub $clock: $crate::chrono::DateTime<$crate::chrono::Utc>,
                }

                impl ::std::convert::From<[<$variant Event>]> for $event_enum {
                    fn from(event: [<$variant Event>]) -> Self {
                        $event_enum::$variant {
                            $($field: event.$field,)*
                            $clock: event.$clock,
                        }
                    }
                }

                impl $crate::EventType for [<$variant Event>] {
                    const EVENT_TYPE: &'static str = concat!(
                        stringify!($aggregate),
                        ".",
                        stringify!($variant)
                    );
                }
            }

            // Emit ApplyEvent or InitEvent (and optionally ActorEvent/ActorInitEvent) based on kind
            $crate::define_events! {
                @emit_trait
                [$vis] [$aggregate] [$event_enum] [$variant]
                [{ $($(#[$field_attr])* $field: $field_ty,)* }]
                kind: $kind,
                actor_type: [$($actor_type)?],
                validate: [$($validate)*],
                validate_spec: [$($validate_spec)*],
                post_validate: [$($post_validate)*],
                post_validate_spec: [$($post_validate_spec)*],
                apply: $apply,
            }
        )*

        // ---- Shared: event enum ----
        #[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
        $vis enum $event_enum {
            $(
                $variant {
                    $($(#[$field_attr])* $field: $field_ty,)*
                    $clock: $crate::chrono::DateTime<$crate::chrono::Utc>,
                }
            ),*
        }

        // ---- Shared: DomainEvent ----
        // Wrapped in pastey::paste! so [<$variant Event>] is available for
        // to_envelope/from_envelope which serialize/deserialize the flat struct format
        $crate::__private::paste! {
            impl $crate::DomainEvent for $event_enum {
                type Aggregate = $aggregate;

                fn event_type(&self) -> &'static str {
                    match self {
                        $(
                            $event_enum::$variant { .. } => {
                                concat!(stringify!($aggregate), ".", stringify!($variant))
                            }
                        ),*
                    }
                }

                fn event_version(&self) -> $crate::EventVersion {
                    match self {
                        $(
                            $event_enum::$variant { .. } => {
                                $crate::EventVersion::new($crate::define_events!(@version_from [$($version)*]))
                            }
                        ),*
                    }
                }

                fn occurred_at(&self) -> $crate::chrono::DateTime<$crate::chrono::Utc> {
                    match self {
                        $(
                            $event_enum::$variant { $clock, .. } => *$clock
                        ),*
                    }
                }

                fn to_envelope(&self, aggregate_id: $crate::__private::uuid::Uuid) -> $crate::Result<$crate::EventEnvelope> {
                    let event_data = match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::__private::serde_json::to_value(&[<$variant Event>] {
                                    $($field: $field.clone(),)*
                                    $clock: *$clock,
                                })
                            }
                        ),*
                    }.map_err(|e| $crate::Error::custom(format!("Failed to serialize event: {e}")))?;

                    Ok($crate::EventEnvelope::new(
                        $crate::__private::uuid::Uuid::new_v4(),
                        aggregate_id,
                        <Self::Aggregate as $crate::Aggregate>::aggregate_type(),
                        self.event_type().to_string(),
                        self.event_version(),
                        event_data,
                    )
                    .with_created_at(self.occurred_at()))
                }

                fn from_envelope(envelope: &$crate::EventEnvelope) -> $crate::Result<Self> {
                    $(
                        if envelope.event_type == <[<$variant Event>] as $crate::EventType>::EVENT_TYPE
                            || $crate::define_events!(@alias_match (envelope.event_type) [$($aliases)*])
                        {
                            let mut data = envelope.event_data.clone();
                            <$event_enum as $crate::DomainEvent>::upcast(
                                &envelope.event_type,
                                envelope.event_version,
                                &mut data,
                            );
                            let event = $crate::__private::serde_json::from_value::<[<$variant Event>]>(data)
                                .map_err(|e| $crate::Error::custom(format!("Failed to deserialize event: {e}")))?;
                            return Ok($event_enum::$variant {
                                $($field: event.$field,)*
                                $clock: event.$clock,
                            });
                        }
                    )*
                    Err($crate::Error::custom(format!("Unknown event type: {}", envelope.event_type)))
                }

                fn encrypted_fields(&self) -> &'static [&'static str] {
                    match self {
                        $(
                            $event_enum::$variant { .. } => {
                                $crate::define_events!(@encrypted_fields_from [$($encrypted_fields)*])
                            }
                        ),*
                    }
                }

                fn has_any_encrypted_fields() -> bool {
                    $crate::define_events!(@has_any_encrypted_fields $([$($encrypted_fields)*])*)
                }

                $crate::define_events!(@upcast_method [$($upcast)*]);
            }
        }

        // ---- Shared: EventApplicator ----
        // Wrapped in pastey::paste! so [<$variant Event>] is available
        $crate::__private::paste! {
            #[allow(unused_variables, unused_assignments)]
            impl $crate::EventApplicator<$aggregate> for $event_enum {
                fn dispatch(&self, aggregate: &mut $aggregate) -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error> {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [aggregate])
                            }
                        ),*
                    }
                    ::std::result::Result::Ok(())
                }

                fn validate_only(&self, aggregate: &$aggregate) -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error> {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@validate_only_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [aggregate])
                            }
                        ),*
                    }
                    ::std::result::Result::Ok(())
                }

                fn dispatch_unchecked(&self, aggregate: &mut $aggregate) {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_unchecked_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [aggregate])
                            }
                        ),*
                    }
                }

                fn is_init(&self) -> bool {
                    match self {
                        $(
                            $event_enum::$variant { .. } => {
                                $crate::define_events!(@is_init $kind)
                            }
                        ),*
                    }
                }

                fn dispatch_init(&self, id: $crate::EntityId) -> ::std::result::Result<$aggregate, <$aggregate as $crate::Aggregate>::Error> {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_init_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [id])
                            }
                        ),*
                    }
                }

                fn dispatch_init_unchecked(&self, id: $crate::EntityId) -> $aggregate {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_init_unchecked_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [id])
                            }
                        ),*
                    }
                }

                fn is_delete(&self) -> bool {
                    match self {
                        $(
                            $event_enum::$variant { .. } => {
                                $crate::define_events!(@is_delete $kind)
                            }
                        ),*
                    }
                }

                fn dispatch_delete(&self, aggregate: $aggregate) -> ::std::result::Result<<$aggregate as $crate::Aggregate>::DeletedState, <$aggregate as $crate::Aggregate>::Error> {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_delete_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [aggregate])
                            }
                        ),*
                    }
                }

                fn dispatch_delete_unchecked(&self, aggregate: $aggregate) -> <$aggregate as $crate::Aggregate>::DeletedState {
                    match self {
                        $(
                            $event_enum::$variant { $($field,)* $clock } => {
                                $crate::define_events!(@dispatch_delete_unchecked_arm $kind [$aggregate] [[<$variant Event>]] [$($field),*] [$clock] [aggregate])
                            }
                        ),*
                    }
                }
            }
        }
    };

    // =========================================================================
    // Helper: Emit ApplyEvent impl (kind: regular), plus ActorEvent when the
    // variant carries an actor type. An `@actor(T)` variant gets the exact
    // same ApplyEvent impl as a plain one — actor-ness only adds the extra
    // trait below, it never changes replay.
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: regular,
        actor_type: [$($actor_type:ty)?],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            impl $crate::ApplyEvent<$aggregate> for [<$variant Event>] {
                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate(&self, aggregate: &$aggregate)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        $crate::__validate!(|$($val_args),+| $val_body, $aggregate, Self, aggregate, self);
                    )?
                    $(
                        $crate::__check_spec!($($val_spec)*, $aggregate, Self, aggregate, self);
                    )?
                    ::std::result::Result::Ok(())
                }

                fn apply(&self, aggregate: &mut $aggregate) {
                    let apply_fn: fn(&mut $aggregate, &Self) = $apply;
                    apply_fn(aggregate, self);
                }

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn post_validate(&self, aggregate: &$aggregate)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        return (|$post_val_agg: &$aggregate, $post_val_evt: &Self| $post_val_body)(aggregate, self);
                    )?
                    $(
                        $crate::__check_spec!($($post_val_spec)*, $aggregate, Self, aggregate, self);
                    )?
                    ::std::result::Result::Ok(())
                }
            }
        }
        $crate::define_events! {
            @emit_actor_impl [ActorEvent validate_actor] [$($actor_type)?]
            [$aggregate] [$variant]
            validate: [$([|$($val_args),+| $val_body])?],
            validate_spec: [$([$($val_spec)*])?],
        }
    };

    // =========================================================================
    // Helper: Emit InitEvent impl (kind: init), plus ActorInitEvent when the
    // variant carries an actor type.
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: init,
        actor_type: [$($actor_type:ty)?],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            impl $crate::InitEvent<$aggregate> for [<$variant Event>] {
                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate_init(&self) -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error> {
                    $(
                        $crate::__validate_init!(|$($val_args),+| $val_body, Self, self);
                    )?
                    $(
                        $crate::__check_spec_init!($($val_spec)*, Self, self);
                    )?
                    ::std::result::Result::Ok(())
                }

                fn init(&self, id: $crate::EntityId) -> $aggregate {
                    let init_fn: fn($crate::EntityId, &Self) -> $aggregate = $apply;
                    init_fn(id, self)
                }

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn post_validate_init(&self, aggregate: &$aggregate) -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error> {
                    $(
                        return (|$post_val_agg: &$aggregate, $post_val_evt: &Self| $post_val_body)(aggregate, self);
                    )?
                    $(
                        $crate::__check_spec!($($post_val_spec)*, $aggregate, Self, aggregate, self);
                    )?
                    ::std::result::Result::Ok(())
                }
            }
        }
        $crate::define_events! {
            @emit_actor_init_event [$($actor_type)?]
            [$aggregate] [$variant]
            validate: [$([|$($val_args),+| $val_body])?],
            validate_spec: [$([$($val_spec)*])?],
        }
    };

    // =========================================================================
    // Helper: Emit DeleteEvent impl (kind: delete), plus ActorDeleteEvent when
    // the variant carries an actor type.
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: delete,
        actor_type: [$($actor_type:ty)?],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            impl $crate::DeleteEvent<$aggregate> for [<$variant Event>] {
                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate_delete(&self, aggregate: &$aggregate)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        $crate::__validate!(|$($val_args),+| $val_body, $aggregate, Self, aggregate, self);
                    )?
                    $(
                        $crate::__check_spec!($($val_spec)*, $aggregate, Self, aggregate, self);
                    )?
                    ::std::result::Result::Ok(())
                }

                fn delete(&self, aggregate: $aggregate) -> <$aggregate as $crate::Aggregate>::DeletedState {
                    let delete_fn: fn($aggregate, &Self) -> <$aggregate as $crate::Aggregate>::DeletedState = $apply;
                    delete_fn(aggregate, self)
                }

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn post_validate_delete(&self, state: &<$aggregate as $crate::Aggregate>::DeletedState)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        return (|$post_val_agg: &<$aggregate as $crate::Aggregate>::DeletedState, $post_val_evt: &Self| $post_val_body)(state, self);
                    )?
                    $(
                        // Post-validate specs not supported for delete events (DeletedState != Aggregate)
                        let _ = ($($post_val_spec)*,);
                    )?
                    ::std::result::Result::Ok(())
                }
            }
        }
        $crate::define_events! {
            @emit_actor_impl [ActorDeleteEvent validate_delete_actor] [$($actor_type)?]
            [$aggregate] [$variant]
            validate: [$([|$($val_args),+| $val_body])?],
            validate_spec: [$([$($val_spec)*])?],
        }
    };

    // =========================================================================
    // Helper: the extra trait an `@actor(T)` variant adds on top of its base
    // kind's impl above — nothing when the variant has no actor. Shared by
    // the regular and delete emitters (`ActorEvent`/`validate_actor` and
    // `ActorDeleteEvent`/`validate_delete_actor` respectively); the init
    // emitter below has its own arm since it takes no aggregate parameter.
    // =========================================================================
    (@emit_actor_impl [$trait:ident $method:ident] [] [$aggregate:ty] [$variant:ident] validate: [$($_v:tt)*], validate_spec: [$($_vs:tt)*],) => {};
    (
        @emit_actor_impl [$trait:ident $method:ident] [$actor_type:ty] [$aggregate:ty] [$variant:ident]
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
    ) => {
        $crate::__private::paste! {
            impl $crate::$trait<$aggregate> for [<$variant Event>] {
                type Actor = $actor_type;

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn $method(&self, aggregate: &$aggregate, actor: &$actor_type)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        $crate::__validate_actor!(|$($val_args),+| $val_body, $aggregate, $actor_type, Self, aggregate, actor, self);
                    )?
                    $(
                        $crate::__check_spec_actor!($($val_spec)*, $aggregate, $actor_type, Self, aggregate, actor, self);
                    )?
                    ::std::result::Result::Ok(())
                }
            }
        }
    };

    (@emit_actor_init_event [] [$aggregate:ty] [$variant:ident] validate: [$($_v:tt)*], validate_spec: [$($_vs:tt)*],) => {};
    (
        @emit_actor_init_event [$actor_type:ty] [$aggregate:ty] [$variant:ident]
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
    ) => {
        $crate::__private::paste! {
            impl $crate::ActorInitEvent<$aggregate> for [<$variant Event>] {
                type Actor = $actor_type;

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate_init_actor(&self, actor: &$actor_type)
                    -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                {
                    $(
                        $crate::__validate_init_actor!(|$($val_args),+| $val_body, $actor_type, Self, actor, self);
                    )?
                    $(
                        $crate::__check_spec_init_actor!($($val_spec)*, $actor_type, Self, actor, self);
                    )?
                    ::std::result::Result::Ok(())
                }
            }
        }
    };


    (@rebuild $evt_type:ident [$($field:ident),*] [$timestamp:ident]) => {
        $evt_type {
            $($field: $field.clone(),)*
            $timestamp: *$timestamp,
        }
    };

    // =========================================================================
    // Helpers: per-variant EventApplicator arms (validate_only, dispatch,
    // init, delete) — called from within pastey::paste! so $evt_type is
    // already resolved (e.g. CreatedEvent). No inner pastey needed.
    //
    // Every dispatch family below matches its one real kind, then falls back
    // to a single catch-all that covers both other kinds: actor-ness never
    // changes which family handles a variant, only the base kind does.
    // =========================================================================
    (@validate_only_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.validate($aggregate_var)?;
        }
    };
    (@validate_only_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        // An init fact has no aggregate to validate against yet, and a delete
        // fact never reaches `dispatch`; each runs its own guard instead.
        {}
    };

    (@dispatch_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.validate($aggregate_var)?;
            evt.apply($aggregate_var);
            evt.post_validate($aggregate_var)?;
        }
    };
    (@dispatch_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch() called on a {} event; use dispatch_init()/dispatch_delete()", stringify!($other))
    };

    (@dispatch_unchecked_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.apply($aggregate_var);
        }
    };
    (@dispatch_unchecked_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_unchecked() called on a {} event; use dispatch_init_unchecked()/dispatch_delete_unchecked()", stringify!($other))
    };

    (@dispatch_init_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.validate_init()?;
            let entity = evt.init($id_var);
            evt.post_validate_init(&entity)?;
            ::std::result::Result::Ok(entity)
        }
    };
    (@dispatch_init_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init() called on a {} event", stringify!($other))
    };

    (@dispatch_init_unchecked_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.init($id_var)
        }
    };
    (@dispatch_init_unchecked_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init_unchecked() called on a {} event", stringify!($other))
    };

    (@is_init init) => { true };
    (@is_init $other:ident) => { false };

    (@is_delete delete) => { true };
    (@is_delete $other:ident) => { false };

    (@dispatch_delete_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.validate_delete(&$aggregate_var)?;
            let state = evt.delete($aggregate_var);
            evt.post_validate_delete(&state)?;
            ::std::result::Result::Ok(state)
        }
    };
    (@dispatch_delete_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete() called on a {} event", stringify!($other))
    };

    (@dispatch_delete_unchecked_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $crate::define_events!(@rebuild $evt_type [$($field),*] [$timestamp]);
            evt.delete($aggregate_var)
        }
    };
    (@dispatch_delete_unchecked_arm $other:ident [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete_unchecked() called on a {} event", stringify!($other))
    };

    // =========================================================================
    // Helper: extract version (default to 1)
    // =========================================================================
    (@version_from [[$version:literal]]) => { $version };
    (@version_from []) => { 1 };


    // =========================================================================
    // Helper: extract encrypted field names for a variant (default to empty)
    // =========================================================================
    (@encrypted_fields_from [[$($enc_field:ident),+]]) => {
        &[$(stringify!($enc_field)),+]
    };
    (@encrypted_fields_from []) => {
        &[]
    };

    // =========================================================================
    // Helper: build the alias-matching condition for a variant in from_envelope.
    // =========================================================================
    // The accumulator stores a variant's aliases as `[["Old.Name", ...]]` when
    // present, or `[]` when absent. This helper expands those into a boolean
    // expression matching the runtime `event_type` against any declared alias.
    // With no aliases it evaluates to `false` so only the canonical EVENT_TYPE
    // matches (preserving the fail-fast default for genuinely-unknown types).
    (@alias_match ($event_type:expr) [[$($alias:literal),+]]) => {
        $($event_type == $alias)||+
    };
    (@alias_match ($event_type:expr) []) => {
        false
    };

    // =========================================================================
    // Helper: check if any variant has encrypted fields
    // =========================================================================
    // Processes each variant's encrypted_fields one at a time.
    // Each item is either `[[field1, field2, ...]]` (encrypted) or `[]` (plain).
    (@has_any_encrypted_fields [[$($enc_field:ident),+]] $($rest:tt)*) => {
        true
    };
    (@has_any_encrypted_fields [] $($rest:tt)*) => {
        $crate::define_events!(@has_any_encrypted_fields $($rest)*)
    };
    (@has_any_encrypted_fields) => {
        false
    };

    // =========================================================================
    // Helper: emit the optional `DomainEvent::upcast` override.
    // =========================================================================
    // When an `@upcast |event_type, from_version, data| { ... }` clause was
    // supplied, generate an override running the user's closure body. When
    // absent, generate nothing so the trait's default (no-op) applies.
    (@upcast_method [[$ut:ident, $uv:ident, $ud:ident, $ubody:block]]) => {
        fn upcast(
            $ut: &str,
            $uv: $crate::EventVersion,
            $ud: &mut $crate::__private::serde_json::Value,
        ) $ubody
    };
    (@upcast_method []) => {};
}

#[cfg(test)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
#[allow(clippy::struct_field_names)]
#[allow(clippy::items_after_statements)]
#[allow(clippy::derivable_impls)]
#[allow(clippy::match_same_arms)]
#[allow(clippy::unnecessary_wraps)]
#[allow(clippy::default_trait_access)]
mod tests;
