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
    // Helper: dispatch arm — called from within pastey::paste! so $evt_type
    // is already resolved (e.g. CreatedEvent). No inner pastey needed.
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
mod tests {
    use crate::{
        command_handler, spec, Aggregate, AggregateError, AggregateRoot, AggregateVersion,
        ApplyEvent, DomainEvent, Entity, EntityId, SpecificationError,
    };
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    // ===== define_events! Macro Tests =====

    // First, define a test aggregate for the macro
    #[derive(Debug, thiserror::Error)]
    pub enum OrderError {
        #[error("Order already completed")]
        OrderAlreadyCompleted,
        #[error("Invalid quantity: {0}")]
        InvalidQuantity(u32),
        #[error("Invalid amount: {0}")]
        InvalidAmount(i64),
        #[error("Order total exceeded: {0}")]
        OrderTotalExceeded(i64),
    }

    impl AggregateError for OrderError {}

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum OrderStatus {
        Created,
        Completed,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct OrderItem {
        item_id: String,
        quantity: u32,
        price: i64,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct OrderState {
        order_id: String,
        items: Vec<OrderItem>,
        total_amount: i64,
        status: OrderStatus,
    }

    impl Default for OrderStatus {
        fn default() -> Self {
            OrderStatus::Created
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Order {
        id: EntityId,
        state: OrderState,
    }

    impl Entity for Order {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                state: OrderState::default(),
            }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for Order {}

    impl Aggregate for Order {
        type Event = OrderEvent;
        type Error = OrderError;
        type DeletedState = Self;
    }

    impl Order {
        /// Apply an event through dispatch.
        fn apply<E: Into<OrderEvent>>(&mut self, event: E) -> Result<(), OrderError> {
            let event = event.into();
            crate::EventApplicator::dispatch(&event, self)?;
            Ok(())
        }
    }

    // This will be generated by the macro
    define_events! {
        pub enum OrderEvent for Order {
            Created {
                order_id: String,
            } => |order, event| {
                order.state.order_id = event.order_id.clone();
            },

            ItemAdded {
                item_id: String,
                quantity: u32,
                price: i64,
            }
            @validate |aggregate, event| {
                if aggregate.state.status == OrderStatus::Completed {
                    return Err(OrderError::OrderAlreadyCompleted);
                }
                if event.quantity == 0 {
                    return Err(OrderError::InvalidQuantity(event.quantity));
                }
                return Ok(());
            }
            @post_validate |aggregate, event| {
                if aggregate.state.total_amount > 1_000_000 {
                    return Err(OrderError::OrderTotalExceeded(aggregate.state.total_amount));
                }
                return Ok(());
            }
            => |order, event| {
                order.state.items.push(OrderItem {
                    item_id: event.item_id.clone(),
                    quantity: event.quantity,
                    price: event.price,
                });
                order.state.total_amount += event.price * i64::from(event.quantity);
            },

            Completed {}
            @version(2)
            @validate |aggregate, _event| {
                if aggregate.state.status == OrderStatus::Completed {
                    return Err(OrderError::OrderAlreadyCompleted);
                }
                return Ok(());
            }
            => |order, _event| {
                order.state.status = OrderStatus::Completed;
            },
        }
    }

    // Tests for basic event generation
    #[test]
    fn test_define_events_creates_event_structs() {
        // Test that individual event structs are created
        let _event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };
    }

    #[test]
    fn test_define_events_creates_event_enum() {
        // Test that the event enum is created
        let event = OrderEvent::Created {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        assert!(matches!(event, OrderEvent::Created { .. }));
    }

    #[test]
    fn test_define_events_implements_domain_event() {
        // Test that DomainEvent is implemented
        let event = OrderEvent::Created {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        assert_eq!(event.event_type(), "Order.Created");
        assert_eq!(event.event_version(), crate::EventVersion::new(1)); // default version
    }

    #[test]
    fn test_define_events_custom_version() {
        // Test that custom version is respected
        let event = OrderEvent::Completed {
            timestamp: Utc::now(),
        };

        assert_eq!(event.event_version(), crate::EventVersion::new(2)); // explicit @version(2)
    }

    #[test]
    fn test_define_events_apply_event_simple() {
        // Test simple event without validation
        let mut order = Order::new(EntityId::new());
        let event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        event.apply(&mut order);

        assert_eq!(order.state.order_id, "order-123");
    }

    #[test]
    fn test_define_events_validation_success() {
        // Test that validation works
        let order = Order::new(EntityId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 5,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_define_events_validation_failure() {
        // Test that validation catches errors
        let mut order = Order::new(EntityId::new());
        order.state.status = OrderStatus::Completed;

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 5,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OrderError::OrderAlreadyCompleted
        ));
    }

    #[test]
    fn test_define_events_validation_zero_quantity() {
        // Test specific validation logic
        let order = Order::new(EntityId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 0,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.validate(&order);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OrderError::InvalidQuantity(0)
        ));
    }

    #[test]
    fn test_define_events_post_validation_success() {
        // Test post-validation success case
        let mut order = Order::new(EntityId::new());
        order.state.total_amount = 500_000;

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 1,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.post_validate(&order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_define_events_post_validation_failure() {
        // Test post-validation failure
        let mut order = Order::new(EntityId::new());
        order.state.total_amount = 1_500_000; // Over the limit

        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 1,
            price: 100,
            timestamp: Utc::now(),
        };

        let result = event.post_validate(&order);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OrderError::OrderTotalExceeded(1_500_000)
        ));
    }

    #[test]
    fn test_define_events_apply_with_state_mutation() {
        // Test that apply actually mutates state
        let mut order = Order::new(EntityId::new());
        let event = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 2,
            price: 500,
            timestamp: Utc::now(),
        };

        event.apply(&mut order);

        assert_eq!(order.state.items.len(), 1);
        assert_eq!(order.state.items[0].item_id, "item-1");
        assert_eq!(order.state.items[0].quantity, 2);
        assert_eq!(order.state.items[0].price, 500);
        assert_eq!(order.state.total_amount, 1000); // 2 * 500
    }

    #[test]
    fn test_define_events_into_conversion() {
        // Test that From<EventStruct> for EventEnum is implemented
        let event_struct = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        let event_enum: OrderEvent = event_struct.into();

        assert!(matches!(event_enum, OrderEvent::Created { .. }));
    }

    #[test]
    fn test_define_events_timestamp_field() {
        // Test that timestamp is automatically added
        let timestamp = Utc::now();
        let event = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp,
        };

        assert_eq!(event.timestamp, timestamp);
    }

    #[test]
    fn test_define_events_full_flow() {
        // Test complete flow: create, validate, apply
        let mut order = Order::new(EntityId::new());

        // Create order
        let created = CreatedEvent {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };
        created.validate(&order).unwrap();
        created.apply(&mut order);
        created.post_validate(&order).unwrap();

        assert_eq!(order.state.order_id, "order-123");

        // Add items
        let item = ItemAddedEvent {
            item_id: "item-1".to_string(),
            quantity: 3,
            price: 200,
            timestamp: Utc::now(),
        };
        item.validate(&order).unwrap();
        item.apply(&mut order);
        item.post_validate(&order).unwrap();

        assert_eq!(order.state.items.len(), 1);
        assert_eq!(order.state.total_amount, 600);

        // Complete order
        let completed = CompletedEvent {
            timestamp: Utc::now(),
        };
        completed.validate(&order).unwrap();
        completed.apply(&mut order);
        completed.post_validate(&order).unwrap();

        assert_eq!(order.state.status, OrderStatus::Completed);
    }

    #[test]
    fn test_define_events_validation_prevents_double_completion() {
        // Test that validation prevents invalid state transitions
        let mut order = Order::new(EntityId::new());
        order.state.status = OrderStatus::Completed;

        let completed = CompletedEvent {
            timestamp: Utc::now(),
        };

        let result = completed.validate(&order);
        assert!(result.is_err());
    }

    #[test]
    fn test_define_events_to_envelope_serializes_flat_struct() {
        // to_envelope() should serialize event data as flat struct (no enum variant wrapper)
        let event = OrderEvent::Created {
            order_id: "order-123".to_string(),
            timestamp: Utc::now(),
        };

        let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();

        // The event_data should be flat: {"order_id":"order-123","timestamp":"..."}
        // NOT enum-wrapped: {"Created":{"order_id":"order-123","timestamp":"..."}}
        let data = &envelope.event_data;
        assert!(
            data.get("order_id").is_some(),
            "event_data should have flat 'order_id' field"
        );
        assert!(
            data.get("timestamp").is_some(),
            "event_data should have flat 'timestamp' field"
        );
        assert!(
            data.get("Created").is_none(),
            "event_data should NOT have enum variant wrapper"
        );
    }

    #[test]
    fn test_define_events_envelope_roundtrip() {
        // to_envelope() → from_envelope() should produce equivalent events
        let timestamp = Utc::now();
        let aggregate_id = uuid::Uuid::new_v4();

        let original = OrderEvent::ItemAdded {
            item_id: "item-42".to_string(),
            quantity: 3,
            price: 500,
            timestamp,
        };

        let envelope = original.to_envelope(aggregate_id).unwrap();
        let deserialized = OrderEvent::from_envelope(&envelope).unwrap();

        assert_eq!(deserialized.event_type(), "Order.ItemAdded");
        if let OrderEvent::ItemAdded {
            item_id,
            quantity,
            price,
            timestamp: ts,
        } = deserialized
        {
            assert_eq!(item_id, "item-42");
            assert_eq!(quantity, 3);
            assert_eq!(price, 500);
            assert_eq!(ts, timestamp);
        } else {
            panic!("Expected ItemAdded variant");
        }
    }

    #[test]
    fn test_define_events_envelope_roundtrip_no_fields() {
        // Round-trip for event with no fields (only timestamp)
        let timestamp = Utc::now();
        let original = OrderEvent::Completed { timestamp };

        let envelope = original.to_envelope(uuid::Uuid::new_v4()).unwrap();
        let deserialized = OrderEvent::from_envelope(&envelope).unwrap();

        assert_eq!(deserialized.event_type(), "Order.Completed");
        if let OrderEvent::Completed { timestamp: ts } = deserialized {
            assert_eq!(ts, timestamp);
        } else {
            panic!("Expected Completed variant");
        }
    }

    #[test]
    fn test_define_events_from_envelope_unknown_event_type() {
        // from_envelope() should return error for unknown event types
        let envelope = crate::EventEnvelope::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "Order".to_string(),
            "Order.Unknown".to_string(),
            crate::EventVersion::from(1),
            serde_json::json!({}),
        );

        let result = OrderEvent::from_envelope(&envelope);
        assert!(result.is_err());
    }

    // ===== @validate_spec / @post_validate_spec Tests =====

    // Error type with SpecificationFailed variant for spec-based validation
    #[derive(Debug, thiserror::Error)]
    enum WarehouseError {
        #[error("Not enough stock")]
        NotEnoughStock,
        #[error("{0}")]
        SpecificationFailed(#[from] SpecificationError<Warehouse>),
    }

    impl AggregateError for WarehouseError {}

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct WarehouseState {
        stock: i64,
        name: String,
        active: bool,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Warehouse {
        id: EntityId,
        state: WarehouseState,
    }

    impl Entity for Warehouse {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                state: WarehouseState::default(),
            }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for Warehouse {}

    impl Aggregate for Warehouse {
        type Event = WarehouseEvent;
        type Error = WarehouseError;
        type DeletedState = Self;
    }

    impl Warehouse {
        /// Apply an event through dispatch.
        fn apply<E: Into<WarehouseEvent>>(&mut self, event: E) -> Result<(), WarehouseError> {
            let event = event.into();
            crate::EventApplicator::dispatch(&event, self)?;
            Ok(())
        }
    }

    // Specifications for the warehouse
    spec!(WarehouseIsActive for Warehouse, "Warehouse must be active", |w| {
        w.state.active
    });

    spec!(StockIsPositive for Warehouse, "Stock must be positive", stock = w.state.stock, |w| {
        w.state.stock > 0
    });

    define_events! {
        enum WarehouseEvent for Warehouse {
            Activated {
                name: String,
            } => |warehouse, event| {
                warehouse.state.name = event.name.clone();
                warehouse.state.active = true;
            },

            StockAdded {
                quantity: i64,
            }
            @validate_spec(WarehouseIsActive)
            => |warehouse, event| {
                warehouse.state.stock += event.quantity;
            },

            StockRemoved {
                quantity: i64,
            }
            @validate_spec(WarehouseIsActive)
            @post_validate_spec(StockIsPositive)
            => |warehouse, event| {
                warehouse.state.stock -= event.quantity;
            },
        }
    }

    #[test]
    fn test_validate_spec_allows_valid_operation() {
        let mut warehouse = Warehouse::new(EntityId::new());
        // Activate the warehouse
        warehouse
            .apply(ActivatedEvent {
                name: "Main Warehouse".to_string(),
                timestamp: Utc::now(),
            })
            .unwrap();

        // Adding stock should succeed (warehouse is active)
        let result = warehouse.apply(StockAddedEvent {
            quantity: 100,
            timestamp: Utc::now(),
        });
        assert!(result.is_ok());
        assert_eq!(warehouse.state.stock, 100);
    }

    #[test]
    fn test_validate_spec_rejects_when_spec_fails() {
        let mut warehouse = Warehouse::new(EntityId::new());
        // Don't activate warehouse - it's inactive by default

        // Adding stock should fail (warehouse is not active)
        let result = warehouse.apply(StockAddedEvent {
            quantity: 100,
            timestamp: Utc::now(),
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, WarehouseError::SpecificationFailed(_)),
            "Expected SpecificationFailed, got: {err}"
        );
        assert_eq!(err.to_string(), "Warehouse must be active");
    }

    #[test]
    fn test_post_validate_spec_allows_valid_state() {
        let mut warehouse = Warehouse::new(EntityId::new());
        warehouse
            .apply(ActivatedEvent {
                name: "Main".to_string(),
                timestamp: Utc::now(),
            })
            .unwrap();
        warehouse
            .apply(StockAddedEvent {
                quantity: 100,
                timestamp: Utc::now(),
            })
            .unwrap();

        // Remove some stock, leaving positive stock -> should succeed
        let result = warehouse.apply(StockRemovedEvent {
            quantity: 50,
            timestamp: Utc::now(),
        });
        assert!(result.is_ok());
        assert_eq!(warehouse.state.stock, 50);
    }

    #[test]
    fn test_post_validate_spec_rejects_invalid_state() {
        let mut warehouse = Warehouse::new(EntityId::new());
        warehouse
            .apply(ActivatedEvent {
                name: "Main".to_string(),
                timestamp: Utc::now(),
            })
            .unwrap();
        warehouse
            .apply(StockAddedEvent {
                quantity: 10,
                timestamp: Utc::now(),
            })
            .unwrap();

        // Remove all stock, leaving 0 -> should fail post_validate_spec
        let result = warehouse.apply(StockRemovedEvent {
            quantity: 10,
            timestamp: Utc::now(),
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, WarehouseError::SpecificationFailed(_)),
            "Expected SpecificationFailed, got: {err}"
        );
        assert!(err.to_string().contains("Stock must be positive"));
    }

    #[test]
    fn test_validate_spec_with_composed_spec() {
        // Verify that composed specs work with @validate_spec
        // The WarehouseIsActive spec is a simple spec, but this test confirms
        // the pattern works end-to-end with the define_events! macro
        let mut warehouse = Warehouse::new(EntityId::new());

        // Inactive warehouse -> stock removal should fail at validate_spec
        let result = warehouse.apply(StockRemovedEvent {
            quantity: 5,
            timestamp: Utc::now(),
        });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Warehouse must be active");
    }

    #[test]
    fn test_validate_spec_and_post_validate_spec_both_run() {
        let mut warehouse = Warehouse::new(EntityId::new());
        warehouse
            .apply(ActivatedEvent {
                name: "Main".to_string(),
                timestamp: Utc::now(),
            })
            .unwrap();
        warehouse
            .apply(StockAddedEvent {
                quantity: 100,
                timestamp: Utc::now(),
            })
            .unwrap();

        // Remove stock successfully (both validate and post_validate pass)
        warehouse
            .apply(StockRemovedEvent {
                quantity: 30,
                timestamp: Utc::now(),
            })
            .unwrap();
        assert_eq!(warehouse.state.stock, 70);

        // Remove more stock (still positive)
        warehouse
            .apply(StockRemovedEvent {
                quantity: 69,
                timestamp: Utc::now(),
            })
            .unwrap();
        assert_eq!(warehouse.state.stock, 1);

        // Try to remove last unit -> post_validate fails (stock would be 0)
        let result = warehouse.apply(StockRemovedEvent {
            quantity: 1,
            timestamp: Utc::now(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_spec_error_is_specification_failed() {
        let warehouse = Warehouse::new(EntityId::new());
        // Directly test the validate method on the event
        let event = StockAddedEvent {
            quantity: 10,
            timestamp: Utc::now(),
        };
        let result = event.validate(&warehouse);
        assert!(result.is_err());
        match result.unwrap_err() {
            WarehouseError::SpecificationFailed(spec_err) => {
                assert_eq!(spec_err.message, "Warehouse must be active");
            }
            other @ WarehouseError::NotEnoughStock => {
                panic!("Expected SpecificationFailed, got: {other}")
            }
        }
    }

    // ===== @init define_events! macro tests =====

    #[derive(Debug, thiserror::Error)]
    pub enum AccountError {
        #[error("Empty name")]
        EmptyName,
        #[error("Negative balance")]
        NegativeBalance,
    }

    impl AggregateError for AccountError {}

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Account {
        id: EntityId,
        name: String,
        balance: i64,
    }

    impl Entity for Account {
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl Aggregate for Account {
        type Event = AccountEvent;
        type Error = AccountError;
        type DeletedState = Self;
    }

    define_events! {
        pub enum AccountEvent for Account {
            AccountOpened {
                name: String,
                initial_balance: i64,
            }
            @init
            @validate |event| {
                if event.name.is_empty() {
                    return Err(AccountError::EmptyName);
                }
                return Ok(());
            }
            => |id, event| {
                Account {
                    id,
                    name: event.name.clone(),
                    balance: event.initial_balance,
                }
            },

            Deposited {
                amount: i64,
            }
            => |account, event| {
                account.balance += event.amount;
            },

            Withdrawn {
                amount: i64,
            }
            @post_validate |account, _event| {
                if account.balance < 0 {
                    return Err(AccountError::NegativeBalance);
                }
                return Ok(());
            }
            => |account, event| {
                account.balance -= event.amount;
            },
        }
    }

    #[test]
    fn test_init_event_macro_creates_init_event_struct() {
        let _event = AccountOpenedEvent {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
    }

    #[test]
    fn test_init_event_macro_init_trait_impl() {
        use crate::InitEvent;
        let event = AccountOpenedEvent {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        let id = EntityId::new();
        let account = event.init(id);
        assert_eq!(account.entity_id(), id);
        assert_eq!(account.name, "Alice");
        assert_eq!(account.balance, 100);
    }

    #[test]
    fn test_init_event_macro_validate_init() {
        use crate::InitEvent;
        let event = AccountOpenedEvent {
            name: String::new(),
            initial_balance: 0,
            timestamp: Utc::now(),
        };
        let result = event.validate_init();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty name");
    }

    #[test]
    fn test_init_event_macro_from_conversion() {
        let event = AccountOpenedEvent {
            name: "Bob".to_string(),
            initial_balance: 50,
            timestamp: Utc::now(),
        };
        let enum_event: AccountEvent = event.into();
        assert!(matches!(enum_event, AccountEvent::AccountOpened { .. }));
    }

    #[test]
    fn test_init_event_macro_event_applicator_is_init() {
        use crate::EventApplicator;
        let init_event = AccountEvent::AccountOpened {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        assert!(EventApplicator::is_init(&init_event));

        let regular_event = AccountEvent::Deposited {
            amount: 50,
            timestamp: Utc::now(),
        };
        assert!(!EventApplicator::is_init(&regular_event));
    }

    #[test]
    fn test_init_event_macro_dispatch_init() {
        use crate::EventApplicator;
        let event = AccountEvent::AccountOpened {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        let id = EntityId::new();
        let account = EventApplicator::dispatch_init(&event, id).unwrap();
        assert_eq!(account.entity_id(), id);
        assert_eq!(account.name, "Alice");
        assert_eq!(account.balance, 100);
    }

    #[test]
    fn test_init_event_macro_dispatch_init_unchecked() {
        use crate::EventApplicator;
        let event = AccountEvent::AccountOpened {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        let id = EntityId::new();
        let account = EventApplicator::dispatch_init_unchecked(&event, id);
        assert_eq!(account.entity_id(), id);
        assert_eq!(account.balance, 100);
    }

    #[test]
    fn test_init_event_macro_regular_dispatch() {
        use crate::ApplyEvent;
        let mut account = Account {
            id: EntityId::new(),
            name: "Alice".to_string(),
            balance: 100,
        };
        let event = DepositedEvent {
            amount: 50,
            timestamp: Utc::now(),
        };
        event.apply(&mut account);
        assert_eq!(account.balance, 150);
    }

    #[test]
    fn test_init_event_macro_regular_post_validate() {
        use crate::ApplyEvent;
        let account = Account {
            id: EntityId::new(),
            name: "Alice".to_string(),
            balance: 10,
        };
        let event = WithdrawnEvent {
            amount: 20,
            timestamp: Utc::now(),
        };
        // Balance would go to -10
        let mut account_clone = Account {
            id: account.id,
            name: account.name.clone(),
            balance: account.balance,
        };
        event.apply(&mut account_clone);
        let result = event.post_validate(&account_clone);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Negative balance");
    }

    #[test]
    fn test_init_event_macro_uninit_aggregate_full_lifecycle() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let id = uninit.entity_id();

        let event = AccountOpenedEvent {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        let mut agg = uninit.apply_init(event).unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.name, "Alice");
        assert_eq!(agg.balance, 100);
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);

        // Apply regular events
        agg.apply(DepositedEvent {
            amount: 50,
            timestamp: Utc::now(),
        })
        .unwrap();
        assert_eq!(agg.balance, 150);
        assert_eq!(agg.version(), AggregateVersion::new(2));

        agg.apply(WithdrawnEvent {
            amount: 30,
            timestamp: Utc::now(),
        })
        .unwrap();
        assert_eq!(agg.balance, 120);
        assert_eq!(agg.version(), AggregateVersion::new(3));
        assert_eq!(agg.pending_events().len(), 3);
    }

    #[test]
    fn test_init_event_macro_uninit_validation_fails() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let event = AccountOpenedEvent {
            name: String::new(),
            initial_balance: 0,
            timestamp: Utc::now(),
        };
        let result = uninit.apply_init(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty name");
    }

    #[test]
    fn test_init_event_macro_domain_event() {
        use crate::DomainEvent;
        let event = AccountEvent::AccountOpened {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp: Utc::now(),
        };
        assert_eq!(event.event_type(), "Account.AccountOpened");
        assert_eq!(event.event_version(), crate::EventVersion::new(1));

        let deposit = AccountEvent::Deposited {
            amount: 50,
            timestamp: Utc::now(),
        };
        assert_eq!(deposit.event_type(), "Account.Deposited");
    }

    // ===== @init command_handler! macro tests =====

    command_handler! {
        impl Account {
            @clock @init fn open_account(name: String, initial_balance: i64)
                -> AccountOpenedEvent { name, initial_balance };
            @clock fn deposit(amount: i64)
                -> DepositedEvent { amount };
            @clock fn withdraw(amount: i64)
                -> WithdrawnEvent { amount };
        }
    }

    #[test]
    fn test_init_command_handler_event_helper_is_associated_fn() {
        // Init event helper is an associated function (no &self)
        let event = Account::open_account_event("Alice".to_string(), 100);
        assert_eq!(event.name, "Alice");
        assert_eq!(event.initial_balance, 100);
    }

    #[test]
    fn test_init_command_handler_init_command_on_uninit() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let id = uninit.entity_id();
        let agg = uninit.open_account("Alice".to_string(), 100).unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.name, "Alice");
        assert_eq!(agg.balance, 100);
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);
    }

    #[test]
    fn test_init_command_handler_init_validation_fails() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let result = uninit.open_account(String::new(), 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty name");
    }

    #[test]
    fn test_init_command_handler_regular_event_helper() {
        let account = Account {
            id: EntityId::new(),
            name: "Alice".to_string(),
            balance: 100,
        };
        // Regular event helper is an instance method
        let event = account.deposit_event(50);
        assert_eq!(event.amount, 50);
    }

    #[test]
    fn test_init_command_handler_regular_command_on_aggregate_root() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let mut agg = uninit.open_account("Alice".to_string(), 100).unwrap();

        agg.deposit(50).unwrap();
        assert_eq!(agg.balance, 150);
        assert_eq!(agg.version(), AggregateVersion::new(2));
    }

    #[test]
    fn test_init_command_handler_full_lifecycle() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let mut agg = uninit.open_account("Alice".to_string(), 100).unwrap();

        agg.deposit(50).unwrap();
        agg.withdraw(30).unwrap();

        assert_eq!(agg.balance, 120);
        assert_eq!(agg.version(), AggregateVersion::new(3));
        assert_eq!(agg.pending_events().len(), 3);
    }

    #[test]
    fn test_init_command_handler_regular_post_validation() {
        let uninit = crate::UninitAggregateRoot::<Account>::new(EntityId::new());
        let mut agg = uninit.open_account("Alice".to_string(), 10).unwrap();

        // Withdraw more than balance should fail post-validation
        let result = agg.withdraw(20);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Negative balance");
        // Note: post_validate fires after apply, so the entity state was mutated
        // but the event was NOT appended to pending_events (dispatch returned Err)
        assert_eq!(agg.pending_events().len(), 1); // Only the init event
    }

    #[test]
    fn test_init_events_envelope_roundtrip_init_variant() {
        use crate::DomainEvent;
        let timestamp = Utc::now();
        let original = AccountEvent::AccountOpened {
            name: "Alice".to_string(),
            initial_balance: 100,
            timestamp,
        };

        let envelope = original.to_envelope(uuid::Uuid::new_v4()).unwrap();

        // Should serialize as flat struct
        assert!(envelope.event_data.get("name").is_some());
        assert!(envelope.event_data.get("AccountOpened").is_none());

        let deserialized = AccountEvent::from_envelope(&envelope).unwrap();
        assert_eq!(deserialized.event_type(), "Account.AccountOpened");
        if let AccountEvent::AccountOpened {
            name,
            initial_balance,
            timestamp: ts,
        } = deserialized
        {
            assert_eq!(name, "Alice");
            assert_eq!(initial_balance, 100);
            assert_eq!(ts, timestamp);
        } else {
            panic!("Expected AccountOpened variant");
        }
    }

    #[test]
    fn test_init_events_envelope_roundtrip_regular_variant() {
        use crate::DomainEvent;
        let timestamp = Utc::now();
        let original = AccountEvent::Deposited {
            amount: 50,
            timestamp,
        };

        let envelope = original.to_envelope(uuid::Uuid::new_v4()).unwrap();

        // Should serialize as flat struct
        assert!(envelope.event_data.get("amount").is_some());
        assert!(envelope.event_data.get("Deposited").is_none());

        let deserialized = AccountEvent::from_envelope(&envelope).unwrap();
        assert_eq!(deserialized.event_type(), "Account.Deposited");
        if let AccountEvent::Deposited {
            amount,
            timestamp: ts,
        } = deserialized
        {
            assert_eq!(amount, 50);
            assert_eq!(ts, timestamp);
        } else {
            panic!("Expected Deposited variant");
        }
    }

    // ===== Init creation function tests =====

    #[test]
    fn test_init_creation_fn_creates_aggregate() {
        let agg = Account::open_account("Alice".to_string(), 100).unwrap();
        assert_eq!(agg.name, "Alice");
        assert_eq!(agg.balance, 100);
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);
    }

    #[test]
    fn test_init_creation_fn_with_id() {
        let id = EntityId::new();
        let agg = Account::open_account_with_id(id, "Bob".to_string(), 200).unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.name, "Bob");
        assert_eq!(agg.balance, 200);
    }

    #[test]
    fn test_init_creation_fn_validation_fails() {
        let result = Account::open_account(String::new(), 100);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty name");
    }

    #[test]
    fn test_init_creation_fn_with_id_validation_fails() {
        let id = EntityId::new();
        let result = Account::open_account_with_id(id, String::new(), 100);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty name");
    }

    #[test]
    fn test_init_creation_fn_then_regular_commands() {
        let mut agg = Account::open_account("Alice".to_string(), 100).unwrap();
        agg.deposit(50).unwrap();
        agg.withdraw(30).unwrap();

        assert_eq!(agg.balance, 120);
        assert_eq!(agg.version(), AggregateVersion::new(3));
        assert_eq!(agg.pending_events().len(), 3);
    }

    #[test]
    fn test_init_creation_fn_generates_unique_ids() {
        let agg1 = Account::open_account("Alice".to_string(), 100).unwrap();
        let agg2 = Account::open_account("Bob".to_string(), 200).unwrap();
        assert_ne!(agg1.entity_id(), agg2.entity_id());
    }

    // ===== Multiple init events on same aggregate tests =====

    #[derive(Debug, thiserror::Error)]
    pub enum MemberError {
        #[error("Empty email")]
        EmptyEmail,
        #[error("Empty name")]
        EmptyName,
        #[error("Invalid invite code")]
        InvalidInviteCode,
        #[error("Already verified")]
        AlreadyVerified,
    }

    impl AggregateError for MemberError {}

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum MemberRole {
        Admin,
        Regular,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Member {
        id: EntityId,
        email: String,
        name: String,
        role: MemberRole,
        verified: bool,
    }

    impl Entity for Member {
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl Aggregate for Member {
        type Event = MemberEvent;
        type Error = MemberError;
        type DeletedState = Self;
    }

    // Two @init events + one regular event
    define_events! {
        pub enum MemberEvent for Member {
            AdminCreated {
                email: String,
                name: String,
            }
            @init
            @validate |evt| {
                if evt.email.is_empty() {
                    return Err(MemberError::EmptyEmail);
                }
                return Ok(());
            }
            => |id, event| {
                Member {
                    id,
                    email: event.email.clone(),
                    name: event.name.clone(),
                    role: MemberRole::Admin,
                    verified: true,
                }
            },

            CreatedByInvite {
                email: String,
                name: String,
                invite_code: String,
            }
            @init
            @validate |evt| {
                if evt.email.is_empty() {
                    return Err(MemberError::EmptyEmail);
                }
                if evt.invite_code.is_empty() {
                    return Err(MemberError::InvalidInviteCode);
                }
                return Ok(());
            }
            => |id, event| {
                Member {
                    id,
                    email: event.email.clone(),
                    name: event.name.clone(),
                    role: MemberRole::Regular,
                    verified: false,
                }
            },

            Verified {} => |member, _event| {
                member.verified = true;
            },
        }
    }

    command_handler! {
        impl Member {
            @clock @init fn create_admin(email: String, name: String)
                -> AdminCreatedEvent { email, name };
            @clock @init fn create_by_invite(email: String, name: String, invite_code: String)
                -> CreatedByInviteEvent { email, name, invite_code };
            @clock fn verify() -> VerifiedEvent { };
        }
    }

    // --- define_events! tests for multiple init variants ---

    #[test]
    fn test_multi_init_event_structs_created() {
        let _admin = AdminCreatedEvent {
            email: "admin@co.com".to_string(),
            name: "Admin".to_string(),
            timestamp: Utc::now(),
        };
        let _invite = CreatedByInviteEvent {
            email: "user@co.com".to_string(),
            name: "User".to_string(),
            invite_code: "ABC123".to_string(),
            timestamp: Utc::now(),
        };
    }

    #[test]
    fn test_multi_init_both_implement_init_event() {
        use crate::InitEvent;
        let id = EntityId::new();

        let admin_evt = AdminCreatedEvent {
            email: "admin@co.com".to_string(),
            name: "Admin".to_string(),
            timestamp: Utc::now(),
        };
        let member = admin_evt.init(id);
        assert_eq!(member.role, MemberRole::Admin);
        assert!(member.verified);

        let invite_evt = CreatedByInviteEvent {
            email: "user@co.com".to_string(),
            name: "User".to_string(),
            invite_code: "ABC123".to_string(),
            timestamp: Utc::now(),
        };
        let member = invite_evt.init(id);
        assert_eq!(member.role, MemberRole::Regular);
        assert!(!member.verified);
    }

    #[test]
    fn test_multi_init_is_init_for_both() {
        use crate::EventApplicator;

        let admin = MemberEvent::AdminCreated {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };
        assert!(EventApplicator::is_init(&admin));

        let invite = MemberEvent::CreatedByInvite {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            invite_code: "X".to_string(),
            timestamp: Utc::now(),
        };
        assert!(EventApplicator::is_init(&invite));

        let verified = MemberEvent::Verified {
            timestamp: Utc::now(),
        };
        assert!(!EventApplicator::is_init(&verified));
    }

    #[test]
    fn test_multi_init_dispatch_init_admin() {
        use crate::EventApplicator;
        let id = EntityId::new();
        let event = MemberEvent::AdminCreated {
            email: "admin@co.com".to_string(),
            name: "Admin".to_string(),
            timestamp: Utc::now(),
        };
        let member = EventApplicator::dispatch_init(&event, id).unwrap();
        assert_eq!(member.email, "admin@co.com");
        assert_eq!(member.role, MemberRole::Admin);
    }

    #[test]
    fn test_multi_init_dispatch_init_invite() {
        use crate::EventApplicator;
        let id = EntityId::new();
        let event = MemberEvent::CreatedByInvite {
            email: "user@co.com".to_string(),
            name: "User".to_string(),
            invite_code: "ABC".to_string(),
            timestamp: Utc::now(),
        };
        let member = EventApplicator::dispatch_init(&event, id).unwrap();
        assert_eq!(member.email, "user@co.com");
        assert_eq!(member.role, MemberRole::Regular);
    }

    #[test]
    fn test_multi_init_dispatch_init_unchecked_both() {
        use crate::EventApplicator;
        let id = EntityId::new();

        let admin = MemberEvent::AdminCreated {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };
        let m = EventApplicator::dispatch_init_unchecked(&admin, id);
        assert_eq!(m.role, MemberRole::Admin);

        let invite = MemberEvent::CreatedByInvite {
            email: "u@b.com".to_string(),
            name: "U".to_string(),
            invite_code: "X".to_string(),
            timestamp: Utc::now(),
        };
        let m = EventApplicator::dispatch_init_unchecked(&invite, id);
        assert_eq!(m.role, MemberRole::Regular);
    }

    #[test]
    fn test_multi_init_validation_per_variant() {
        use crate::InitEvent;

        // Admin: empty email fails
        let evt = AdminCreatedEvent {
            email: String::new(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };
        assert_eq!(evt.validate_init().unwrap_err().to_string(), "Empty email");

        // Invite: empty invite code fails
        let evt = CreatedByInviteEvent {
            email: "a@b.com".to_string(),
            name: "U".to_string(),
            invite_code: String::new(),
            timestamp: Utc::now(),
        };
        assert_eq!(
            evt.validate_init().unwrap_err().to_string(),
            "Invalid invite code"
        );
    }

    // --- command_handler! tests for multiple @init commands ---

    #[test]
    fn test_multi_init_cmd_event_helpers() {
        let admin_evt = Member::create_admin_event("a@b.com".to_string(), "Admin".to_string());
        assert_eq!(admin_evt.email, "a@b.com");

        let invite_evt = Member::create_by_invite_event(
            "u@b.com".to_string(),
            "User".to_string(),
            "INV123".to_string(),
        );
        assert_eq!(invite_evt.invite_code, "INV123");
    }

    #[test]
    fn test_multi_init_cmd_on_uninit_admin() {
        let uninit = crate::UninitAggregateRoot::<Member>::new(EntityId::new());
        let id = uninit.entity_id();
        let agg = uninit
            .create_admin("admin@co.com".to_string(), "Admin".to_string())
            .unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.role, MemberRole::Admin);
        assert!(agg.verified);
        assert_eq!(agg.version(), AggregateVersion::new(1));
    }

    #[test]
    fn test_multi_init_cmd_on_uninit_invite() {
        let uninit = crate::UninitAggregateRoot::<Member>::new(EntityId::new());
        let id = uninit.entity_id();
        let agg = uninit
            .create_by_invite(
                "user@co.com".to_string(),
                "User".to_string(),
                "INV456".to_string(),
            )
            .unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.role, MemberRole::Regular);
        assert!(!agg.verified);
    }

    #[test]
    fn test_multi_init_creation_fn_admin() {
        let agg = Member::create_admin("admin@co.com".to_string(), "Admin".to_string()).unwrap();
        assert_eq!(agg.role, MemberRole::Admin);
        assert!(agg.verified);
    }

    #[test]
    fn test_multi_init_creation_fn_invite() {
        let agg = Member::create_by_invite(
            "user@co.com".to_string(),
            "User".to_string(),
            "INV789".to_string(),
        )
        .unwrap();
        assert_eq!(agg.role, MemberRole::Regular);
        assert!(!agg.verified);
    }

    #[test]
    fn test_multi_init_creation_fn_with_id() {
        let id = EntityId::new();
        let agg = Member::create_admin_with_id(id, "admin@co.com".to_string(), "Admin".to_string())
            .unwrap();
        assert_eq!(agg.entity_id(), id);

        let id2 = EntityId::new();
        let agg = Member::create_by_invite_with_id(
            id2,
            "user@co.com".to_string(),
            "User".to_string(),
            "CODE".to_string(),
        )
        .unwrap();
        assert_eq!(agg.entity_id(), id2);
    }

    #[test]
    fn test_multi_init_creation_fn_validation() {
        let result = Member::create_admin(String::new(), "Admin".to_string());
        assert!(result.is_err());

        let result =
            Member::create_by_invite("a@b.com".to_string(), "User".to_string(), String::new());
        assert!(result.is_err());
    }

    // --- Full lifecycle: init then regular events ---

    #[test]
    fn test_multi_init_admin_then_regular() {
        let mut agg =
            Member::create_admin("admin@co.com".to_string(), "Admin".to_string()).unwrap();
        assert!(agg.verified); // Already verified as admin

        // Verify is a no-op for admin but still works
        agg.verify().unwrap();
        assert!(agg.verified);
        assert_eq!(agg.version(), AggregateVersion::new(2));
        assert_eq!(agg.pending_events().len(), 2);
    }

    #[test]
    fn test_multi_init_invite_then_verify() {
        let mut agg = Member::create_by_invite(
            "user@co.com".to_string(),
            "User".to_string(),
            "INV".to_string(),
        )
        .unwrap();
        assert!(!agg.verified);

        agg.verify().unwrap();
        assert!(agg.verified);
        assert_eq!(agg.version(), AggregateVersion::new(2));
    }

    #[test]
    fn test_multi_init_domain_event_types() {
        use crate::DomainEvent;

        let admin = MemberEvent::AdminCreated {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };
        assert_eq!(admin.event_type(), "Member.AdminCreated");

        let invite = MemberEvent::CreatedByInvite {
            email: "u@b.com".to_string(),
            name: "U".to_string(),
            invite_code: "X".to_string(),
            timestamp: Utc::now(),
        };
        assert_eq!(invite.event_type(), "Member.CreatedByInvite");
    }

    #[test]
    fn test_multi_init_envelope_roundtrip_admin() {
        use crate::DomainEvent;
        let event = MemberEvent::AdminCreated {
            email: "admin@co.com".to_string(),
            name: "Admin".to_string(),
            timestamp: Utc::now(),
        };
        let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();
        let restored = MemberEvent::from_envelope(&envelope).unwrap();
        assert!(matches!(restored, MemberEvent::AdminCreated { .. }));
    }

    #[test]
    fn test_multi_init_envelope_roundtrip_invite() {
        use crate::DomainEvent;
        let event = MemberEvent::CreatedByInvite {
            email: "user@co.com".to_string(),
            name: "User".to_string(),
            invite_code: "INV".to_string(),
            timestamp: Utc::now(),
        };
        let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();
        let restored = MemberEvent::from_envelope(&envelope).unwrap();
        assert!(matches!(restored, MemberEvent::CreatedByInvite { .. }));
    }

    // ===================================================================
    // @actor define_events! + command_handler! tests (in submodule to avoid
    // naming collisions with existing CreatedEvent etc.)
    // ===================================================================

    #[allow(clippy::too_many_lines)]
    mod actor_tests {
        use super::*;

        // --- Actor entity for tests ---

        #[derive(Debug, Serialize, Deserialize)]
        struct Operator {
            id: EntityId,
            is_admin: bool,
        }

        impl Entity for Operator {
            fn new(id: EntityId) -> Self {
                Self {
                    id,
                    is_admin: false,
                }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl crate::DefaultEntity for Operator {}

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum OperatorEvent {
            Noop,
        }

        impl DomainEvent for OperatorEvent {
            type Aggregate = Operator;
            fn event_type(&self) -> &'static str {
                "Operator.Noop"
            }
            fn event_version(&self) -> crate::EventVersion {
                crate::EventVersion::new(1)
            }
            fn occurred_at(&self) -> DateTime<Utc> {
                Utc::now()
            }
        }

        impl crate::EventApplicator<Operator> for OperatorEvent {
            fn dispatch(&self, _: &mut Operator) -> Result<(), TicketError> {
                Ok(())
            }
            fn dispatch_unchecked(&self, _: &mut Operator) {}
        }

        impl Aggregate for Operator {
            type Event = OperatorEvent;
            type Error = TicketError;
            type DeletedState = Self;
        }

        // --- Ticket aggregate with actor events ---

        #[derive(Debug, thiserror::Error)]
        enum TicketError {
            #[error("Not authorized")]
            NotAuthorized,
            #[error("Empty title")]
            EmptyTitle,
            #[error("Already closed")]
            AlreadyClosed,
        }

        impl AggregateError for TicketError {}

        #[derive(Debug, Serialize, Deserialize)]
        struct Ticket {
            id: EntityId,
            title: String,
            assignee: Option<String>,
            closed: bool,
        }

        impl Entity for Ticket {
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl Aggregate for Ticket {
            type Event = TicketEvent;
            type Error = TicketError;
            type DeletedState = Self;
        }

        define_events! {
            enum TicketEvent for Ticket {
                TicketOpened {
                    title: String,
                }
                @init
                @actor(Operator)
                @validate |actor, evt| {
                    if !actor.is_admin {
                        return Err(TicketError::NotAuthorized);
                    }
                    if evt.title.is_empty() {
                        return Err(TicketError::EmptyTitle);
                    }
                    return Ok(());
                }
                => |id, event| {
                    Ticket {
                        id,
                        title: event.title.clone(),
                        assignee: None,
                        closed: false,
                    }
                },

                TicketAssigned {
                    assignee: String,
                }
                @actor(Operator)
                @validate |_ticket, actor, _evt| {
                    if !actor.is_admin {
                        return Err(TicketError::NotAuthorized);
                    }
                    return Ok(());
                }
                => |ticket, event| {
                    ticket.assignee = Some(event.assignee.clone());
                },

                TicketClosed {
                    reason: String,
                }
                @validate |ticket, _event| {
                    if ticket.closed {
                        return Err(TicketError::AlreadyClosed);
                    }
                    return Ok(());
                }
                => |ticket, _event| {
                    ticket.closed = true;
                },
            }
        }

        command_handler! {
            impl Ticket {
                @clock @init @actor(Operator)
                fn open_ticket(title: String) -> TicketOpenedEvent { title };
                @clock @actor(Operator)
                fn assign_ticket(assignee: String) -> TicketAssignedEvent { assignee };
                @clock fn close_ticket(reason: String) -> TicketClosedEvent { reason };
            }
        }

        fn make_admin() -> AggregateRoot<Operator> {
            let entity = Operator {
                id: EntityId::new(),
                is_admin: true,
            };
            AggregateRoot::restore(AggregateVersion::new(1), entity)
        }

        // --- define_events! @actor tests ---

        #[test]
        fn test_actor_event_struct_generated() {
            let _event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let _event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
        }

        #[test]
        fn test_actor_init_event_trait_generated() {
            use crate::ActorInitEvent;
            let event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let admin = Operator {
                id: EntityId::new(),
                is_admin: true,
            };
            assert!(event.validate_init_actor(&admin).is_ok());
        }

        #[test]
        fn test_actor_init_event_validation_rejects_non_admin() {
            use crate::ActorInitEvent;
            let event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let non_admin = Operator::new(EntityId::new());
            let result = event.validate_init_actor(&non_admin);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_actor_regular_event_trait_generated() {
            use crate::ActorEvent;
            let event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            let admin = Operator {
                id: EntityId::new(),
                is_admin: true,
            };
            assert!(event.validate_actor(&ticket, &admin).is_ok());
        }

        #[test]
        fn test_actor_regular_event_validation_rejects() {
            use crate::ActorEvent;
            let event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            let non_admin = Operator::new(EntityId::new());
            let result = event.validate_actor(&ticket, &non_admin);
            assert!(result.is_err());
        }

        #[test]
        fn test_actor_event_apply_works_during_replay() {
            let event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let mut ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            ApplyEvent::apply(&event, &mut ticket);
            assert_eq!(ticket.assignee.as_deref(), Some("Alice"));
        }

        #[test]
        fn test_actor_init_event_init_trait_works() {
            use crate::InitEvent;
            let event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let id = EntityId::new();
            let ticket = event.init(id);
            assert_eq!(ticket.id, id);
            assert_eq!(ticket.title, "Bug");
            assert!(!ticket.closed);
        }

        #[test]
        fn test_actor_event_applicator_dispatch() {
            use crate::EventApplicator;
            let event = TicketEvent::TicketAssigned {
                assignee: "Bob".to_string(),
                timestamp: Utc::now(),
            };
            let mut ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            EventApplicator::dispatch(&event, &mut ticket).unwrap();
            assert_eq!(ticket.assignee.as_deref(), Some("Bob"));
        }

        #[test]
        fn test_actor_event_applicator_dispatch_unchecked() {
            use crate::EventApplicator;
            let event = TicketEvent::TicketAssigned {
                assignee: "Bob".to_string(),
                timestamp: Utc::now(),
            };
            let mut ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            EventApplicator::dispatch_unchecked(&event, &mut ticket);
            assert_eq!(ticket.assignee.as_deref(), Some("Bob"));
        }

        #[test]
        fn test_actor_event_domain_event_types() {
            let event = TicketEvent::TicketOpened {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            assert_eq!(event.event_type(), "Ticket.TicketOpened");

            let event = TicketEvent::TicketAssigned {
                assignee: "Bob".to_string(),
                timestamp: Utc::now(),
            };
            assert_eq!(event.event_type(), "Ticket.TicketAssigned");
        }

        #[test]
        fn test_actor_is_init_classification() {
            use crate::EventApplicator;
            let created = TicketEvent::TicketOpened {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            assert!(EventApplicator::<Ticket>::is_init(&created));

            let assigned = TicketEvent::TicketAssigned {
                assignee: "Bob".to_string(),
                timestamp: Utc::now(),
            };
            assert!(!EventApplicator::<Ticket>::is_init(&assigned));

            let closed = TicketEvent::TicketClosed {
                reason: "done".to_string(),
                timestamp: Utc::now(),
            };
            assert!(!EventApplicator::<Ticket>::is_init(&closed));
        }

        // --- command_handler! @actor tests ---

        #[test]
        fn test_actor_init_cmd_event_helper() {
            let event = Ticket::open_ticket_event("Bug report".to_string());
            assert_eq!(event.title, "Bug report");
        }

        #[test]
        fn test_actor_regular_cmd_event_helper() {
            let ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            let event = ticket.assign_ticket_event("Alice".to_string());
            assert_eq!(event.assignee, "Alice");
        }

        #[test]
        fn test_actor_init_cmd_on_uninit_with_admin() {
            let admin = make_admin();
            let uninit = crate::UninitAggregateRoot::<Ticket>::new(EntityId::new());
            let ticket = uninit
                .open_ticket(&admin, "Bug report".to_string())
                .unwrap();
            assert_eq!(ticket.title, "Bug report");
            assert_eq!(ticket.version(), AggregateVersion::new(1));
            assert_eq!(ticket.pending_events().len(), 1);
        }

        #[test]
        fn test_actor_init_cmd_on_uninit_rejects_non_admin() {
            let non_admin = AggregateRoot::<Operator>::new(EntityId::new());
            let uninit = crate::UninitAggregateRoot::<Ticket>::new(EntityId::new());
            let result = uninit.open_ticket(&non_admin, "Bug report".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_actor_init_creation_fn_with_admin() {
            let admin = make_admin();
            let ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            assert_eq!(ticket.title, "Bug report");
            assert_eq!(ticket.version(), AggregateVersion::new(1));
        }

        #[test]
        fn test_actor_init_creation_fn_rejects_non_admin() {
            let non_admin = AggregateRoot::<Operator>::new(EntityId::new());
            let result = Ticket::open_ticket(&non_admin, "Bug report".to_string());
            assert!(result.is_err());
        }

        #[test]
        fn test_actor_init_creation_fn_with_id() {
            let admin = make_admin();
            let id = EntityId::new();
            let ticket = Ticket::open_ticket_with_id(id, &admin, "Bug report".to_string()).unwrap();
            assert_eq!(ticket.entity_id(), id);
            assert_eq!(ticket.title, "Bug report");
        }

        #[test]
        fn test_actor_init_creation_fn_validates_event_too() {
            let admin = make_admin();
            let result = Ticket::open_ticket(&admin, String::new());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Empty title");
        }

        #[test]
        fn test_actor_regular_cmd_with_admin() {
            let admin = make_admin();
            let mut ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            ticket.assign_ticket(&admin, "Alice".to_string()).unwrap();
            assert_eq!(ticket.assignee.as_deref(), Some("Alice"));
            assert_eq!(ticket.version(), AggregateVersion::new(2));
        }

        #[test]
        fn test_actor_regular_cmd_rejects_non_admin() {
            let admin = make_admin();
            let non_admin = AggregateRoot::<Operator>::new(EntityId::new());
            let mut ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            let result = ticket.assign_ticket(&non_admin, "Alice".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_non_actor_cmd_works_without_actor() {
            let admin = make_admin();
            let mut ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            ticket.close_ticket("done".to_string()).unwrap();
            assert!(ticket.closed);
        }

        #[test]
        fn test_actor_cmd_tracks_actor_id() {
            let admin = make_admin();
            let admin_id = admin.entity_id();

            let mut ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            ticket.assign_ticket(&admin, "Alice".to_string()).unwrap();
            ticket.close_ticket("done".to_string()).unwrap();

            let pending = ticket.pending_events_with_actors();
            assert_eq!(pending[0].actor_id, Some(admin_id));
            assert_eq!(pending[1].actor_id, Some(admin_id));
            assert_eq!(pending[2].actor_id, None);
        }

        #[test]
        fn test_actor_full_lifecycle() {
            let admin = make_admin();

            let mut ticket = Ticket::open_ticket(&admin, "Bug report".to_string()).unwrap();
            assert_eq!(ticket.version(), AggregateVersion::new(1));

            ticket.assign_ticket(&admin, "Alice".to_string()).unwrap();
            assert_eq!(ticket.version(), AggregateVersion::new(2));

            ticket.close_ticket("fixed".to_string()).unwrap();
            assert_eq!(ticket.version(), AggregateVersion::new(3));

            assert_eq!(ticket.title, "Bug report");
            assert_eq!(ticket.assignee.as_deref(), Some("Alice"));
            assert!(ticket.closed);
            assert_eq!(ticket.pending_events().len(), 3);
        }

        #[test]
        fn test_actor_event_envelope_roundtrip() {
            use crate::DomainEvent;
            let event = TicketEvent::TicketAssigned {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();
            let restored = TicketEvent::from_envelope(&envelope).unwrap();
            assert!(matches!(restored, TicketEvent::TicketAssigned { .. }));
        }

        #[test]
        fn test_actor_init_event_envelope_roundtrip() {
            use crate::DomainEvent;
            let event = TicketEvent::TicketOpened {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();
            let restored = TicketEvent::from_envelope(&envelope).unwrap();
            assert!(matches!(restored, TicketEvent::TicketOpened { .. }));
        }

        #[test]
        fn test_actor_dispatch_init_via_event_applicator() {
            use crate::EventApplicator;
            let event = TicketEvent::TicketOpened {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let id = EntityId::new();
            let ticket = EventApplicator::dispatch_init(&event, id).unwrap();
            assert_eq!(ticket.title, "Bug");
            assert_eq!(ticket.entity_id(), id);
        }

        #[test]
        fn test_actor_replay_skips_actor_validation() {
            use crate::EventApplicator;
            let created = TicketEvent::TicketOpened {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let id = EntityId::new();
            let mut ticket = EventApplicator::dispatch_init_unchecked(&created, id);

            let assigned = TicketEvent::TicketAssigned {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            EventApplicator::dispatch_unchecked(&assigned, &mut ticket);
            assert_eq!(ticket.assignee.as_deref(), Some("Alice"));
        }

        // --- Arity-based @validate dispatch tests ---

        #[test]
        fn test_3arg_validate_runs_in_validate_actor_context() {
            // TicketAssigned has @validate |_ticket, actor, _evt| { ... }
            // This 3-arg closure should run in validate_actor(), rejecting non-admins
            use crate::ActorEvent;
            let event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            let non_admin = Operator::new(EntityId::new());
            assert!(event.validate_actor(&ticket, &non_admin).is_err());
        }

        #[test]
        fn test_3arg_validate_skipped_in_replay_validate_context() {
            // TicketAssigned has a 3-arg @validate — should be skipped during replay
            use crate::ApplyEvent;
            let event = TicketAssignedEvent {
                assignee: "Alice".to_string(),
                timestamp: Utc::now(),
            };
            let ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: false,
            };
            // validate() should pass even without actor check (3-arg is skipped)
            assert!(event.validate(&ticket).is_ok());
        }

        #[test]
        fn test_2arg_validate_runs_in_validate_init_actor_context() {
            // TicketOpened has @validate |actor, evt| { ... }
            // This 2-arg closure should run in validate_init_actor()
            use crate::ActorInitEvent;
            let event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            let non_admin = Operator::new(EntityId::new());
            assert!(event.validate_init_actor(&non_admin).is_err());
        }

        #[test]
        fn test_2arg_validate_skipped_in_replay_validate_init_context() {
            // TicketOpened has a 2-arg @validate — should be skipped during replay
            use crate::InitEvent;
            let event = TicketOpenedEvent {
                title: "Bug".to_string(),
                timestamp: Utc::now(),
            };
            // validate_init() should pass (2-arg actor closure is skipped)
            assert!(event.validate_init().is_ok());
        }

        #[test]
        fn test_2arg_validate_on_regular_event_runs_in_validate_context() {
            // TicketClosed has @validate |ticket, _event| { ... }
            // This 2-arg closure should run in validate() (replay)
            use crate::ApplyEvent;
            let event = TicketClosedEvent {
                reason: "done".to_string(),
                timestamp: Utc::now(),
            };
            let closed_ticket = Ticket {
                id: EntityId::new(),
                title: "Bug".to_string(),
                assignee: None,
                closed: true,
            };
            assert!(event.validate(&closed_ticket).is_err());
        }

        #[test]
        fn test_actor_init_validates_event_data_in_actor_context() {
            // TicketOpened validates both actor.is_admin AND evt.title.is_empty()
            // With 2-arg |actor, evt|, both checks run in validate_init_actor
            use crate::ActorInitEvent;
            let event = TicketOpenedEvent {
                title: String::new(), // empty title
                timestamp: Utc::now(),
            };
            let admin = Operator {
                id: EntityId::new(),
                is_admin: true,
            };
            // Admin passes actor check but event data check fails
            let result = event.validate_init_actor(&admin);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Empty title");
        }
    }

    // ===================================================================
    // @encrypted_fields define_events! tests (in submodule to avoid
    // naming collisions with existing generated types)
    // ===================================================================

    mod encrypted_fields_tests {
        use super::*;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Patient {
            id: EntityId,
            name: String,
            diagnosis: String,
            visit_count: i32,
        }

        impl Entity for Patient {
            fn new(id: EntityId) -> Self {
                Self {
                    id,
                    name: String::new(),
                    diagnosis: String::new(),
                    visit_count: 0,
                }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl crate::DefaultEntity for Patient {}

        #[derive(Debug, thiserror::Error)]
        #[error("Patient error")]
        struct PatientError;

        impl AggregateError for PatientError {}

        impl Aggregate for Patient {
            type Event = PatientEvent;
            type Error = PatientError;
            type DeletedState = Self;
        }

        define_events! {
            enum PatientEvent for Patient {
                Registered {
                    name: String,
                    diagnosis: String,
                    visit_count: i32,
                }
                @encrypted_fields(name, diagnosis)
                => |patient, event| {
                    patient.name = event.name.clone();
                    patient.diagnosis = event.diagnosis.clone();
                    patient.visit_count = event.visit_count;
                },

                VisitRecorded {
                    visit_count: i32,
                }
                => |patient, event| {
                    patient.visit_count = event.visit_count;
                },
            }
        }

        #[test]
        fn test_encrypted_fields_returns_field_names_for_encrypted_variant() {
            let event = PatientEvent::Registered {
                name: "Alice".to_string(),
                diagnosis: "flu".to_string(),
                visit_count: 1,
                timestamp: Utc::now(),
            };
            let fields = event.encrypted_fields();
            assert_eq!(fields, &["name", "diagnosis"]);
        }

        #[test]
        fn test_encrypted_fields_returns_empty_for_non_encrypted_variant() {
            let event = PatientEvent::VisitRecorded {
                visit_count: 2,
                timestamp: Utc::now(),
            };
            assert!(event.encrypted_fields().is_empty());
        }

        #[test]
        fn test_has_any_encrypted_fields_is_true() {
            assert!(PatientEvent::has_any_encrypted_fields());
        }

        // Test a define_events! with NO encrypted fields at all
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct PlainEntity {
            id: EntityId,
            value: i32,
        }

        impl Entity for PlainEntity {
            fn new(id: EntityId) -> Self {
                Self { id, value: 0 }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl crate::DefaultEntity for PlainEntity {}

        #[derive(Debug, thiserror::Error)]
        #[error("Plain error")]
        struct PlainError;

        impl AggregateError for PlainError {}

        impl Aggregate for PlainEntity {
            type Event = PlainEvent;
            type Error = PlainError;
            type DeletedState = Self;
        }

        define_events! {
            enum PlainEvent for PlainEntity {
                Updated {
                    value: i32,
                }
                => |entity, event| {
                    entity.value = event.value;
                },
            }
        }

        #[test]
        fn test_has_any_encrypted_fields_false_when_no_encrypted_fields() {
            assert!(!PlainEvent::has_any_encrypted_fields());
        }

        #[test]
        fn test_encrypted_fields_empty_for_plain_event() {
            let event = PlainEvent::Updated {
                value: 42,
                timestamp: Utc::now(),
            };
            assert!(event.encrypted_fields().is_empty());
        }
    }

    mod delete_tests {
        use super::*;
        use crate::{
            ActorDeleteEvent, AggregateRoot, AggregateVersion, DeleteEvent, DeletedAggregateRoot,
            EventApplicator,
        };

        // --- Operator entity (actor) ---

        #[derive(Debug, Serialize, Deserialize)]
        struct Admin {
            id: EntityId,
            role: String,
        }

        impl Entity for Admin {
            fn new(id: EntityId) -> Self {
                Self {
                    id,
                    role: String::new(),
                }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl crate::DefaultEntity for Admin {}

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum AdminEvent {
            Noop,
        }

        impl DomainEvent for AdminEvent {
            type Aggregate = Admin;
            fn event_type(&self) -> &'static str {
                "Admin.Noop"
            }
            fn event_version(&self) -> crate::EventVersion {
                crate::EventVersion::new(1)
            }
            fn occurred_at(&self) -> DateTime<Utc> {
                Utc::now()
            }
        }

        impl crate::EventApplicator<Admin> for AdminEvent {
            fn dispatch(&self, _: &mut Admin) -> Result<(), AccountError> {
                Ok(())
            }
            fn dispatch_unchecked(&self, _: &mut Admin) {}
        }

        impl Aggregate for Admin {
            type Event = AdminEvent;
            type Error = AccountError;
            type DeletedState = Self;
        }

        // --- Account aggregate with delete events ---

        #[derive(Debug, thiserror::Error)]
        enum AccountError {
            #[error("Already deactivated")]
            AlreadyDeactivated,
            #[error("Not authorized")]
            NotAuthorized,
        }

        impl AggregateError for AccountError {}

        #[derive(Debug, Serialize, Deserialize)]
        struct Account {
            id: EntityId,
            name: String,
            active: bool,
        }

        impl Entity for Account {
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl Aggregate for Account {
            type Event = AccountEvent;
            type Error = AccountError;
            type DeletedState = Self;
        }

        define_events! {
            enum AccountEvent for Account {
                AccountCreated {
                    name: String,
                }
                @init
                => |id, event| {
                    Account {
                        id,
                        name: event.name.clone(),
                        active: true,
                    }
                },

                AccountUpdated {
                    name: String,
                }
                @validate |account, _event| {
                    if !account.active {
                        return Err(AccountError::AlreadyDeactivated);
                    }
                    return Ok(());
                }
                => |account, event| {
                    account.name = event.name.clone();
                },

                AccountDeactivated {
                    reason: String,
                }
                @delete
                @validate |account, _event| {
                    if !account.active {
                        return Err(AccountError::AlreadyDeactivated);
                    }
                    return Ok(());
                }
                => |mut account, _event| {
                    account.active = false;
                    account
                },

                AccountAdminDeleted {
                    reason: String,
                }
                @delete
                @actor(Admin)
                @validate |account, admin, _event| {
                    if admin.role != "superadmin" {
                        return Err(AccountError::NotAuthorized);
                    }
                    if !account.active {
                        return Err(AccountError::AlreadyDeactivated);
                    }
                    return Ok(());
                }
                => |mut account, _event| {
                    account.active = false;
                    account.name.clear();
                    account
                },
            }
        }

        command_handler! {
            impl Account {
                @clock @init fn create_account(name: String) -> AccountCreatedEvent { name };
                @clock fn update_account(name: String) -> AccountUpdatedEvent { name };
                @clock @delete fn deactivate_account(reason: String) -> AccountDeactivatedEvent { reason };
                @clock @delete @actor(Admin)
                fn admin_delete_account(reason: String) -> AccountAdminDeletedEvent { reason };
            }
        }

        fn make_admin(role: &str) -> AggregateRoot<Admin> {
            let entity = Admin {
                id: EntityId::new(),
                role: role.to_string(),
            };
            AggregateRoot::restore(AggregateVersion::new(1), entity)
        }

        // --- define_events! @delete tests ---

        #[test]
        fn test_delete_event_struct_generated() {
            let _event = AccountDeactivatedEvent {
                reason: "goodbye".to_string(),
                timestamp: Utc::now(),
            };
            let _event = AccountAdminDeletedEvent {
                reason: "policy".to_string(),
                timestamp: Utc::now(),
            };
        }

        #[test]
        fn test_delete_event_trait_generated() {
            let event = AccountDeactivatedEvent {
                reason: "goodbye".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            assert!(event.validate_delete(&account).is_ok());
        }

        #[test]
        fn test_delete_event_validation_rejects() {
            let event = AccountDeactivatedEvent {
                reason: "goodbye".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: false,
            };
            let result = event.validate_delete(&account);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Already deactivated");
        }

        #[test]
        fn test_delete_event_delete_fn() {
            let event = AccountDeactivatedEvent {
                reason: "goodbye".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let deleted = event.delete(account);
            assert!(!deleted.active);
        }

        #[test]
        fn test_actor_delete_event_trait_generated() {
            let event = AccountAdminDeletedEvent {
                reason: "policy".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let admin = Admin {
                id: EntityId::new(),
                role: "superadmin".to_string(),
            };
            assert!(event.validate_delete_actor(&account, &admin).is_ok());
        }

        #[test]
        fn test_actor_delete_event_validation_rejects_non_superadmin() {
            let event = AccountAdminDeletedEvent {
                reason: "policy".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let admin = Admin {
                id: EntityId::new(),
                role: "regular".to_string(),
            };
            let result = event.validate_delete_actor(&account, &admin);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_actor_delete_event_delete_fn_clears_name() {
            let event = AccountAdminDeletedEvent {
                reason: "policy".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let deleted = event.delete(account);
            assert!(!deleted.active);
            assert!(deleted.name.is_empty());
        }

        // --- EventApplicator tests ---

        #[test]
        fn test_event_applicator_is_delete() {
            let delete_event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            let actor_delete_event = AccountEvent::AccountAdminDeleted {
                reason: "admin".to_string(),
                timestamp: Utc::now(),
            };
            let regular_event = AccountEvent::AccountUpdated {
                name: "Bob".to_string(),
                timestamp: Utc::now(),
            };
            let init_event = AccountEvent::AccountCreated {
                name: "Alice".to_string(),
                timestamp: Utc::now(),
            };

            assert!(delete_event.is_delete());
            assert!(actor_delete_event.is_delete());
            assert!(!regular_event.is_delete());
            assert!(!init_event.is_delete());
        }

        #[test]
        fn test_event_applicator_is_init_false_for_delete() {
            let delete_event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            assert!(!delete_event.is_init());
        }

        #[test]
        fn test_event_applicator_dispatch_delete() {
            let event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let deleted = event.dispatch_delete(account).unwrap();
            assert!(!deleted.active);
        }

        #[test]
        fn test_event_applicator_dispatch_delete_unchecked() {
            let event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let deleted = event.dispatch_delete_unchecked(account);
            assert!(!deleted.active);
        }

        // --- AggregateRoot::apply_delete tests ---

        #[test]
        fn test_apply_delete_type_state_transition() {
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let event = AccountDeactivatedEvent {
                reason: "goodbye".to_string(),
                timestamp: Utc::now(),
            };
            let deleted: DeletedAggregateRoot<Account> = agg.apply_delete(event).unwrap();
            assert!(!deleted.active);
            assert_eq!(deleted.pending_events().len(), 2); // init + delete
        }

        #[test]
        fn test_apply_delete_validation_fails() {
            let agg = Account::create_account("Alice".to_string()).unwrap();
            // Deactivate first
            let event = AccountDeactivatedEvent {
                reason: "first".to_string(),
                timestamp: Utc::now(),
            };
            let deleted = agg.apply_delete(event).unwrap();
            assert!(!deleted.active);
            // Can't create a second delete from the same aggregate since it's consumed
            // The type-state pattern enforces this at compile time
        }

        #[test]
        fn test_apply_delete_with_actor() {
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let admin = make_admin("superadmin");
            let event = AccountAdminDeletedEvent {
                reason: "policy".to_string(),
                timestamp: Utc::now(),
            };
            let deleted = agg
                .apply_delete_with_actor(event, admin.entity_id())
                .unwrap();
            assert!(!deleted.active);
            assert!(deleted.name.is_empty());
        }

        // --- command_handler! @delete tests ---

        #[test]
        fn test_delete_command_event_helper() {
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let event = account.deactivate_account_event("goodbye".to_string());
            assert_eq!(event.reason, "goodbye");
        }

        #[test]
        fn test_actor_delete_command_event_helper() {
            let account = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: true,
            };
            let event = account.admin_delete_account_event("policy".to_string());
            assert_eq!(event.reason, "policy");
        }

        #[test]
        fn test_delete_command_on_aggregate_root() {
            use crate::macros::define_events::tests::delete_tests::AccountDeleteCommands;
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let deleted = agg.deactivate_account("goodbye".to_string()).unwrap();
            assert!(!deleted.active);
        }

        #[test]
        fn test_delete_command_validation_rejects() {
            use crate::macros::define_events::tests::delete_tests::AccountDeleteCommands;
            // Build an inactive account via snapshot
            let entity = Account {
                id: EntityId::new(),
                name: "Alice".to_string(),
                active: false,
            };
            let agg = AggregateRoot::restore(AggregateVersion::new(1), entity);
            let result = agg.deactivate_account("again".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Already deactivated");
        }

        #[test]
        fn test_actor_delete_command_on_aggregate_root() {
            use crate::macros::define_events::tests::delete_tests::AccountDeleteCommands;
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let admin = make_admin("superadmin");
            let deleted = agg
                .admin_delete_account(&admin, "policy".to_string())
                .unwrap();
            assert!(!deleted.active);
            assert!(deleted.name.is_empty());
        }

        #[test]
        fn test_actor_delete_command_validation_rejects() {
            use crate::macros::define_events::tests::delete_tests::AccountDeleteCommands;
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let non_admin = make_admin("regular");
            let result = agg.admin_delete_account(&non_admin, "policy".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_delete_command_full_lifecycle() {
            use crate::macros::define_events::tests::delete_tests::AccountDeleteCommands;
            // Create -> Update -> Delete
            let mut agg = Account::create_account("Alice".to_string()).unwrap();
            agg.update_account("Bob".to_string()).unwrap();
            assert_eq!(agg.entity().name, "Bob");
            let deleted = agg.deactivate_account("leaving".to_string()).unwrap();
            assert!(!deleted.active);
            assert_eq!(deleted.pending_events().len(), 3); // create + update + delete
        }

        #[test]
        fn test_delete_event_from_conversion() {
            let event = AccountDeactivatedEvent {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            let enum_event: AccountEvent = event.into();
            assert!(matches!(
                enum_event,
                AccountEvent::AccountDeactivated { .. }
            ));
        }

        #[test]
        fn test_delete_event_domain_event_type() {
            let event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            assert_eq!(event.event_type(), "Account.AccountDeactivated");
        }

        #[test]
        fn test_delete_event_envelope_roundtrip() {
            let event = AccountEvent::AccountDeactivated {
                reason: "bye".to_string(),
                timestamp: Utc::now(),
            };
            let envelope = event.to_envelope(uuid::Uuid::new_v4()).unwrap();
            let restored = AccountEvent::from_envelope(&envelope).unwrap();
            assert!(matches!(restored, AccountEvent::AccountDeactivated { .. }));
        }
    }
}
