//! Helper macros for reducing boilerplate in event-sourced aggregates
//!
//! This module provides declarative macros that simplify common patterns in event sourcing:
//! - `command_handler!` - Automatic command method generation
//! - `policy!` - Declarative policy definition for cross-aggregate event reactions

/// Generate command handler methods for an aggregate.
///
/// This macro reduces boilerplate by automatically generating command methods
/// that create events, apply timestamps, and apply the events via `AggregateRoot`.
/// It generates:
///
/// 1. **Event helper functions** on the entity type (for creating events)
/// 2. **A commands trait** implemented on `AggregateRoot<Entity>` (for applying them)
/// 3. **Init commands trait** (if any `@init` commands) on `UninitAggregateRoot<Entity>`
/// 4. **Init creation functions** (if any `@init` commands) as associated functions on the entity
///
/// # Syntax
///
/// ```ignore
/// command_handler! {
///     impl AggregateType {
///         // Regular commands (on AggregateRoot)
///         fn command_name(param1: Type1, param2: Type2)
///             -> EventStruct { field1, field2 };
///
///         // Init commands (on UninitAggregateRoot + creation functions)
///         // Multiple @init commands are supported for different creation paths
///         @init fn create(param1: Type1)
///             -> CreatedEventStruct { field1 };
///         @init fn create_by_invite(param1: Type1, code: String)
///             -> InvitedEventStruct { field1, code };
///     }
/// }
/// ```
///
/// # Generated Code
///
/// For each **regular** command, the macro generates:
///
/// 1. **Event helper function** on entity: `<command>_event(&self, params) -> EventStruct`
///    - Creates the event struct with provided parameters
///    - Takes every field of the event as a parameter, instants included. A
///      command marked `@clock` gets a `timestamp` stamped from the wall clock
///      instead — see below
///    - Returns the event **without applying it**
///
/// 2. **Commands trait + impl on `AggregateRoot`**:
///    - `<command>(&mut self, params) -> Result<(), Error>`
///    - Applies the event through `AggregateRoot::apply()`
///
/// For each **`@init`** command, the macro generates:
///
/// 1. **Event helper associated function**: `Aggregate::cmd_event(params) -> EventStruct` (no `&self`)
/// 2. **Init commands trait + impl on `UninitAggregateRoot`**:
///    - `<command>(self, params) -> Result<AggregateRoot<A>, Error>`
/// 3. **Creation functions** as associated functions on the entity:
///    - `Aggregate::cmd(params) -> Result<AggregateRoot<A>, Error>` (random ID)
///    - `Aggregate::cmd_with_id(id, params) -> Result<AggregateRoot<A>, Error>` (explicit ID)
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::command_handler;
///
/// command_handler! {
///     impl Order {
///         @init fn create_order(user_id: EntityId) -> OrderCreatedEvent { user_id };
///         fn add_item(item: String, qty: u32) -> ItemAddedEvent { item, qty };
///     }
/// }
///
/// // Creation functions (random ID):
/// let mut order = Order::create_order(user_id)?;
///
/// // Creation functions (explicit ID):
/// let mut order = Order::create_order_with_id(id, user_id)?;
///
/// // Regular commands (on AggregateRoot):
/// order.add_item("laptop".to_string(), 1)?;
/// ```
///
/// # Benefits
///
/// - Reduces boilerplate by ~70%
/// - Ensures consistent command pattern
/// - Automatic timestamp handling
/// - Type-safe command parameters
/// - Commands operate through `AggregateRoot` (events are the only mutation path)
/// - Init creation functions provide ergonomic aggregate construction
///
/// # `@clock`, for a command that means "whenever this ran"
///
/// By default a command takes every field of its event as a parameter, instants
/// included, and the generated body reads no clock. That is the form that cannot be
/// wrong: an event is usually ABOUT an instant, and stamping it with the moment the
/// command happened to execute is right only when those coincide.
///
/// It is also what lets a DOMAIN crate compile `chrono` without the `clock` feature,
/// so `Utc::now()` does not compile there and the purity is enforced rather than
/// agreed; and what lets a producer whose facts carry two clocks — the time the world
/// did something, and the time this service found out — name both, where a single
/// injected `timestamp` could only be one of them and would read as either.
///
/// So the default is:
///
/// ```text
/// command_handler! {
///     impl Sensor {
///         fn measure(
///             measured_at: DateTime<Utc>,
///             received_at: DateTime<Utc>,
///             celsius: i32,
///         ) -> MeasuredEvent { measured_at, received_at, celsius };
///     }
/// }
/// ```
///
/// `@clock` is the opt-in for the other case:
///
/// ```text
/// command_handler! {
///     impl Session {
///         @clock fn touch() -> TouchedEvent { };
///     }
/// }
/// ```
///
/// which stamps a `timestamp` field from `Utc::now()`. Pair the default with
/// [`define_events!`](crate::define_events)'s `@occurred_at`, which names the instant
/// the event declares.
#[macro_export]
macro_rules! command_handler {
    // Single entry point — always delegates to TT muncher.
    // Supports `@init`, `@actor(Type)`, `@delete`, and regular commands.
    (
        impl $aggregate:ty {
            $($rest:tt)*
        }
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: []
            regular_commands: []
            actor_commands: []
            actor_init_commands: []
            delete_commands: []
            actor_delete_commands: []
            $($rest)*
        }
    };
}
/// The event literal a command builds, with or without a clock read.
///
/// `clock: [now]` is what `@clock` selects: the generated event carries a
/// `timestamp` and the command fills it from the wall clock.
///
/// `clock: [none]` is the default. The command lists every field of the event,
/// instants included, and nothing here reads a clock.
#[macro_export]
#[doc(hidden)]
macro_rules! __command_handler_event {
    (clock: [now] $evt:ident { $($field:ident: $value:expr,)* }) => {
        $evt {
            $($field: $value,)*
            timestamp: $crate::__private::chrono::Utc::now(),
        }
    };
    (clock: [none] $evt:ident { $($field:ident: $value:expr,)* }) => {
        $evt {
            $($field: $value,)*
        }
    };
}

/// Internal TT muncher for command_handler! — supports @init flag on individual commands.
///
/// When `@init` is present on a command:
/// - Event helper is an **associated function** (no `&self`): `Aggregate::cmd_event(params) -> Event`
/// - Command method is on `UninitAggregateRoot<Aggregate>`, consuming `self` and returning
///   `Result<AggregateRoot<Aggregate>, Error>`
/// - Creation functions are generated: `Aggregate::cmd(params) -> Result<AggregateRoot<A>, Error>`
///   and `Aggregate::cmd_with_id(id, params) -> Result<AggregateRoot<A>, Error>`
///
/// Regular commands (no `@init`) generate the same code as before.
// Internal TT-muncher for `command_handler!`.
//
// Each arm matches a distinct command shape (regular / @init / @delete /
// @actor(Ty) / combinations) and pushes the parsed metadata into the
// corresponding accumulator before recursing on the tail.
//
// | Markers                  | Accumulator             | Variant kind   |
// |--------------------------|-------------------------|----------------|
// | (none)                   | `regular_commands`      | regular        |
// | `@actor(T)`              | `actor_commands`        | actor          |
// | `@init`                  | `init_commands`         | init           |
// | `@init @actor(T)`        | `actor_init_commands`   | actor_init     |
// | `@delete`                | `delete_commands`       | delete         |
// | `@delete @actor(T)`      | `actor_delete_commands` | actor_delete   |
//
// The arms are listed most-specific-first (combined markers before bare
// markers) so macro_rules picks the right one for compound annotations like
// `@init @actor(T)`. After the last command is munched, the base case fires
// and emits the actual command-method impls keyed off each accumulator.
#[doc(hidden)]
#[macro_export]
macro_rules! __command_handler_init_internal {
    // ---- TT muncher: parse @init @actor(Type) command ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        @init
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] actor: $actor_type } ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @init
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] actor: $actor_type } ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };

    // ---- TT muncher: parse @init command (no actor) ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        @init
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] } ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @init
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] } ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };

    // ---- TT muncher: parse @delete @actor(Type) command ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        @delete
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] actor: $actor_type } ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @delete
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] actor: $actor_type } ]
            $($($rest)*)?
        }
    };

    // ---- TT muncher: parse @delete command (no actor) ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        @delete
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] } ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @delete
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] } ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };

    // ---- TT muncher: parse @actor(Type) command (regular + actor) ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] actor: $actor_type } ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @actor($actor_type:ty)
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* ]
            actor_commands: [ $($a_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] actor: $actor_type } ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };

    // ---- TT muncher: parse regular command (no @init, no @actor, no @delete) ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        @clock
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [now] } ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };
    (
        @munch [$aggregate:ty]
        init_commands: [ $($i_acc:tt)* ]
        regular_commands: [ $($r_acc:tt)* ]
        actor_commands: [ $($a_acc:tt)* ]
        actor_init_commands: [ $($ai_acc:tt)* ]
        delete_commands: [ $($d_acc:tt)* ]
        actor_delete_commands: [ $($ad_acc:tt)* ]

        $(#[$attr:meta])*
        fn $command:ident($($param:ident: $param_ty:ty),* $(,)?)
            -> $event_struct:ident { $($field:ident),* $(,)? }
        $(; $($rest:tt)*)?
    ) => {
        $crate::__command_handler_init_internal! {
            @munch [$aggregate]
            init_commands: [ $($i_acc)* ]
            regular_commands: [ $($r_acc)* { attrs: [$($attr),*] cmd: $command ($($param: $param_ty),*) -> $event_struct { $($field),* } clock: [none] } ]
            actor_commands: [ $($a_acc)* ]
            actor_init_commands: [ $($ai_acc)* ]
            delete_commands: [ $($d_acc)* ]
            actor_delete_commands: [ $($ad_acc)* ]
            $($($rest)*)?
        }
    };

    // ---- Base case: all commands parsed, emit code ----
    (
        @munch [$aggregate:ty]
        init_commands: [ $({ attrs: [$($i_attr:meta),*] cmd: $i_cmd:ident ($($i_param:ident: $i_param_ty:ty),*) -> $i_evt:ident { $($i_field:ident),* } clock: [$i_clock:ident] })* ]
        regular_commands: [ $({ attrs: [$($r_attr:meta),*] cmd: $r_cmd:ident ($($r_param:ident: $r_param_ty:ty),*) -> $r_evt:ident { $($r_field:ident),* } clock: [$r_clock:ident] })* ]
        actor_commands: [ $({ attrs: [$($a_attr:meta),*] cmd: $a_cmd:ident ($($a_param:ident: $a_param_ty:ty),*) -> $a_evt:ident { $($a_field:ident),* } clock: [$a_clock:ident] actor: $a_actor:ty })* ]
        actor_init_commands: [ $({ attrs: [$($ai_attr:meta),*] cmd: $ai_cmd:ident ($($ai_param:ident: $ai_param_ty:ty),*) -> $ai_evt:ident { $($ai_field:ident),* } clock: [$ai_clock:ident] actor: $ai_actor:ty })* ]
        delete_commands: [ $({ attrs: [$($d_attr:meta),*] cmd: $d_cmd:ident ($($d_param:ident: $d_param_ty:ty),*) -> $d_evt:ident { $($d_field:ident),* } clock: [$d_clock:ident] })* ]
        actor_delete_commands: [ $({ attrs: [$($ad_attr:meta),*] cmd: $ad_cmd:ident ($($ad_param:ident: $ad_param_ty:ty),*) -> $ad_evt:ident { $($ad_field:ident),* } clock: [$ad_clock:ident] actor: $ad_actor:ty })* ]
    ) => {
        // --- Init event helpers: associated functions (no &self) ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$i_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$i_cmd _event>]($($i_param: $i_param_ty),*) -> $i_evt {
                        $crate::__command_handler_event! {
                            clock: [$i_clock] $i_evt { $($i_field: $i_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Actor init event helpers: associated functions (no &self) ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$ai_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$ai_cmd _event>]($($ai_param: $ai_param_ty),*) -> $ai_evt {
                        $crate::__command_handler_event! {
                            clock: [$ai_clock] $ai_evt { $($ai_field: $ai_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Regular event helpers: instance methods ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$r_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$r_cmd _event>](&self, $($r_param: $r_param_ty),*) -> $r_evt {
                        $crate::__command_handler_event! {
                            clock: [$r_clock] $r_evt { $($r_field: $r_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Actor event helpers: instance methods ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$a_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$a_cmd _event>](&self, $($a_param: $a_param_ty),*) -> $a_evt {
                        $crate::__command_handler_event! {
                            clock: [$a_clock] $a_evt { $($a_field: $a_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Init commands trait + impl on UninitAggregateRoot ---
        $crate::__private::paste! {
            #[allow(missing_docs, private_interfaces)]
            pub trait [<$aggregate InitCommands>] {
                $(
                    $(#[$i_attr])*
                    fn $i_cmd(self, $($i_param: $i_param_ty),*)
                        -> ::std::result::Result<$crate::AggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>;
                )*
                $(
                    $(#[$ai_attr])*
                    fn $ai_cmd(self, actor: &$crate::AggregateRoot<$ai_actor>, $($ai_param: $ai_param_ty),*)
                        -> ::std::result::Result<$crate::AggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>;
                )*
            }

            impl [<$aggregate InitCommands>] for $crate::UninitAggregateRoot<$aggregate> {
                $(
                    $(#[$i_attr])*
                    fn $i_cmd(self, $($i_param: $i_param_ty),*)
                        -> ::std::result::Result<$crate::AggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = $aggregate::[<$i_cmd _event>]($($i_param),*);
                        self.apply_init(event)
                    }
                )*
                $(
                    $(#[$ai_attr])*
                    fn $ai_cmd(self, actor: &$crate::AggregateRoot<$ai_actor>, $($ai_param: $ai_param_ty),*)
                        -> ::std::result::Result<$crate::AggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = $aggregate::[<$ai_cmd _event>]($($ai_param),*);
                        $crate::ActorInitEvent::validate_init_actor(&event, actor.entity())?;
                        self.apply_init_with_actor(event, actor.entity_id())
                    }
                )*
            }
        }

        // --- Init creation functions: associated functions returning AggregateRoot ---
        $(
            $crate::__private::paste! {
                impl $aggregate {
                    $(#[$i_attr])*
                    #[allow(missing_docs)]
                    pub fn $i_cmd($($i_param: $i_param_ty),*)
                        -> ::std::result::Result<
                            $crate::AggregateRoot<$aggregate>,
                            <$aggregate as $crate::Aggregate>::Error,
                        >
                    {
                        let event = $aggregate::[<$i_cmd _event>]($($i_param),*);
                        $crate::UninitAggregateRoot::<$aggregate>::new(
                            $crate::EntityId::new()
                        ).apply_init(event)
                    }

                    $(#[$i_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$i_cmd _with_id>](id: $crate::EntityId, $($i_param: $i_param_ty),*)
                        -> ::std::result::Result<
                            $crate::AggregateRoot<$aggregate>,
                            <$aggregate as $crate::Aggregate>::Error,
                        >
                    {
                        let event = $aggregate::[<$i_cmd _event>]($($i_param),*);
                        $crate::UninitAggregateRoot::<$aggregate>::new(id).apply_init(event)
                    }
                }
            }
        )*

        // --- Actor init creation functions ---
        $(
            $crate::__private::paste! {
                impl $aggregate {
                    $(#[$ai_attr])*
                    #[allow(missing_docs)]
                    pub fn $ai_cmd(actor: &$crate::AggregateRoot<$ai_actor>, $($ai_param: $ai_param_ty),*)
                        -> ::std::result::Result<
                            $crate::AggregateRoot<$aggregate>,
                            <$aggregate as $crate::Aggregate>::Error,
                        >
                    {
                        let event = $aggregate::[<$ai_cmd _event>]($($ai_param),*);
                        $crate::ActorInitEvent::validate_init_actor(&event, actor.entity())?;
                        $crate::UninitAggregateRoot::<$aggregate>::new(
                            $crate::EntityId::new()
                        ).apply_init_with_actor(event, actor.entity_id())
                    }

                    $(#[$ai_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$ai_cmd _with_id>](id: $crate::EntityId, actor: &$crate::AggregateRoot<$ai_actor>, $($ai_param: $ai_param_ty),*)
                        -> ::std::result::Result<
                            $crate::AggregateRoot<$aggregate>,
                            <$aggregate as $crate::Aggregate>::Error,
                        >
                    {
                        let event = $aggregate::[<$ai_cmd _event>]($($ai_param),*);
                        $crate::ActorInitEvent::validate_init_actor(&event, actor.entity())?;
                        $crate::UninitAggregateRoot::<$aggregate>::new(id).apply_init_with_actor(event, actor.entity_id())
                    }
                }
            }
        )*

        // --- Regular commands trait + impl on AggregateRoot ---
        $crate::__private::paste! {
            #[allow(missing_docs, private_interfaces)]
            pub trait [<$aggregate Commands>] {
                $(
                    $(#[$r_attr])*
                    fn $r_cmd(&mut self, $($r_param: $r_param_ty),*)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>;
                )*
                $(
                    $(#[$a_attr])*
                    fn $a_cmd(&mut self, actor: &$crate::AggregateRoot<$a_actor>, $($a_param: $a_param_ty),*)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>;
                )*
            }

            impl [<$aggregate Commands>] for $crate::AggregateRoot<$aggregate> {
                $(
                    $(#[$r_attr])*
                    fn $r_cmd(&mut self, $($r_param: $r_param_ty),*)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = self.[<$r_cmd _event>]($($r_param),*);
                        self.apply(event)
                    }
                )*
                $(
                    $(#[$a_attr])*
                    fn $a_cmd(&mut self, actor: &$crate::AggregateRoot<$a_actor>, $($a_param: $a_param_ty),*)
                        -> ::std::result::Result<(), <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = self.[<$a_cmd _event>]($($a_param),*);
                        $crate::ActorEvent::validate_actor(&event, self.entity(), actor.entity())?;
                        self.apply_with_actor(event, actor.entity_id())
                    }
                )*
            }
        }

        // --- Delete event helpers: instance methods ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$d_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$d_cmd _event>](&self, $($d_param: $d_param_ty),*) -> $d_evt {
                        $crate::__command_handler_event! {
                            clock: [$d_clock] $d_evt { $($d_field: $d_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Actor delete event helpers: instance methods ---
        impl $aggregate {
            $(
                $crate::__private::paste! {
                    $(#[$ad_attr])*
                    #[allow(missing_docs)]
                    pub fn [<$ad_cmd _event>](&self, $($ad_param: $ad_param_ty),*) -> $ad_evt {
                        $crate::__command_handler_event! {
                            clock: [$ad_clock] $ad_evt { $($ad_field: $ad_param,)* }
                            }
                    }
                }
            )*
        }

        // --- Delete commands trait + impl on AggregateRoot ---
        $crate::__private::paste! {
            #[allow(missing_docs, private_interfaces)]
            pub trait [<$aggregate DeleteCommands>] {
                $(
                    $(#[$d_attr])*
                    fn $d_cmd(self, $($d_param: $d_param_ty),*)
                        -> ::std::result::Result<$crate::DeletedAggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>;
                )*
                $(
                    $(#[$ad_attr])*
                    fn $ad_cmd(self, actor: &$crate::AggregateRoot<$ad_actor>, $($ad_param: $ad_param_ty),*)
                        -> ::std::result::Result<$crate::DeletedAggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>;
                )*
            }

            impl [<$aggregate DeleteCommands>] for $crate::AggregateRoot<$aggregate> {
                $(
                    $(#[$d_attr])*
                    fn $d_cmd(self, $($d_param: $d_param_ty),*)
                        -> ::std::result::Result<$crate::DeletedAggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = self.entity().[<$d_cmd _event>]($($d_param),*);
                        self.apply_delete(event)
                    }
                )*
                $(
                    $(#[$ad_attr])*
                    fn $ad_cmd(self, actor: &$crate::AggregateRoot<$ad_actor>, $($ad_param: $ad_param_ty),*)
                        -> ::std::result::Result<$crate::DeletedAggregateRoot<$aggregate>, <$aggregate as $crate::Aggregate>::Error>
                    {
                        let event = self.entity().[<$ad_cmd _event>]($($ad_param),*);
                        $crate::ActorDeleteEvent::validate_delete_actor(&event, self.entity(), actor.entity())?;
                        self.apply_delete_with_actor(event, actor.entity_id())
                    }
                )*
            }
        }
    };
}

// =========================================================================
// Arity-dispatch helper macros for @validate and @validate_spec
//
// `define_events!` generates one validation hook per *context* (validate /
// validate_actor / validate_init / validate_init_actor / post_validate). For
// each event variant the macro emits a call to **every** context macro,
// passing the user's closure unchanged. Whichever context matches the
// closure's arity runs the body; all other contexts expand to nothing.
//
// This silent-no-op is intentional and load-bearing: a single `@validate`
// closure on an `@actor` variant should run during `validate_actor()` (3-arg
// closure) while leaving `validate()` (2-arg) inert — and we don't know at the
// dispatch site which kind of variant we're inside without further plumbing.
//
// | Context                  | Macro                      | Arity that runs |
// |--------------------------|----------------------------|-----------------|
// | regular `validate()`     | `__validate`               | 2-arg `\|agg, evt\|` |
// | actor `validate_actor()` | `__validate_actor`         | 3-arg `\|agg, actor, evt\|` |
// | init `validate_init()`   | `__validate_init`          | 1-arg `\|evt\|` |
// | actor_init               | `__validate_init_actor`    | 2-arg `\|actor, evt\|` |
// | spec in regular          | `__check_spec`             | 2-arg or bare expr |
// | spec in actor            | `__check_spec_actor`       | 3-arg only |
// | spec in init             | `__check_spec_init`        | 1-arg or bare expr |
// | spec in actor_init       | `__check_spec_init_actor`  | 2-arg only |
//
// Closures with mismatched arity silently no-op in the wrong context (so a
// shared closure dispatches correctly), but a closure that doesn't match
// **any** of its variant's contexts will never run. Today this is detectable
// only via missing test coverage — see docs/validation.md for the supported
// shapes per event kind.
//
// These macros receive context variables (aggregate, self, actor) from the
// calling macro to work around macro hygiene — `#[macro_export]` macros
// cannot directly access the caller's `self` or local variables.
// =========================================================================

/// Dispatch `@validate` closure in `validate()` context: runs 2-arg, skips 3-arg.
#[doc(hidden)]
#[macro_export]
macro_rules! __validate {
    (|$a:ident, $b:ident| $body:block, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {
        return (|$a: &$agg_ty, $b: &$evt_ty| $body)($agg, $evt);
    };
    (|$a:ident, $b:ident, $c:ident| $body:block, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {};
    ($($_:tt)*) => {
        compile_error!(
            "@validate closure must take exactly 2 args (|aggregate, event|) for regular events \
             or 3 args (|aggregate, actor, event|) for actor events"
        );
    };
}

/// Dispatch `@validate` closure in `validate_actor()` context: runs 3-arg, skips 2-arg.
#[doc(hidden)]
#[macro_export]
macro_rules! __validate_actor {
    (|$a:ident, $b:ident, $c:ident| $body:block, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {
        return (|$a: &$agg_ty, $b: &$actor_ty, $c: &$evt_ty| $body)($agg, $actor, $evt);
    };
    (|$a:ident, $b:ident| $body:block, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {};
    ($($_:tt)*) => {
        compile_error!(
            "@validate closure must take exactly 2 args (|aggregate, event|) for regular events \
             or 3 args (|aggregate, actor, event|) for actor events"
        );
    };
}

/// Dispatch `@validate` closure in `validate_init()` context: runs 1-arg, skips 2-arg.
#[doc(hidden)]
#[macro_export]
macro_rules! __validate_init {
    (|$a:ident| $body:block, $evt_ty:ty, $evt:expr) => {
        return (|$a: &$evt_ty| $body)($evt);
    };
    (|$a:ident, $b:ident| $body:block, $evt_ty:ty, $evt:expr) => {};
    ($($_:tt)*) => {
        compile_error!(
            "@validate closure must take exactly 1 arg (|event|) for init events \
             or 2 args (|actor, event|) for actor_init events"
        );
    };
}

/// Dispatch `@validate` closure in `validate_init_actor()` context: runs 2-arg, skips 1-arg.
#[doc(hidden)]
#[macro_export]
macro_rules! __validate_init_actor {
    (|$a:ident, $b:ident| $body:block, $actor_ty:ty, $evt_ty:ty, $actor:expr, $evt:expr) => {
        return (|$a: &$actor_ty, $b: &$evt_ty| $body)($actor, $evt);
    };
    (|$a:ident| $body:block, $actor_ty:ty, $evt_ty:ty, $actor:expr, $evt:expr) => {};
    ($($_:tt)*) => {
        compile_error!(
            "@validate closure must take exactly 1 arg (|event|) for init events \
             or 2 args (|actor, event|) for actor_init events"
        );
    };
}

/// Dispatch `@validate_spec` in `validate()`/`post_validate()` context:
/// runs simple exprs and 2-arg closures, skips 3-arg closures.
#[doc(hidden)]
#[macro_export]
macro_rules! __check_spec {
    (|$a:ident, $b:ident, $c:ident| $body:expr, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {};
    (|$a:ident, $b:ident| $body:expr, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {
        $crate::Specification::check(&(|$a: &$agg_ty, $b: &$evt_ty| $body)($agg, $evt), $agg)?;
    };
    (|$a:ident| $body:expr, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {};
    ($spec:expr, $agg_ty:ty, $evt_ty:ty, $agg:expr, $evt:expr) => {
        $crate::Specification::check(&$spec, $agg)?;
    }; // Bare expressions match the spec branch above; this fallback only fires
       // for malformed inputs the parser couldn't classify.
}

/// Dispatch `@validate_spec` in `validate_actor()` context:
/// runs 3-arg closures only, skips everything else.
#[doc(hidden)]
#[macro_export]
macro_rules! __check_spec_actor {
    (|$a:ident, $b:ident, $c:ident| $body:expr, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {
        $crate::Specification::check(
            &(|$a: &$agg_ty, $b: &$actor_ty, $c: &$evt_ty| $body)($agg, $actor, $evt),
            $agg,
        )?;
    };
    (|$a:ident, $b:ident| $body:expr, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {};
    (|$a:ident| $body:expr, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {};
    ($spec:expr, $agg_ty:ty, $actor_ty:ty, $evt_ty:ty, $agg:expr, $actor:expr, $evt:expr) => {};
}

/// Dispatch `@validate_spec` in `validate_init()` context:
/// runs simple exprs and 1-arg closures, skips 2-arg closures.
#[doc(hidden)]
#[macro_export]
macro_rules! __check_spec_init {
    (|$a:ident, $b:ident| $body:expr, $evt_ty:ty, $evt:expr) => {};
    (|$a:ident| $body:expr, $evt_ty:ty, $evt:expr) => {
        $crate::Specification::check(&(|$a: &$evt_ty| $body)($evt), $evt)?;
    };
    ($spec:expr, $evt_ty:ty, $evt:expr) => {
        $crate::Specification::check(&$spec, $evt)?;
    };
}

/// Dispatch `@validate_spec` in `validate_init_actor()` context:
/// runs 2-arg closures only, skips everything else.
#[doc(hidden)]
#[macro_export]
macro_rules! __check_spec_init_actor {
    (|$a:ident, $b:ident| $body:expr, $actor_ty:ty, $evt_ty:ty, $actor:expr, $evt:expr) => {
        $crate::Specification::check(
            &(|$a: &$actor_ty, $b: &$evt_ty| $body)($actor, $evt),
            $actor,
        )?;
    };
    (|$a:ident| $body:expr, $actor_ty:ty, $evt_ty:ty, $actor:expr, $evt:expr) => {};
    ($spec:expr, $actor_ty:ty, $evt_ty:ty, $actor:expr, $evt:expr) => {};
}

/// Declaratively define domain events for an aggregate.
///
/// This macro generates event structs, an event enum, and all the required trait
/// implementations (`DomainEvent`, `EventApplicator`, `ApplyEvent`/`InitEvent`,
/// `ActorEvent`/`ActorInitEvent`) from a concise, readable declaration.
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
// Internal architecture of `define_events!`:
//
// The macro runs in two phases — *parse* (TT muncher) and *emit* (per-kind
// trait emitter):
//
// 1. **Parse phase**, itself split in two steps so the hook grammar is
//    written once instead of once per kind:
//
//    - **Classify** (the `@munch` arms): six tiny arms match only the
//      variant's marker tokens — structural, not lookahead, so a marker
//      that requires a literal `@init`/`@delete`/`@actor(Ty)` token must be
//      tried before the marker-less catch-all — and forward to `@hooks`
//      with the kind tag and actor type resolved.
//
//      | Annotations          | Kind tag        |
//      |----------------------|-----------------|
//      | `@actor(T)`          | `actor`         |
//      | `@init @actor(T)`    | `actor_init`    |
//      | `@delete @actor(T)`  | `actor_delete`  |
//      | `@delete`            | `delete`        |
//      | `@init`              | `init`          |
//      | (none)               | `regular`       |
//
//    - **Parse hooks** (the `@hooks` arms): the eight optional clauses
//      (`@version`, `@occurred_at`, `@validate`, `@validate_spec`,
//      `@post_validate`, `@post_validate_spec`, `@encrypted_fields`,
//      `@aliases`) and the `=> apply` closure are parsed once, kind-agnostic,
//      and pushed into the normalised metadata blob (variant name, fields,
//      kind, optional actor type, version, validate/post_validate/spec
//      hooks, encrypted fields, apply closure). Two arms exist here, not
//      one, only because macro_rules can't default the `@occurred_at` field
//      name to `timestamp` when the clause is absent.
//
// 2. **Emit phase** (`@emit_trait` arms): once all variants are parsed,
//    each variant in the accumulator is dispatched to one of six emit arms
//    (one per kind) which produces the appropriate trait impls
//    (`ApplyEvent`/`InitEvent`/`DeleteEvent`/`ActorEvent`/`ActorInitEvent`/
//    `ActorDeleteEvent`) plus the per-variant struct. Unlike the parse
//    phase, this duplication is inherent to macro_rules! today — converting
//    to a proc-macro would let the kind-tag drive emission via runtime
//    data, but that's a larger change than this commit.
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
    // | Annotations          | Kind tag        |
    // |----------------------|-----------------|
    // | `@actor(T)`          | `actor`         |
    // | `@init @actor(T)`    | `actor_init`    |
    // | `@delete @actor(T)`  | `actor_delete`  |
    // | `@delete`            | `delete`        |
    // | `@init`              | `init`          |
    // | (none)               | `regular`       |
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
            kind: [actor]
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
            kind: [actor_init]
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
            kind: [actor_delete]
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
                    pub $clock: $crate::__private::chrono::DateTime<$crate::__private::chrono::Utc>,
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
                    $clock: $crate::__private::chrono::DateTime<$crate::__private::chrono::Utc>,
                }
            ),*
        }

        // ---- Shared: DomainEvent ----
        // Wrapped in paste::paste! so [<$variant Event>] is available for
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

                fn occurred_at(&self) -> $crate::__private::chrono::DateTime<$crate::__private::chrono::Utc> {
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
        // Wrapped in paste::paste! so [<$variant Event>] is available
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
    // Helper: Emit ApplyEvent impl for REGULAR events
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: regular,
        actor_type: [],
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
    };

    // =========================================================================
    // Helper: Emit ApplyEvent + ActorEvent impl for ACTOR events (regular + @actor)
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: actor,
        actor_type: [$actor_type:ty],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            // ApplyEvent impl (for replay — dispatches 2-arg @validate, skips 3-arg)
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

            // ActorEvent impl (for command-time — dispatches 3-arg @validate, skips 2-arg)
            impl $crate::ActorEvent<$aggregate> for [<$variant Event>] {
                type Actor = $actor_type;

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate_actor(&self, aggregate: &$aggregate, actor: &$actor_type)
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

    // =========================================================================
    // Helper: Emit InitEvent impl for @init events
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: init,
        actor_type: [],
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
    };

    // =========================================================================
    // Helper: Emit InitEvent + ActorInitEvent impl for @init @actor events
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: actor_init,
        actor_type: [$actor_type:ty],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            // InitEvent impl (for replay — dispatches 1-arg @validate, skips 2-arg)
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

            // ActorInitEvent impl (for command-time — dispatches 2-arg @validate, skips 1-arg)
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

    // =========================================================================
    // Helper: Emit DeleteEvent impl for @delete events
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: delete,
        actor_type: [],
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
    };

    // =========================================================================
    // Helper: Emit DeleteEvent + ActorDeleteEvent impl for @delete @actor events
    // =========================================================================
    (
        @emit_trait
        [$vis:vis] [$aggregate:ty] [$event_enum:ident] [$variant:ident]
        [{ $($(#[$field_attr:meta])* $field:ident: $field_ty:ty,)* }]
        kind: actor_delete,
        actor_type: [$actor_type:ty],
        validate: [$([|$($val_args:ident),+| $val_body:block])?],
        validate_spec: [$([$($val_spec:tt)*])?],
        post_validate: [$([$post_val_agg:ident, $post_val_evt:ident, $post_val_body:block])?],
        post_validate_spec: [$([$($post_val_spec:tt)*])?],
        apply: $apply:expr,
    ) => {
        $crate::__private::paste! {
            // DeleteEvent impl (for replay — dispatches 2-arg @validate, skips 3-arg)
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
                        let _ = ($($post_val_spec)*,);
                    )?
                    ::std::result::Result::Ok(())
                }
            }

            // ActorDeleteEvent impl (for command-time — dispatches 3-arg @validate, skips 2-arg)
            impl $crate::ActorDeleteEvent<$aggregate> for [<$variant Event>] {
                type Actor = $actor_type;

                #[allow(unused_variables, unreachable_code, clippy::redundant_closure_call)]
                fn validate_delete_actor(&self, aggregate: &$aggregate, actor: &$actor_type)
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

    // =========================================================================
    // Helper: dispatch arm — called from within paste::paste! so $evt_type
    // is already resolved (e.g. CreatedEvent). No inner paste needed.
    // =========================================================================
    (@validate_only_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate($aggregate_var)?;
        }
    };
    (@validate_only_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate($aggregate_var)?;
        }
    };
    (@validate_only_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        // An init fact has no aggregate to validate against yet; the post-validate
        // that guards it runs inside `dispatch_init`.
        {}
    };
    (@validate_only_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        // A delete fact never reaches `dispatch`; `dispatch_delete` runs its guard.
        {}
    };
    (@validate_only_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        // A delete fact never reaches `dispatch`; `dispatch_delete` runs its guard.
        {}
    };
    (@validate_only_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {}
    };
    (@dispatch_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate($aggregate_var)?;
            evt.apply($aggregate_var);
            evt.post_validate($aggregate_var)?;
        }
    };
    (@dispatch_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate($aggregate_var)?;
            evt.apply($aggregate_var);
            evt.post_validate($aggregate_var)?;
        }
    };
    (@dispatch_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch() called on init event; use dispatch_init()")
    };
    (@dispatch_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch() called on init event; use dispatch_init()")
    };
    (@dispatch_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch() called on delete event; use dispatch_delete()")
    };
    (@dispatch_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch() called on delete event; use dispatch_delete()")
    };

    // =========================================================================
    // Helper: dispatch_unchecked arm
    // =========================================================================
    (@dispatch_unchecked_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.apply($aggregate_var);
        }
    };
    (@dispatch_unchecked_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::ApplyEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.apply($aggregate_var);
        }
    };
    (@dispatch_unchecked_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_unchecked() called on init event; use dispatch_init_unchecked()")
    };
    (@dispatch_unchecked_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_unchecked() called on init event; use dispatch_init_unchecked()")
    };
    (@dispatch_unchecked_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_unchecked() called on delete event; use dispatch_delete_unchecked()")
    };
    (@dispatch_unchecked_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_unchecked() called on delete event; use dispatch_delete_unchecked()")
    };

    // =========================================================================
    // Helper: dispatch_init arm
    // =========================================================================
    (@dispatch_init_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate_init()?;
            let entity = evt.init($id_var);
            evt.post_validate_init(&entity)?;
            ::std::result::Result::Ok(entity)
        }
    };
    (@dispatch_init_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init() called on non-init event")
    };
    (@dispatch_init_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init() called on non-init event")
    };
    (@dispatch_init_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate_init()?;
            let entity = evt.init($id_var);
            evt.post_validate_init(&entity)?;
            ::std::result::Result::Ok(entity)
        }
    };
    (@dispatch_init_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init() called on delete event; use dispatch_delete()")
    };
    (@dispatch_init_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init() called on delete event; use dispatch_delete()")
    };

    // =========================================================================
    // Helper: dispatch_init_unchecked arm
    // =========================================================================
    (@dispatch_init_unchecked_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.init($id_var)
        }
    };
    (@dispatch_init_unchecked_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init_unchecked() called on non-init event")
    };
    (@dispatch_init_unchecked_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init_unchecked() called on non-init event")
    };
    (@dispatch_init_unchecked_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        {
            use $crate::InitEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.init($id_var)
        }
    };
    (@dispatch_init_unchecked_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init_unchecked() called on delete event; use dispatch_delete_unchecked()")
    };
    (@dispatch_init_unchecked_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$id_var:ident]) => {
        unreachable!("dispatch_init_unchecked() called on delete event; use dispatch_delete_unchecked()")
    };

    // =========================================================================
    // Helper: is_init
    // =========================================================================
    (@is_init init) => { true };
    (@is_init actor_init) => { true };
    (@is_init regular) => { false };
    (@is_init actor) => { false };
    (@is_init delete) => { false };
    (@is_init actor_delete) => { false };

    // =========================================================================
    // Helper: is_delete
    // =========================================================================
    (@is_delete delete) => { true };
    (@is_delete actor_delete) => { true };
    (@is_delete regular) => { false };
    (@is_delete actor) => { false };
    (@is_delete init) => { false };
    (@is_delete actor_init) => { false };

    // =========================================================================
    // Helper: dispatch_delete arm
    // =========================================================================
    (@dispatch_delete_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate_delete(&$aggregate_var)?;
            let state = evt.delete($aggregate_var);
            evt.post_validate_delete(&state)?;
            ::std::result::Result::Ok(state)
        }
    };
    (@dispatch_delete_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.validate_delete(&$aggregate_var)?;
            let state = evt.delete($aggregate_var);
            evt.post_validate_delete(&state)?;
            ::std::result::Result::Ok(state)
        }
    };
    (@dispatch_delete_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete() called on non-delete event")
    };
    (@dispatch_delete_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete() called on non-delete event")
    };
    (@dispatch_delete_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete() called on non-delete event")
    };
    (@dispatch_delete_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete() called on non-delete event")
    };

    // =========================================================================
    // Helper: dispatch_delete_unchecked arm
    // =========================================================================
    (@dispatch_delete_unchecked_arm delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.delete($aggregate_var)
        }
    };
    (@dispatch_delete_unchecked_arm actor_delete [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        {
            use $crate::DeleteEvent;
            let evt = $evt_type {
                $($field: $field.clone(),)*
                $timestamp: *$timestamp,
            };
            evt.delete($aggregate_var)
        }
    };
    (@dispatch_delete_unchecked_arm regular [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete_unchecked() called on non-delete event")
    };
    (@dispatch_delete_unchecked_arm actor [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete_unchecked() called on non-delete event")
    };
    (@dispatch_delete_unchecked_arm init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete_unchecked() called on non-delete event")
    };
    (@dispatch_delete_unchecked_arm actor_init [$aggregate:ty] [$evt_type:ident] [$($field:ident),*] [$timestamp:ident] [$aggregate_var:ident]) => {
        unreachable!("dispatch_delete_unchecked() called on non-delete event")
    };

    // =========================================================================
    // Helper: extract version (default to 1)
    // =========================================================================
    (@version_from [[$version:literal]]) => { $version };
    (@version_from []) => { 1 };

    // Legacy version helper
    (@version $version:literal) => { $version };
    (@version) => { 1 };

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

/// Generates `BitAnd`, `BitOr`, and `Not` operator impls for a specification struct.
///
/// This is an internal helper macro used by [`spec!`] and `#[specification]`.
#[doc(hidden)]
#[macro_export]
macro_rules! __impl_spec_ops {
    ($name:ident, $target:ty) => {
        impl<__SpecR> ::std::ops::BitAnd<__SpecR> for $name {
            type Output = $crate::specification::Spec<$crate::specification::And<$name, __SpecR>>;

            #[inline]
            fn bitand(self, rhs: __SpecR) -> Self::Output {
                $crate::specification::Spec::__new($crate::specification::And::__new(self, rhs))
            }
        }

        impl<__SpecR> ::std::ops::BitOr<__SpecR> for $name {
            type Output = $crate::specification::Spec<$crate::specification::Or<$name, __SpecR>>;

            #[inline]
            fn bitor(self, rhs: __SpecR) -> Self::Output {
                $crate::specification::Spec::__new($crate::specification::Or::__new(self, rhs))
            }
        }

        impl ::std::ops::Not for $name {
            type Output = $crate::specification::Spec<$crate::specification::Not<$name>>;

            #[inline]
            fn not(self) -> Self::Output {
                $crate::specification::Spec::__new($crate::specification::Not::__new(self))
            }
        }
    };
}

/// Create a specification struct or inline specification value.
///
/// Named-struct forms generate `&` (AND), `|` (OR), and `!` (NOT)
/// operator impls, so specs can be composed directly:
///
/// ```ignore
/// // Operator syntax
/// let combined = (IsActive | HasFunds | IsAdmin) & GeneralSpec;
///
/// // Named struct with static message
/// spec!(IsActive for Account, "Account must be active",
///     |account| { account.status == "active" }
/// );
///
/// // Named struct with context fields
/// spec!(HasPositiveBalance for Account,
///     "Balance must be positive", balance = candidate.balance,
///     |candidate| { candidate.balance > 0 }
/// );
///
/// // Inline form (closure-based, no operator support)
/// let s = spec!("Must be positive", |val: &i64| { *val > 0 });
/// ```
#[macro_export]
macro_rules! spec {
    // Named struct form with context fields:
    // spec!(pub SpecName for Type, "message", key = expr, ..., |candidate| { body })
    (
        $vis:vis $name:ident for $target:ty,
        $msg:literal, $($ctx_key:ident = $ctx_expr:expr),+ ,
        |$candidate:ident| $body:block
    ) => {
        $vis struct $name;

        impl $crate::Specification<$target> for $name {
            fn is_satisfied_by(&self, $candidate: &$target) -> bool {
                $body
            }

            fn error_message(&self, $candidate: &$target) -> String {
                let base = $msg;
                let ctx = [
                    $(format!("{}={}", stringify!($ctx_key), $ctx_expr)),+
                ].join(", ");
                format!("{base}: {ctx}")
            }
        }

        $crate::__impl_spec_ops!($name, $target);
    };

    // Named struct form with static message:
    // spec!(pub SpecName for Type, "message", |candidate| { body })
    (
        $vis:vis $name:ident for $target:ty,
        $msg:literal,
        |$candidate:ident| $body:block
    ) => {
        $vis struct $name;

        impl $crate::Specification<$target> for $name {
            fn is_satisfied_by(&self, $candidate: &$target) -> bool {
                $body
            }

            fn error_message(&self, _candidate: &$target) -> String {
                $msg.to_string()
            }
        }

        $crate::__impl_spec_ops!($name, $target);
    };

    // Inline form:
    // spec!("message", |val: &Type| { body })
    (
        $msg:literal, |$candidate:ident : &$target:ty| $body:block
    ) => {
        $crate::specification::FnSpec::new(
            |$candidate: &$target| -> bool { $body },
            |_: &$target| -> String { $msg.to_string() },
        )
    };
}

/// Define a policy that handles events by issuing commands on other aggregates.
///
/// This macro generates a struct implementing [`Policy<S>`](crate::policy::Policy)
/// that routes events to handler closures based on event type. Each handler receives
/// a deserialized event struct and a [`PolicyContext`](crate::policy::PolicyContext)
/// for loading and committing aggregates with automatic causation tracking.
///
/// # Syntax
///
/// ```ignore
/// policy! {
///     /// Optional doc comment
///     PolicyName {
///         on EventStruct |event, ctx| {
///             // Handle the event
///         },
///         on AnotherEventStruct |event, ctx| {
///             // Handle another event
///         },
///     }
/// }
/// ```
///
/// Each `EventStruct` must implement [`EventType`](crate::EventType) (which provides
/// the event type string for filtering) and [`serde::de::DeserializeOwned`] (for
/// deserialization from the event envelope).
///
/// When using [`define_events!`](crate::define_events), each variant `Foo` generates
/// a `FooEvent` struct that implements both traits automatically.
///
/// # Generated Code
///
/// For `policy! { MyPolicy { on FooEvent |e, ctx| { ... }, on BarEvent |e, ctx| { ... } } }`:
///
/// 1. `struct MyPolicy;`
/// 2. `impl<S: EventStore + 'static> Policy<S> for MyPolicy` with:
///    - `name()` → `"MyPolicy"`
///    - `event_filter()` → `EventFilter::any_of_event_types([FooEvent::EVENT_TYPE, BarEvent::EVENT_TYPE])`
///    - `handle()` → match on `event_type`, deserialize `event_data` to the struct, call handler
///
/// # Examples
///
/// ```ignore
/// use event_sauce::policy;
///
/// policy! {
///     /// Reacts to user kicks by removing them from groups.
///     KickUserPolicy {
///         on KickedUserEvent |event, ctx| {
///             let mut group = ctx.load(event.group_id).await?;
///             group.remove_user(event.user_id, "kicked")?;
///             ctx.commit(&mut group).await?;
///         },
///     }
/// }
///
/// // Register with a runner:
/// let runner = PolicyRunner::new(store, checkpoint_store)
///     .register(Arc::new(KickUserPolicy));
/// ```
#[cfg(feature = "event-sourcing")]
#[macro_export]
macro_rules! policy {
    // Entry point: with doc comments
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident {
            $(
                on $event_struct:ty |$event:ident, $ctx:ident| $handler:block
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name;

        #[$crate::__private::async_trait]
        impl<S: $crate::EventStore + 'static> $crate::policy::Policy<S> for $name {
            fn name(&self) -> &str {
                stringify!($name)
            }

            fn event_filter(&self) -> $crate::EventFilter {
                $crate::EventFilter::any_of_event_types([
                    $(<$event_struct as $crate::EventType>::EVENT_TYPE),+
                ])
            }

            async fn handle(
                &self,
                event: &$crate::EventEnvelope,
                ctx: &$crate::policy::PolicyContext<S>,
            ) -> $crate::Result<()> {
                $(
                    if event.event_type == <$event_struct as $crate::EventType>::EVENT_TYPE {
                        let $event: $event_struct = $crate::__private::serde_json::from_value(event.event_data.clone())?;
                        let $ctx = ctx;
                        return $handler;
                    }
                )+

                // No matching handler — should not happen due to event_filter
                Ok(())
            }
        }
    };
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
        Aggregate, AggregateError, AggregateRoot, AggregateVersion, ApplyEvent, DomainEvent,
        Entity, EntityId, Specification, SpecificationError,
    };
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    // Test Events
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct IncrementedEvent {
        amount: i32,
        timestamp: DateTime<Utc>,
    }

    impl crate::EventType for IncrementedEvent {
        const EVENT_TYPE: &'static str = "Test.Incremented";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DecrementedEvent {
        amount: i32,
        timestamp: DateTime<Utc>,
    }

    impl crate::EventType for DecrementedEvent {
        const EVENT_TYPE: &'static str = "Test.Decremented";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ResetEvent {
        timestamp: DateTime<Utc>,
    }

    impl crate::EventType for ResetEvent {
        const EVENT_TYPE: &'static str = "Test.Reset";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestEvent {
        Incremented(IncrementedEvent),
        Decremented(DecrementedEvent),
        Reset(ResetEvent),
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Incremented(_) => "Test.Incremented",
                TestEvent::Decremented(_) => "Test.Decremented",
                TestEvent::Reset(_) => "Test.Reset",
            }
        }

        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            match self {
                TestEvent::Incremented(e) => e.timestamp,
                TestEvent::Decremented(e) => e.timestamp,
                TestEvent::Reset(e) => e.timestamp,
            }
        }
    }

    // Into implementations for events
    impl From<IncrementedEvent> for TestEvent {
        fn from(e: IncrementedEvent) -> Self {
            TestEvent::Incremented(e)
        }
    }

    impl From<DecrementedEvent> for TestEvent {
        fn from(e: DecrementedEvent) -> Self {
            TestEvent::Decremented(e)
        }
    }

    impl From<ResetEvent> for TestEvent {
        fn from(e: ResetEvent) -> Self {
            TestEvent::Reset(e)
        }
    }

    // Test Error
    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("Invalid amount: {0}")]
        InvalidAmount(i32),
    }

    impl AggregateError for TestError {}

    // Test Aggregate (entity with embedded state)
    #[derive(Debug, Serialize, Deserialize)]
    struct TestAggregate {
        id: EntityId,
        value: i32,
    }

    impl Entity for TestAggregate {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for TestAggregate {}

    impl crate::EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestError> {
            match self {
                TestEvent::Incremented(e) => {
                    e.validate(aggregate)?;
                    e.apply(aggregate);
                    e.post_validate(aggregate)?;
                }
                TestEvent::Decremented(e) => {
                    e.validate(aggregate)?;
                    e.apply(aggregate);
                    e.post_validate(aggregate)?;
                }
                TestEvent::Reset(e) => {
                    e.validate(aggregate)?;
                    e.apply(aggregate);
                    e.post_validate(aggregate)?;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Incremented(e) => {
                    e.apply(aggregate);
                }
                TestEvent::Decremented(e) => {
                    e.apply(aggregate);
                }
                TestEvent::Reset(e) => {
                    e.apply(aggregate);
                }
            }
        }
    }

    impl Aggregate for TestAggregate {
        type Event = TestEvent;
        type Error = TestError;
        type DeletedState = Self;
    }

    impl TestAggregate {
        fn value(&self) -> i32 {
            self.value
        }
    }

    // ApplyEvent implementations
    impl ApplyEvent<TestAggregate> for IncrementedEvent {
        fn validate(&self, _aggregate: &TestAggregate) -> Result<(), TestError> {
            if self.amount <= 0 {
                return Err(TestError::InvalidAmount(self.amount));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.value += self.amount;
        }
    }

    impl ApplyEvent<TestAggregate> for DecrementedEvent {
        fn validate(&self, _aggregate: &TestAggregate) -> Result<(), TestError> {
            if self.amount <= 0 {
                return Err(TestError::InvalidAmount(self.amount));
            }
            Ok(())
        }

        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.value -= self.amount;
        }
    }

    impl ApplyEvent<TestAggregate> for ResetEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            aggregate.value = 0;
        }
    }

    // This is the test for the command_handler! macro
    command_handler! {
        impl TestAggregate {
            @clock fn increment(amount: i32) -> IncrementedEvent { amount };
            @clock fn decrement(amount: i32) -> DecrementedEvent { amount };
            @clock fn reset() -> ResetEvent { };
        }
    }

    #[test]
    fn test_command_handler_increment() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Test that generated command method works through AggregateRoot
        let result = root.increment(5);

        assert!(result.is_ok());
        assert_eq!(root.value(), 5);
        assert_eq!(root.pending_events().len(), 1);
        assert_eq!(root.version(), AggregateVersion::from(1));
    }

    #[test]
    fn test_command_handler_decrement() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
        root.increment(10).unwrap();

        // Test that generated decrement method works
        let result = root.decrement(3);

        assert!(result.is_ok());
        assert_eq!(root.value(), 7);
        assert_eq!(root.pending_events().len(), 2);
    }

    #[test]
    fn test_command_handler_reset() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
        root.increment(10).unwrap();

        // Test that generated reset method works
        let result = root.reset();

        assert!(result.is_ok());
        assert_eq!(root.value(), 0);
        assert_eq!(root.pending_events().len(), 2);
    }

    #[test]
    fn test_command_handler_validation() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Test that validation is applied through the generated command
        let result = root.increment(0);

        assert!(result.is_err());
        assert_eq!(root.value(), 0); // Value should be unchanged
        assert_eq!(root.pending_events().len(), 0); // No event added
    }

    #[test]
    fn test_command_handler_timestamp() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());
        let before = Utc::now();

        root.increment(5).unwrap();

        let after = Utc::now();

        // Check that the event has a timestamp
        let event = &root.pending_events()[0];
        let event_time = event.occurred_at();

        assert!(event_time >= before);
        assert!(event_time <= after);
    }

    #[test]
    fn test_command_handler_multiple_commands() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Test multiple commands in sequence
        root.increment(5).unwrap();
        root.increment(3).unwrap();
        root.decrement(2).unwrap();
        root.reset().unwrap();
        root.increment(10).unwrap();

        assert_eq!(root.value(), 10);
        assert_eq!(root.pending_events().len(), 5);
        assert_eq!(root.version(), AggregateVersion::from(5));
    }

    #[test]
    fn test_command_handler_return_type() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Verify the return type is Result<(), TestError>
        let result: Result<(), TestError> = root.increment(5);

        assert!(result.is_ok());
    }

    #[test]
    fn test_command_handler_event_helper() {
        let root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Event helpers are available on the entity via Deref
        let event = root.increment_event(42);
        assert_eq!(event.amount, 42);
    }

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

    // ===== command_handler! Event Helper Function Tests =====

    #[test]
    fn test_command_handler_generates_event_helper_functions() {
        // Test that <command>_event helper functions are generated
        let aggregate = TestAggregate::new(EntityId::new());

        // These should create events without applying them
        let event = aggregate.increment_event(5);
        assert_eq!(event.amount, 5);

        let event = aggregate.decrement_event(3);
        assert_eq!(event.amount, 3);

        let event = aggregate.reset_event();
        // Reset has no fields besides timestamp
        let _ = event.timestamp;
    }

    #[test]
    fn test_event_helper_creates_event_without_applying() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Create event using helper - should NOT apply it
        let event = root.increment_event(10);

        // Aggregate state should be unchanged
        assert_eq!(root.value(), 0);
        assert_eq!(root.pending_events().len(), 0);

        // Now apply the event through AggregateRoot
        root.apply(event).unwrap();

        // Now state should be updated
        assert_eq!(root.value(), 10);
        assert_eq!(root.pending_events().len(), 1);
    }

    #[test]
    fn test_event_helper_adds_timestamp() {
        let aggregate = TestAggregate::new(EntityId::new());
        let before = Utc::now();

        let event = aggregate.increment_event(5);

        let after = Utc::now();

        // Check that the event has a timestamp
        assert!(event.timestamp >= before);
        assert!(event.timestamp <= after);
    }

    #[test]
    fn test_event_helper_with_multiple_parameters() {
        // Define a more complex event structure for testing
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct MultiParamEvent {
            field1: String,
            field2: i32,
            field3: bool,
            timestamp: DateTime<Utc>,
        }

        impl ApplyEvent<TestAggregate> for MultiParamEvent {
            fn apply(&self, _aggregate: &mut TestAggregate) {
                // No-op for test
            }
        }

        impl From<MultiParamEvent> for TestEvent {
            fn from(e: MultiParamEvent) -> Self {
                // Mock conversion
                TestEvent::Reset(ResetEvent {
                    timestamp: e.timestamp,
                })
            }
        }

        // We can't actually test this with TestAggregate since we'd need to modify
        // the command_handler! invocation, but this documents expected behavior
    }

    #[test]
    fn test_command_method_uses_event_helper() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // The command method should internally use the event helper
        // We verify this by checking that the behavior is consistent
        let before = Utc::now();
        root.apply(root.increment_event(7)).unwrap();
        let after = Utc::now();

        assert_eq!(root.value(), 7);
        let event = &root.pending_events()[0];
        let event_time = event.occurred_at();
        assert!(event_time >= before);
        assert!(event_time <= after);
    }

    #[test]
    fn test_event_helper_can_be_used_for_conditional_application() {
        let mut root = AggregateRoot::<TestAggregate>::new(EntityId::new());

        // Create multiple events using helpers
        let event1 = root.increment_event(5);
        let _event2 = root.increment_event(10);
        let event3 = root.increment_event(15);

        // Conditionally apply only some events
        root.apply(event1).unwrap();
        // Skip _event2
        root.apply(event3).unwrap();

        // Should only have applied event1 and event3
        assert_eq!(root.value(), 20); // 5 + 15
        assert_eq!(root.pending_events().len(), 2);
    }

    // ===== spec! Macro Tests =====

    // Test type for spec! tests
    #[derive(Debug)]
    struct BankAccount {
        balance: i64,
        status: &'static str,
    }

    // Named struct form with static message
    spec!(IsAccountActive for BankAccount, "Account must be active",
        |account| { account.status == "active" }
    );

    // Named struct form with context fields
    spec!(HasSufficientBalance for BankAccount,
        "Insufficient balance", balance = candidate.balance,
        |candidate| { candidate.balance > 0 }
    );

    #[test]
    fn test_spec_macro_named_static_message_satisfied() {
        let account = BankAccount {
            balance: 100,
            status: "active",
        };
        assert!(IsAccountActive.is_satisfied_by(&account));
    }

    #[test]
    fn test_spec_macro_named_static_message_unsatisfied() {
        let account = BankAccount {
            balance: 100,
            status: "frozen",
        };
        assert!(!IsAccountActive.is_satisfied_by(&account));
    }

    #[test]
    fn test_spec_macro_named_static_error_message() {
        let account = BankAccount {
            balance: 100,
            status: "frozen",
        };
        assert_eq!(
            IsAccountActive.error_message(&account),
            "Account must be active"
        );
    }

    #[test]
    fn test_spec_macro_named_static_check() {
        let active = BankAccount {
            balance: 100,
            status: "active",
        };
        let frozen = BankAccount {
            balance: 100,
            status: "frozen",
        };
        assert!(IsAccountActive.check(&active).is_ok());
        assert!(IsAccountActive.check(&frozen).is_err());
    }

    #[test]
    fn test_spec_macro_named_context_satisfied() {
        let account = BankAccount {
            balance: 100,
            status: "active",
        };
        assert!(HasSufficientBalance.is_satisfied_by(&account));
    }

    #[test]
    fn test_spec_macro_named_context_unsatisfied() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        assert!(!HasSufficientBalance.is_satisfied_by(&account));
    }

    #[test]
    fn test_spec_macro_named_context_error_message() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        assert_eq!(
            HasSufficientBalance.error_message(&account),
            "Insufficient balance: balance=-50"
        );
    }

    #[test]
    fn test_spec_macro_named_context_check() {
        let positive = BankAccount {
            balance: 100,
            status: "active",
        };
        let negative = BankAccount {
            balance: -50,
            status: "active",
        };
        assert!(HasSufficientBalance.check(&positive).is_ok());
        let err = HasSufficientBalance.check(&negative).unwrap_err();
        assert_eq!(err.message, "Insufficient balance: balance=-50");
    }

    #[test]
    fn test_spec_macro_inline_form_satisfied() {
        let s = spec!("Must be positive", |val: &i64| { *val > 0 });
        assert!(s.is_satisfied_by(&42));
    }

    #[test]
    fn test_spec_macro_inline_form_unsatisfied() {
        let s = spec!("Must be positive", |val: &i64| { *val > 0 });
        assert!(!s.is_satisfied_by(&-1));
    }

    #[test]
    fn test_spec_macro_inline_form_error_message() {
        let s = spec!("Must be positive", |val: &i64| { *val > 0 });
        assert_eq!(s.error_message(&-1), "Must be positive");
    }

    #[test]
    fn test_spec_macro_inline_form_check() {
        let s = spec!("Must be positive", |val: &i64| { *val > 0 });
        assert!(s.check(&42).is_ok());
        assert!(s.check(&-1).is_err());
    }

    #[test]
    fn test_spec_macro_named_composable_with_and() {
        let account = BankAccount {
            balance: 100,
            status: "active",
        };
        let spec = IsAccountActive.and(HasSufficientBalance);
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_named_composable_with_and_fails() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        let spec = IsAccountActive.and(HasSufficientBalance);
        assert!(spec.check(&account).is_err());
    }

    #[test]
    fn test_spec_macro_named_composable_with_or() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        let spec = IsAccountActive.or(HasSufficientBalance);
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_named_composable_with_not() {
        let frozen = BankAccount {
            balance: 100,
            status: "frozen",
        };
        let spec = IsAccountActive.not();
        assert!(spec.check(&frozen).is_ok());
    }

    #[test]
    fn test_spec_macro_inline_composable() {
        let positive = spec!("Must be positive", |val: &i64| { *val > 0 });
        let even = spec!("Must be even", |val: &i64| { *val % 2 == 0 });
        let spec = positive.and(even);
        assert!(spec.check(&42).is_ok());
        assert!(spec.check(&3).is_err());
    }

    // Test multiple context fields
    spec!(HasMinMaxBalance for BankAccount,
        "Balance out of range", balance = candidate.balance, status = candidate.status,
        |candidate| { candidate.balance > 0 && candidate.balance < 1_000_000 }
    );

    #[test]
    fn test_spec_macro_multiple_context_fields() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        assert_eq!(
            HasMinMaxBalance.error_message(&account),
            "Balance out of range: balance=-50, status=active"
        );
    }

    #[test]
    fn test_spec_macro_multiple_context_satisfied() {
        let account = BankAccount {
            balance: 500,
            status: "active",
        };
        assert!(HasMinMaxBalance.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_multiple_context_unsatisfied() {
        let account = BankAccount {
            balance: 2_000_000,
            status: "active",
        };
        assert!(HasMinMaxBalance.check(&account).is_err());
    }

    // Test private visibility (default)
    spec!(PrivateSpec for i64, "must pass",
        |val| { *val > 0 }
    );

    #[test]
    fn test_spec_macro_private_visibility() {
        assert!(PrivateSpec.is_satisfied_by(&1));
    }

    // Test with validate_or
    #[test]
    fn test_spec_macro_with_validate_or() {
        let account = BankAccount {
            balance: -50,
            status: "frozen",
        };

        let result: Result<(), String> = IsAccountActive.validate_or(&account, |msg| msg);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Account must be active");
    }

    // ===== spec! Operator Tests =====

    #[test]
    fn test_spec_macro_bitand_operator() {
        let account = BankAccount {
            balance: 100,
            status: "active",
        };
        let spec = IsAccountActive & HasSufficientBalance;
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_bitor_operator() {
        let account = BankAccount {
            balance: -50,
            status: "active",
        };
        let spec = IsAccountActive | HasSufficientBalance;
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_not_operator() {
        let account = BankAccount {
            balance: 100,
            status: "frozen",
        };
        let spec = !IsAccountActive;
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_operator_chaining() {
        let account = BankAccount {
            balance: 100,
            status: "active",
        };
        // (active | has balance) & in range
        let spec = (IsAccountActive | HasSufficientBalance) & HasMinMaxBalance;
        assert!(spec.check(&account).is_ok());
    }

    #[test]
    fn test_spec_macro_operator_triple_or() {
        let account = BankAccount {
            balance: -50,
            status: "frozen",
        };
        // All three fail → combined fails
        let spec = IsAccountActive | HasSufficientBalance | HasMinMaxBalance;
        assert!(spec.check(&account).is_err());
    }

    #[test]
    fn test_spec_macro_operator_not_and_chain() {
        let account = BankAccount {
            balance: 100,
            status: "frozen",
        };
        // !active & has balance
        let spec = !IsAccountActive & HasSufficientBalance;
        assert!(spec.check(&account).is_ok());
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
            use crate::macros::tests::delete_tests::AccountDeleteCommands;
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let deleted = agg.deactivate_account("goodbye".to_string()).unwrap();
            assert!(!deleted.active);
        }

        #[test]
        fn test_delete_command_validation_rejects() {
            use crate::macros::tests::delete_tests::AccountDeleteCommands;
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
            use crate::macros::tests::delete_tests::AccountDeleteCommands;
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
            use crate::macros::tests::delete_tests::AccountDeleteCommands;
            let agg = Account::create_account("Alice".to_string()).unwrap();
            let non_admin = make_admin("regular");
            let result = agg.admin_delete_account(&non_admin, "policy".to_string());
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), "Not authorized");
        }

        #[test]
        fn test_delete_command_full_lifecycle() {
            use crate::macros::tests::delete_tests::AccountDeleteCommands;
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
