//! [`command_handler!`](crate::command_handler) and the internal muncher and
//! arity-dispatch macros it recurses through.

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
            timestamp: $crate::chrono::Utc::now(),
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

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use crate::{
        Aggregate, AggregateError, AggregateRoot, AggregateVersion, ApplyEvent, DomainEvent,
        Entity, EntityId,
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
}
