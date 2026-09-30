//! Arity-dispatch helper macros `define_events!` recurses through to run a
//! shared `@validate`/`@validate_spec` closure in the right context.
//!
//! `define_events!` generates one validation hook per *context* (`validate` /
//! `validate_actor` / `validate_init` / `validate_init_actor` /
//! `post_validate`). For each event variant the macro emits a call to
//! **every** context macro, passing the user's closure unchanged. Whichever
//! context matches the closure's arity runs the body; all other contexts
//! expand to nothing.
//!
//! This silent-no-op is intentional and load-bearing: a single `@validate`
//! closure on an `@actor` variant should run during `validate_actor()` (3-arg
//! closure) while leaving `validate()` (2-arg) inert — and we don't know at
//! the dispatch site which kind of variant we're inside without further
//! plumbing.
//!
//! | Context                  | Macro                      | Arity that runs |
//! |---------------------------|----------------------------|-----------------|
//! | regular `validate()`     | `__validate`               | 2-arg `\|agg, evt\|` |
//! | actor `validate_actor()` | `__validate_actor`         | 3-arg `\|agg, actor, evt\|` |
//! | init `validate_init()`   | `__validate_init`          | 1-arg `\|evt\|` |
//! | `actor_init`             | `__validate_init_actor`    | 2-arg `\|actor, evt\|` |
//! | spec in regular          | `__check_spec`             | 2-arg or bare expr |
//! | spec in actor            | `__check_spec_actor`       | 3-arg only |
//! | spec in init             | `__check_spec_init`        | 1-arg or bare expr |
//! | spec in `actor_init`     | `__check_spec_init_actor`  | 2-arg only |
//!
//! Closures with mismatched arity silently no-op in the wrong context (so a
//! shared closure dispatches correctly), but a closure that doesn't match
//! **any** of its variant's contexts will never run. Today this is
//! detectable only via missing test coverage — see docs/validation.md for
//! the supported shapes per event kind.
//!
//! These macros receive context variables (aggregate, self, actor) from the
//! calling macro to work around macro hygiene — `#[macro_export]` macros
//! cannot directly access the caller's `self` or local variables.

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
    };
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
