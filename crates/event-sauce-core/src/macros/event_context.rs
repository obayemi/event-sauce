//! [`event_context!`](crate::event_context!) and the internal muncher it
//! recurses through.

/// Build an [`EventContext`](crate::EventContext), the way `tracing::span!`
/// builds a span.
///
/// # Syntax
///
/// ```text
/// event_context!()                          // EventContext::new()
/// event_context!(root, field, field, ..)    // EventContext::root()
/// event_context!(field, field, ..)          // EventContext::new()
/// ```
///
/// Fields come in any order, with an optional trailing comma:
///
/// | field                    | expands to                                   |
/// |--------------------------|----------------------------------------------|
/// | `tenant = expr`          | `.tenant(expr)`, only valid after `root`       |
/// | `correlation_id = expr`  | `.correlation_id(expr)`                        |
/// | `tenant`                 | `.tenant(tenant.clone())`, only after `root`   |
/// | `correlation_id`         | `.correlation_id(correlation_id.clone())`      |
/// | `key = expr`             | `.with("key", expr)`                           |
/// | `key = %expr`            | `.with("key", display(&expr))`                 |
/// | `"some.key" = expr`      | `.with("some.key", expr)`                      |
/// | `"some.key" = %expr`     | `.with("some.key", display(&expr))`            |
/// | `ident`                  | `.with("ident", ident.clone())`                |
/// | `%ident`                 | `.with("ident", display(&ident))`              |
///
/// # Examples
///
/// ```
/// use event_sauce_core::event_context;
///
/// let attempt = 2_u32;
/// event_context!(root, tenant = "acme", correlation_id = "req-1", region = "eu", %attempt)
///     .in_scope(|| {
///         event_context!("http.method" = "GET").in_scope(|| ());
///     });
/// ```
///
/// A tenant needs a `root` context:
///
/// ```compile_fail
/// use event_sauce_core::event_context;
///
/// let _ = event_context!(tenant = "acme");
/// ```
///
/// So does the bare `tenant` shorthand:
///
/// ```compile_fail
/// use event_sauce_core::event_context;
///
/// let tenant = "acme";
/// let _ = event_context!(tenant);
/// ```
#[macro_export]
macro_rules! event_context {
    () => {
        $crate::EventContext::new()
    };
    (root $(, $($fields:tt)*)?) => {
        $crate::event_context!(@fields root, $crate::EventContext::root(); $($($fields)*)?)
    };
    (@fields $mode:ident, $ctx:expr;) => { $ctx };

    (@fields root, $ctx:expr; tenant = $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields root, $ctx.tenant($value); $($($rest)*)?)
    };
    (@fields nested, $ctx:expr; tenant = $($rest:tt)*) => {
        compile_error!("`tenant` is only valid after `root`: use `event_context!(root, tenant = ..)`")
    };
    (@fields $mode:ident, $ctx:expr; correlation_id = $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.correlation_id($value); $($($rest)*)?)
    };

    (@fields $mode:ident, $ctx:expr; $key:ident = % $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.with(stringify!($key), $crate::display(&$value)); $($($rest)*)?)
    };
    (@fields $mode:ident, $ctx:expr; $key:literal = % $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.with($key, $crate::display(&$value)); $($($rest)*)?)
    };
    (@fields $mode:ident, $ctx:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.with(stringify!($key), $value); $($($rest)*)?)
    };
    (@fields $mode:ident, $ctx:expr; $key:literal = $value:expr $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.with($key, $value); $($($rest)*)?)
    };

    (@fields $mode:ident, $ctx:expr; % $key:ident $(, $($rest:tt)*)?) => {
        $crate::event_context!(@fields $mode, $ctx.with(stringify!($key), $crate::display(&$key)); $($($rest)*)?)
    };
    (@fields $mode:ident, $ctx:expr; $key:ident $(, $($rest:tt)*)?) => {
        $crate::event_context!(@bare $mode, $key, $key, $ctx; $($($rest)*)?)
    };

    (@bare root, tenant, $value:ident, $ctx:expr; $($rest:tt)*) => {
        $crate::event_context!(@fields root, $ctx.tenant(::core::clone::Clone::clone(&$value)); $($rest)*)
    };
    (@bare nested, tenant, $value:ident, $ctx:expr; $($rest:tt)*) => {
        compile_error!("`tenant` is only valid after `root`: use `event_context!(root, tenant)`")
    };
    (@bare $mode:ident, correlation_id, $value:ident, $ctx:expr; $($rest:tt)*) => {
        $crate::event_context!(@fields $mode, $ctx.correlation_id(::core::clone::Clone::clone(&$value)); $($rest)*)
    };
    (@bare $mode:ident, $key:ident, $value:ident, $ctx:expr; $($rest:tt)*) => {
        $crate::event_context!(@fields $mode, $ctx.with(stringify!($key), ::core::clone::Clone::clone(&$value)); $($rest)*)
    };

    ($($fields:tt)+) => {
        $crate::event_context!(@fields nested, $crate::EventContext::new(); $($fields)+)
    };
}

#[cfg(test)]
mod tests;
