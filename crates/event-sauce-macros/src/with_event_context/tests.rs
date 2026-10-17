use super::expand;
use proc_macro2::TokenStream;
use quote::quote;

fn expanded(args: &TokenStream, item: TokenStream) -> String {
    expand(&quote!(::event_sauce), args, item).to_string()
}

fn squeezed(code: &str) -> String {
    code.split_whitespace().collect()
}

#[test]
fn sync_fn_runs_its_body_in_scope_with_the_declared_return_type() {
    let out = expanded(
        &quote!(root, tenant = "acme"),
        quote!(
            fn f(x: u8) -> u8 {
                x
            }
        ),
    );
    assert_eq!(
        squeezed(&out),
        squeezed(
            "fn f(x: u8) -> u8 { \
             let __event_context_ret: u8 = \
             ::event_sauce::event_context!(root, tenant = \"acme\").in_scope(|| { x }); \
             __event_context_ret }"
        )
    );
}

#[test]
fn sync_fn_without_return_type_is_annotated_with_unit() {
    let out = expanded(
        &quote!(),
        quote!(
            fn f() {}
        ),
    );
    assert!(out.contains("__event_context_ret : ()"), "{out}");
    assert!(out.contains("event_context ! ()"), "{out}");
}

#[test]
fn opaque_return_type_is_left_unannotated() {
    let out = expanded(
        &quote!(),
        quote!(
            fn f() -> impl Sized {
                1
            }
        ),
    );
    assert!(!out.contains("__event_context_ret :"), "{out}");
    assert!(out.contains("in_scope"), "{out}");
}

#[test]
fn async_fn_awaits_its_body_wrapped_in_the_context() {
    let out = expanded(
        &quote!(correlation_id = "c"),
        quote!(
            async fn f(x: &str) -> usize {
                x.len()
            }
        ),
    );
    assert_eq!(
        squeezed(&out),
        squeezed(
            "async fn f(x: &str) -> usize { \
             let __event_context_ret: usize = \
             ::event_sauce::WithEventContextExt::with_event_context(async { x.len() }, ::event_sauce::event_context!(correlation_id = \"c\")).await; \
             __event_context_ret }"
        )
    );
}

#[test]
fn async_trait_expansion_wraps_the_boxed_future() {
    let out = expanded(
        &quote!(correlation_id = "c"),
        quote!(
            fn f<'a>(&'a self) -> Pin<Box<dyn Future<Output = u8> + Send + 'a>> {
                let __self = self;
                Box::pin(async move { 1 })
            }
        ),
    );
    assert_eq!(
        squeezed(&out),
        squeezed(
            "fn f<'a>(&'a self) -> Pin<Box<dyn Future<Output = u8> + Send + 'a>> { \
             let __self = self; \
             let __event_context = ::event_sauce::event_context!(correlation_id = \"c\"); \
             Box::pin(::event_sauce::WithEventContextExt::with_event_context(async move { 1 }, __event_context)) }"
        )
    );
}

#[test]
fn pin_box_without_a_boxed_async_tail_is_a_plain_sync_fn() {
    let out = expanded(
        &quote!(),
        quote!(
            fn f() -> Pin<Box<dyn Future<Output = u8>>> {
                make()
            }
        ),
    );
    assert!(out.contains("in_scope"), "{out}");
}

#[test]
fn non_function_input_is_a_compile_error() {
    let out = expanded(
        &quote!(),
        quote!(
            struct S;
        ),
    );
    assert!(out.contains("compile_error"), "{out}");
}
