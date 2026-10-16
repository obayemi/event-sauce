//! Expansion of `#[with_event_context]`.

use proc_macro2::{TokenStream, TokenTree};
use quote::quote;
use syn::{parse_quote, Block, Expr, ExprCall, ExprPath, ItemFn, ReturnType, Stmt, Type};

/// Rewrites the function so its body runs inside the event context built by
/// `event_context!(args)`.
///
/// A sync function becomes `ctx.in_scope(|| body)`. An `async fn` awaits its
/// body wrapped with `with_event_context`, so the context follows the future
/// across awaits and threads. A method already expanded by `#[async_trait]`
/// (a sync fn whose tail is `Box::pin(async ..)`) gets its inner async block
/// wrapped the same way.
pub(crate) fn expand(core: &TokenStream, args: &TokenStream, item: TokenStream) -> TokenStream {
    let mut function = match syn::parse2::<ItemFn>(item) {
        Ok(function) => function,
        Err(error) => return error.to_compile_error(),
    };
    let context = quote!(#core::event_context!(#args));
    let block = &function.block;
    let body = if function.sig.asyncness.is_some() {
        let wrapped = quote!(#core::WithEventContextExt::with_event_context(async #block, __event_context).await);
        annotated(&function.sig.output, &wrapped, Some(&context))
    } else if let Some(body) = boxed_async_body(block, core, &context) {
        body
    } else {
        annotated(
            &function.sig.output,
            &quote!(#context.in_scope(|| #block)),
            None,
        )
    };
    *function.block = body;
    quote!(#function)
}

fn annotated(output: &ReturnType, value: &TokenStream, binding: Option<&TokenStream>) -> Block {
    let binding = binding.map(|context| quote!(let __event_context = #context;));
    let result = match output {
        ReturnType::Default => Some(quote!(())),
        ReturnType::Type(_, ty) if !has_opaque_type(ty) => Some(quote!(#ty)),
        ReturnType::Type(..) => None,
    };
    match result {
        Some(ty) => parse_quote!({
            #binding
            let __event_context_ret: #ty = #value;
            __event_context_ret
        }),
        None => parse_quote!({
            #binding
            #value
        }),
    }
}

fn has_opaque_type(ty: &Type) -> bool {
    quote!(#ty)
        .into_iter()
        .any(|token| matches!(token, TokenTree::Ident(ident) if ident == "impl"))
}

fn boxed_async_body(block: &Block, core: &TokenStream, context: &TokenStream) -> Option<Block> {
    let (Stmt::Expr(Expr::Call(call), None), leading) = block.stmts.split_last()? else {
        return None;
    };
    let ExprCall { func, args, .. } = call;
    let Expr::Path(ExprPath { path, .. }) = func.as_ref() else {
        return None;
    };
    let segments: Vec<_> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    let [.., boxed, pin] = segments.as_slice() else {
        return None;
    };
    let (true, Some(Expr::Async(block)), 1) =
        (boxed == "Box" && pin == "pin", args.first(), args.len())
    else {
        return None;
    };
    Some(parse_quote!({
        #(#leading)*
        let __event_context = #context;
        #func(#core::WithEventContextExt::with_event_context(#block, __event_context))
    }))
}

#[cfg(test)]
mod tests;
