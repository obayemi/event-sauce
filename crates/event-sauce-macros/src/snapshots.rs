//! Parsing and expansion of `#[aggregate(snapshots(...))]`.

use darling::FromMeta;
use quote::quote;
use std::num::NonZeroU32;

/// The snapshot strategy an aggregate declares through `snapshots(...)`.
#[derive(Debug)]
pub(crate) enum SnapshotDecl {
    Every(NonZeroU32),
    Always,
    Never,
    With(syn::Path),
}

impl FromMeta for SnapshotDecl {
    fn from_meta(item: &syn::Meta) -> darling::Result<Self> {
        const USAGE: &str =
            "`snapshots` takes exactly one of `every = N`, `always`, `never` or `with = \"path\"`";
        let usage = || darling::Error::custom(USAGE).with_span(item);
        let syn::Meta::List(list) = item else {
            return Err(usage());
        };
        let nested = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())?;
        let [darling::ast::NestedMeta::Meta(option)] = nested.as_slice() else {
            return Err(usage());
        };
        match option {
            syn::Meta::Path(p) if p.is_ident("always") => Ok(Self::Always),
            syn::Meta::Path(p) if p.is_ident("never") => Ok(Self::Never),
            m if m.path().is_ident("every") => NonZeroU32::new(u32::from_meta(m)?)
                .map(Self::Every)
                .ok_or_else(|| {
                    darling::Error::custom("`snapshots(every = N)` requires N greater than 0")
                        .with_span(m)
                }),
            m if m.path().is_ident("with") => syn::parse_str(&String::from_meta(m)?)
                .map(Self::With)
                .map_err(|e| darling::Error::custom(e.to_string()).with_span(m)),
            _ => Err(usage()),
        }
    }
}

impl SnapshotDecl {
    pub(crate) fn to_override(&self, core: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        let strategy = match self {
            Self::Every(n) => {
                let n = n.get();
                quote! { const { #core::EveryNEvents::new(::core::num::NonZeroU32::new(#n).unwrap()) } }
            }
            Self::Always => quote! { #core::AlwaysSnapshot },
            Self::Never => quote! { #core::NeverSnapshot },
            Self::With(path) => quote! { #path },
        };
        quote! {
            fn snapshot_strategy() -> ::core::option::Option<&'static dyn #core::SnapshotStrategy> {
                ::core::option::Option::Some(&#strategy)
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/snapshots_tests.rs"]
mod tests;
