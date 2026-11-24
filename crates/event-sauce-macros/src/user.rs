//! Parsing and expansion of `#[aggregate(user)]` and its `#[user(...)]` field markers.

use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{
    token::Comma, Data, DeriveInput, Field, Fields, GenericArgument, Ident, Meta, PathArguments,
    Type,
};

const MARKER_USAGE: &str = "`#[user(...)]` takes exactly one of `identity` or `active`";

/// The `#[user(...)]` marker a field carries.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Marker {
    Identity,
    Active,
}

impl Marker {
    fn name(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Active => "active",
        }
    }
}

/// The `UserAggregate` impl and the `claims()` override an aggregate declares
/// through `user` and `claims`.
///
/// Without `user`, stray field markers are refused and the override only
/// forwards to the aggregate's own claims, when `claims` is declared.
pub(crate) fn expand(
    declared: bool,
    own_claims: bool,
    aggregate: &Ident,
    fields: &Punctuated<Field, Comma>,
    core: &TokenStream,
) -> syn::Result<(TokenStream, TokenStream)> {
    if declared {
        return UserFields::from_fields(aggregate, fields)?.expand(aggregate, core, own_claims);
    }
    reject_markers(fields)?;
    let claims_override = own_claims.then(|| {
        quote! {
            fn claims(&self) -> Vec<#core::AggregateClaim> {
                self.aggregate_claims()
            }
        }
    });
    Ok((quote! {}, claims_override.unwrap_or_default()))
}

fn reject_markers(fields: &Punctuated<Field, Comma>) -> syn::Result<()> {
    match fields.iter().find(|field| has_marker(field)) {
        Some(field) => Err(syn::Error::new_spanned(
            field,
            "`#[user(...)]` markers require the `user` flag in `#[aggregate(...)]`",
        )),
        None => Ok(()),
    }
}

/// The fields of an aggregate that make it a user.
#[derive(Debug)]
struct UserFields {
    identity: Ident,
    credentials: Type,
    active: Option<Ident>,
}

impl UserFields {
    fn from_fields(aggregate: &Ident, fields: &Punctuated<Field, Comma>) -> syn::Result<Self> {
        let mut identity: Option<&Field> = None;
        let mut active: Option<&Field> = None;
        for field in fields {
            let Some(marker) = marker_of(field)? else {
                continue;
            };
            let slot = match marker {
                Marker::Identity => &mut identity,
                Marker::Active => &mut active,
            };
            if slot.replace(field).is_some() {
                return Err(syn::Error::new_spanned(
                    field,
                    format!("only one field may be marked `#[user({})]`", marker.name()),
                ));
            }
        }
        let identity = identity.ok_or_else(|| {
            syn::Error::new_spanned(
                aggregate,
                format!("`user` on `{aggregate}` requires a field marked `#[user(identity)]`"),
            )
        })?;
        Ok(Self {
            credentials: credentials_of(&identity.ty)?,
            identity: field_ident(identity),
            active: active.map(field_ident),
        })
    }

    fn expand(
        &self,
        aggregate: &Ident,
        core: &TokenStream,
        own_claims: bool,
    ) -> syn::Result<(TokenStream, TokenStream)> {
        let auth = auth_path().ok_or_else(|| {
            syn::Error::new_spanned(
                aggregate,
                "`user` needs the `event-sauce-auth` crate, or `event-sauce` with its `auth` feature, as a dependency",
            )
        })?;
        Ok((
            self.to_impl(aggregate, &auth),
            self.claims_override(core, &auth, own_claims),
        ))
    }

    fn to_impl(&self, aggregate: &Ident, auth: &TokenStream) -> TokenStream {
        let Self {
            identity,
            credentials,
            active,
        } = self;
        let is_active = active
            .as_ref()
            .map(|field| quote! { fn is_active(&self) -> bool { self.#field } });
        quote! {
            impl #auth::UserAggregate for #aggregate {
                type Credentials = #credentials;

                fn identity(&self) -> &#auth::Identity<Self::Credentials> {
                    &self.#identity
                }

                #is_active
            }
        }
    }

    fn claims_override(
        &self,
        core: &TokenStream,
        auth: &TokenStream,
        own_claims: bool,
    ) -> TokenStream {
        let identity = &self.identity;
        let seed = if own_claims {
            quote! { self.aggregate_claims() }
        } else {
            quote! { Vec::new() }
        };
        quote! {
            fn claims(&self) -> Vec<#core::AggregateClaim> {
                let mut claims = #seed;
                if let ::core::option::Option::Some(credentials) = self.#identity.credentials() {
                    claims.extend(#auth::Credentials::claims(credentials));
                }
                claims
            }
        }
    }
}

/// Removes the `#[user(...)]` markers, which only the macro understands.
pub(crate) fn strip_user_markers(input: &mut DeriveInput) {
    if let Data::Struct(data) = &mut input.data {
        if let Fields::Named(named) = &mut data.fields {
            for field in &mut named.named {
                field.attrs.retain(|attr| !attr.path().is_ident("user"));
            }
        }
    }
}

fn has_marker(field: &Field) -> bool {
    field.attrs.iter().any(|attr| attr.path().is_ident("user"))
}

fn field_ident(field: &Field) -> Ident {
    field
        .ident
        .clone()
        .expect("named fields were checked by the caller")
}

fn marker_of(field: &Field) -> syn::Result<Option<Marker>> {
    let mut markers = Vec::new();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("user")) {
        let usage = || syn::Error::new_spanned(attr, MARKER_USAGE);
        let Meta::List(list) = &attr.meta else {
            return Err(usage());
        };
        list.parse_nested_meta(|meta| {
            let marker = [Marker::Identity, Marker::Active]
                .into_iter()
                .find(|m| meta.path.is_ident(m.name()))
                .ok_or_else(|| meta.error(MARKER_USAGE))?;
            markers.push(marker);
            Ok(())
        })
        .map_err(|_| usage())?;
        if markers.is_empty() {
            return Err(usage());
        }
    }
    match markers.as_slice() {
        [] => Ok(None),
        [marker] => Ok(Some(*marker)),
        _ => Err(syn::Error::new_spanned(
            field,
            "a field takes exactly one `#[user(...)]` marker",
        )),
    }
}

fn credentials_of(identity_type: &Type) -> syn::Result<Type> {
    let not_identity = || {
        syn::Error::new_spanned(
            identity_type,
            "a `#[user(identity)]` field must have the type `Identity<C>`",
        )
    };
    let Type::Path(path) = identity_type else {
        return Err(not_identity());
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(not_identity());
    };
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(not_identity());
    };
    match args.args.first() {
        Some(GenericArgument::Type(credentials)) if args.args.len() == 1 => Ok(credentials.clone()),
        _ => Err(not_identity()),
    }
}

fn auth_path() -> Option<TokenStream> {
    auth_path_from(crate::imported_crate)
}

/// Path to the auth crate, given a lookup from package name to the identifier
/// the dependency is imported under: directly when the application depends on
/// `event-sauce-auth`, through the facade re-export otherwise.
fn auth_path_from(lookup: impl Fn(&str) -> Option<String>) -> Option<TokenStream> {
    let ident = |name: &str| Ident::new(name, proc_macro2::Span::call_site());
    lookup("event-sauce-auth")
        .map(|name| {
            let name = ident(&name);
            quote!(::#name)
        })
        .or_else(|| {
            lookup("event-sauce").map(|name| {
                let name = ident(&name);
                quote!(::#name::auth)
            })
        })
}

#[cfg(test)]
#[path = "tests/user_tests.rs"]
mod tests;
