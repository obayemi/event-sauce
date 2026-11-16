use super::{auth_path_from, strip_user_markers, UserFields};
use quote::quote;
use syn::{parse_quote, DeriveInput};

fn squeezed(tokens: &proc_macro2::TokenStream) -> String {
    tokens.to_string().split_whitespace().collect()
}

fn struct_input(body: &proc_macro2::TokenStream) -> DeriveInput {
    parse_quote! { struct Profile { #body } }
}

fn named(input: &DeriveInput) -> &syn::punctuated::Punctuated<syn::Field, syn::token::Comma> {
    let syn::Data::Struct(data) = &input.data else {
        unreachable!()
    };
    let syn::Fields::Named(named) = &data.fields else {
        unreachable!()
    };
    &named.named
}

fn parse(body: &proc_macro2::TokenStream) -> syn::Result<UserFields> {
    UserFields::from_fields(&parse_quote!(Profile), named(&struct_input(body)))
}

fn credentials(fields: &UserFields) -> String {
    let credentials = &fields.credentials;
    squeezed(&quote!(#credentials))
}

fn message(body: &proc_macro2::TokenStream) -> String {
    parse(body).unwrap_err().to_string()
}

#[test]
fn identity_alone_is_enough_and_always_active() {
    let fields = parse(&quote! {
        #[id] id: EntityId,
        #[user(identity)] identity: Identity<PasswordOrFederated<Email>>,
        display_name: String,
    })
    .unwrap();
    assert_eq!(fields.identity.to_string(), "identity");
    assert_eq!(credentials(&fields), "PasswordOrFederated<Email>");
    assert!(fields.active.is_none());
}

#[test]
fn the_active_marker_names_the_active_field() {
    let fields = parse(&quote! {
        #[user(identity)] identity: Identity<C>,
        #[user(active)] is_active: bool,
    })
    .unwrap();
    assert_eq!(fields.active.unwrap().to_string(), "is_active");
}

#[test]
fn markers_sit_next_to_other_attributes() {
    let fields = parse(&quote! {
        #[doc = "who"] #[user(identity)] #[serde(skip)] identity: Identity<C>,
    })
    .unwrap();
    assert_eq!(fields.identity.to_string(), "identity");
}

#[test]
fn a_missing_identity_is_reported_on_the_aggregate() {
    let msg = message(&quote! { #[id] id: EntityId, name: String });
    assert!(msg.contains("#[user(identity)]"), "{msg}");
    assert!(msg.contains("Profile"), "{msg}");
}

#[test]
fn a_second_identity_is_rejected() {
    let msg = message(&quote! {
        #[user(identity)] a: Identity<C>,
        #[user(identity)] b: Identity<C>,
    });
    assert!(msg.contains("only one field"), "{msg}");
    assert!(msg.contains("identity"), "{msg}");
}

#[test]
fn a_second_active_is_rejected() {
    let msg = message(&quote! {
        #[user(identity)] identity: Identity<C>,
        #[user(active)] a: bool,
        #[user(active)] b: bool,
    });
    assert!(msg.contains("only one field"), "{msg}");
    assert!(msg.contains("active"), "{msg}");
}

#[test]
fn one_field_cannot_carry_both_markers() {
    let msg = message(&quote! {
        #[user(identity, active)] identity: Identity<C>,
    });
    assert!(msg.contains("exactly one"), "{msg}");
}

#[test]
fn unknown_and_malformed_markers_report_the_usage() {
    for marker in [
        quote!(#[user(admin)]),
        quote!(#[user]),
        quote!(#[user()]),
        quote!(#[user(identity = 1)]),
    ] {
        let msg = message(&quote! { #marker identity: Identity<C> });
        assert!(msg.contains("`identity` or `active`"), "{msg}");
    }
}

#[test]
fn an_identity_field_must_carry_a_credentials_type_argument() {
    for ty in [quote!(String), quote!(Identity), quote!(Identity<'a>)] {
        let msg = message(&quote! { #[user(identity)] identity: #ty });
        assert!(msg.contains("Identity<C>"), "{msg}");
    }
}

#[test]
fn qualified_identity_types_are_accepted() {
    let fields = parse(&quote! {
        #[user(identity)] identity: event_sauce_auth::Identity<Cred>,
    })
    .unwrap();
    assert_eq!(credentials(&fields), "Cred");
}

#[test]
fn markers_without_the_user_flag_are_rejected() {
    let input = struct_input(&quote! { #[user(active)] is_active: bool });
    let err = UserFields::reject_markers(named(&input)).unwrap_err();
    assert!(err.to_string().contains("`user`"), "{err}");

    let input = struct_input(&quote! { #[id] id: EntityId });
    assert!(UserFields::reject_markers(named(&input)).is_ok());
}

#[test]
fn stripping_removes_only_the_user_markers() {
    let mut input = struct_input(&quote! {
        #[id] #[user(identity)] identity: Identity<C>,
        #[serde(skip)] #[user(active)] is_active: bool,
    });
    strip_user_markers(&mut input);
    let out = squeezed(&quote!(#input));
    assert!(!out.contains("user"), "{out}");
    assert!(out.contains("#[id]"), "{out}");
    assert!(out.contains("#[serde(skip)]"), "{out}");
}

fn fields_with_active() -> UserFields {
    parse(&quote! {
        #[user(identity)] identity: Identity<Cred>,
        #[user(active)] is_active: bool,
    })
    .unwrap()
}

fn fields_without_active() -> UserFields {
    parse(&quote! { #[user(identity)] identity: Identity<Cred> }).unwrap()
}

fn user_impl(fields: &UserFields) -> String {
    squeezed(&fields.to_impl(&parse_quote!(Profile), &quote!(::auth)))
}

#[test]
fn the_impl_reads_the_identity_and_the_active_field() {
    let out = user_impl(&fields_with_active());
    assert!(out.contains("impl::auth::UserAggregateforProfile"), "{out}");
    assert!(out.contains("typeCredentials=Cred;"), "{out}");
    assert!(
        out.contains("fnidentity(&self)->&::auth::Identity<Self::Credentials>{&self.identity}"),
        "{out}"
    );
    assert!(
        out.contains("fnis_active(&self)->bool{self.is_active}"),
        "{out}"
    );
}

#[test]
fn without_an_active_field_the_trait_default_applies() {
    assert!(!user_impl(&fields_without_active()).contains("is_active"));
}

fn claims(fields: &UserFields, own_claims: bool) -> String {
    squeezed(&fields.claims_override(&quote!(::core_crate), &quote!(::auth), own_claims))
}

#[test]
fn claims_start_from_the_credentials_alone() {
    let out = claims(&fields_without_active(), false);
    assert!(out.contains("fnclaims(&self)->Vec<::core_crate::AggregateClaim>"));
    assert!(out.contains("letmutclaims=Vec::new();"), "{out}");
    assert!(out.contains("self.identity.credentials()"), "{out}");
    assert!(
        out.contains("::auth::Credentials::claims(credentials)"),
        "{out}"
    );
}

#[test]
fn claims_extend_the_aggregates_own_claims_when_declared() {
    let out = claims(&fields_without_active(), true);
    assert!(
        out.contains("letmutclaims=self.aggregate_claims();"),
        "{out}"
    );
}

fn resolve(found: &[(&str, &str)]) -> Option<String> {
    auth_path_from(|package| {
        found
            .iter()
            .find(|(name, _)| *name == package)
            .map(|(_, ident)| (*ident).to_string())
    })
    .map(|path| squeezed(&path))
}

#[test]
fn the_auth_crate_is_reached_directly_when_it_is_a_dependency() {
    assert_eq!(
        resolve(&[("event-sauce-auth", "event_sauce_auth")]).as_deref(),
        Some("::event_sauce_auth")
    );
    assert_eq!(
        resolve(&[("event-sauce-auth", "renamed")]).as_deref(),
        Some("::renamed")
    );
}

#[test]
fn the_facade_reaches_the_auth_crate_through_its_reexport() {
    assert_eq!(
        resolve(&[("event-sauce", "event_sauce")]).as_deref(),
        Some("::event_sauce::auth")
    );
}

#[test]
fn the_direct_dependency_wins_over_the_facade() {
    assert_eq!(
        resolve(&[
            ("event-sauce", "event_sauce"),
            ("event-sauce-auth", "event_sauce_auth")
        ])
        .as_deref(),
        Some("::event_sauce_auth")
    );
}

#[test]
fn no_auth_dependency_resolves_to_nothing() {
    assert_eq!(resolve(&[]), None);
}
