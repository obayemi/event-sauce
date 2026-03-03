//! # event-sauce-macros
//!
//! Derive macros for event-sauce to reduce boilerplate.
//!
//! Provides: `#[derive(Entity)]`, `#[aggregate]`, `#[derive(Event)]`,
//! `#[derive(AggregateError)]`, `#[aggregate_error]`, `#[specification]`

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Ident, Meta};

/// Attributes for the #[aggregate(...)] container attribute
#[derive(Debug, FromMeta)]
struct AggregateAttrs {
    event: String,
    #[darling(default)]
    error: Option<String>,
    /// When true, skip generating `DefaultEntity` and `Entity::new()`.
    /// The aggregate must be constructed via init events.
    #[darling(default)]
    init: bool,
    /// Optional custom type name override. When set, uses this literal
    /// instead of `stringify!(StructName)` for `aggregate_type()`.
    #[darling(default)]
    type_name: Option<String>,
    /// When true, the aggregate's event/snapshot data will be encrypted at rest.
    /// Generates `fn is_encrypted() -> bool { true }`.
    #[darling(default)]
    encrypted: bool,
    /// Optional custom deleted state type. When set, generates
    /// `type DeletedState = <type>;` instead of `type DeletedState = Self;`.
    #[darling(default)]
    deleted_state: Option<String>,
}

/// Attributes for the #[event(...)] container attribute
#[derive(Debug, FromMeta)]
struct EventAttrs {
    version: i64,
    #[darling(default)]
    type_prefix: Option<String>,
    #[darling(default)]
    aggregate: Option<String>,
}

/// Derive macro for `Event` trait
///
/// # Panics
///
/// This macro may panic if:
/// - The enum variants are not in the expected format
/// - Tuple variants do not contain exactly one field when generating `Into` implementations
#[proc_macro_derive(Event, attributes(event))]
#[allow(clippy::too_many_lines)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the enum name
    let name = &input.ident;

    // Parse the #[event(...)] attribute
    let attrs = match extract_event_attrs(&input.attrs) {
        Ok(attrs) => attrs,
        Err(err) => return err,
    };

    // Extract variants from enum
    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return syn::Error::new_spanned(&input.ident, "Event can only be derived for enums")
                .to_compile_error()
                .into();
        }
    };

    // Generate event_type match arms
    let type_prefix = attrs.type_prefix.clone().unwrap_or_else(|| {
        // Extract the base name by removing "Event" suffix if present
        let name_str = name.to_string();
        name_str
            .strip_suffix("Event")
            .map(std::string::ToString::to_string)
            .unwrap_or(name_str)
    });

    let event_type_arms = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let variant_str = variant_name.to_string();
        let event_type_name = format!("{type_prefix}.{variant_str}");

        // Check if this is a tuple variant or named field variant
        match &variant.fields {
            Fields::Unnamed(_) => {
                quote! {
                    #name::#variant_name(..) => #event_type_name,
                }
            }
            _ => {
                quote! {
                    #name::#variant_name { .. } => #event_type_name,
                }
            }
        }
    });

    // Generate occurred_at match arms
    let occurred_at_arms = variants.iter().map(|variant| {
        let variant_name = &variant.ident;

        // Check if this is a tuple variant or named field variant
        match &variant.fields {
            Fields::Unnamed(_) => {
                quote! {
                    #name::#variant_name(event) => event.timestamp,
                }
            }
            _ => {
                quote! {
                    #name::#variant_name { timestamp, .. } => *timestamp,
                }
            }
        }
    });

    let version = attrs.version;

    // Generate Into and EventType implementations for tuple variants
    let into_impls = variants.iter().filter_map(|variant| {
        let variant_name = &variant.ident;

        // Only generate Into for tuple variants with exactly one field
        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed.first().unwrap().ty;
                Some(quote! {
                    impl Into<#name> for #field_type {
                        fn into(self) -> #name {
                            #name::#variant_name(self)
                        }
                    }
                })
            }
            _ => None,
        }
    });

    // Generate EventType implementations for tuple variant inner types
    let event_type_impls = variants.iter().filter_map(|variant| {
        let variant_name = &variant.ident;
        let variant_str = variant_name.to_string();
        let event_type_name = format!("{type_prefix}.{variant_str}");

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed.first().unwrap().ty;
                Some(quote! {
                    impl event_sauce_core::EventType for #field_type {
                        const EVENT_TYPE: &'static str = #event_type_name;
                    }
                })
            }
            _ => None,
        }
    });

    // Generate EventApplicator impl if aggregate type is specified AND there are tuple variants.
    // Events with only named field variants need manual EventApplicator implementations.
    let has_tuple_variants = variants
        .iter()
        .any(|v| matches!(&v.fields, Fields::Unnamed(_)));
    let event_applicator_impl = if let (Some(aggregate_type_str), true) =
        (&attrs.aggregate, has_tuple_variants)
    {
        let aggregate_type = Ident::new(aggregate_type_str, proc_macro2::Span::call_site());

        // Generate match arms for dispatch (with validation)
        let dispatch_arms = variants.iter().map(|variant| {
            let variant_name = &variant.ident;
            match &variant.fields {
                Fields::Unnamed(_) => {
                    quote! {
                        #name::#variant_name(e) => {
                            use event_sauce_core::ApplyEvent;
                            e.validate(aggregate)?;
                            e.apply(aggregate);
                            e.post_validate(aggregate)?;
                        },
                    }
                }
                _ => {
                    quote! {
                        #name::#variant_name { .. } => {},
                    }
                }
            }
        });

        // Generate match arms for dispatch_unchecked (apply only, no validation)
        let dispatch_unchecked_arms = variants.iter().map(|variant| {
            let variant_name = &variant.ident;
            match &variant.fields {
                Fields::Unnamed(_) => {
                    quote! {
                        #name::#variant_name(e) => {
                            use event_sauce_core::ApplyEvent;
                            e.apply(aggregate);
                        },
                    }
                }
                _ => {
                    quote! {
                        #name::#variant_name { .. } => {},
                    }
                }
            }
        });

        quote! {
            impl event_sauce_core::EventApplicator<#aggregate_type> for #name {
                fn dispatch(&self, aggregate: &mut #aggregate_type) -> std::result::Result<(), <#aggregate_type as event_sauce_core::Aggregate>::Error> {
                    match self {
                        #(#dispatch_arms)*
                    }
                    Ok(())
                }

                fn dispatch_unchecked(&self, aggregate: &mut #aggregate_type) {
                    match self {
                        #(#dispatch_unchecked_arms)*
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    // Generate aggregate type - required for the DomainEvent trait
    let aggregate_type = if let Some(aggregate_type_str) = &attrs.aggregate {
        let aggregate_ident = Ident::new(aggregate_type_str, proc_macro2::Span::call_site());
        quote! { type Aggregate = #aggregate_ident; }
    } else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "Missing aggregate type. Add aggregate = \"AggregateType\" to #[event(...)] attribute",
        )
        .to_compile_error()
        .into();
    };

    // Generate TryFrom implementations for EventEnvelope conversions
    let try_from_impls = quote! {
        // TryFrom<EventEnvelope> for owned conversion
        impl TryFrom<event_sauce_core::EventEnvelope> for #name {
            type Error = event_sauce_core::Error;

            fn try_from(envelope: event_sauce_core::EventEnvelope) -> event_sauce_core::Result<Self> {
                Self::from_envelope(&envelope)
            }
        }

        // TryFrom<&EventEnvelope> for reference conversion
        impl TryFrom<&event_sauce_core::EventEnvelope> for #name {
            type Error = event_sauce_core::Error;

            fn try_from(envelope: &event_sauce_core::EventEnvelope) -> event_sauce_core::Result<Self> {
                Self::from_envelope(envelope)
            }
        }
    };

    // Generate the implementation
    let gen = quote! {
        impl event_sauce_core::DomainEvent for #name {
            #aggregate_type

            fn event_type(&self) -> &'static str {
                match self {
                    #(#event_type_arms)*
                }
            }

            fn event_version(&self) -> event_sauce_core::EventVersion {
                event_sauce_core::EventVersion::new(#version)
            }

            fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
                match self {
                    #(#occurred_at_arms)*
                }
            }
        }

        // Generate EventApplicator impl if aggregate type is specified
        #event_applicator_impl

        // Generate Into implementations for each variant
        #(#into_impls)*

        // Generate EventType implementations for tuple variant inner types
        #(#event_type_impls)*

        // Generate TryFrom implementations for idiomatic Rust conversions
        #try_from_impls
    };

    gen.into()
}

/// Extract event attributes from the #[event(...)] attribute
fn extract_event_attrs(attrs: &[Attribute]) -> Result<EventAttrs, TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("event") {
            let nested = match &attr.meta {
                Meta::List(list) => &list.tokens,
                _ => continue,
            };

            // Parse the attribute arguments
            let event_attrs: EventAttrs =
                match darling::ast::NestedMeta::parse_meta_list(nested.clone()) {
                    Ok(nested) => match EventAttrs::from_list(&nested) {
                        Ok(attrs) => attrs,
                        Err(err) => return Err(err.write_errors().into()),
                    },
                    Err(err) => return Err(err.to_compile_error().into()),
                };

            return Ok(event_attrs);
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing #[event(version = ...)] attribute",
    )
    .to_compile_error()
    .into())
}

/// Find the ID field - looks for `#[id]` attribute or falls back to field named "id"
fn find_id_field_flexible(fields: &Fields) -> Result<Ident, TokenStream> {
    if let Fields::Named(named) = fields {
        // First, try to find field with #[id] attribute
        for field in &named.named {
            let field_name = field.ident.as_ref().expect("Named field should have ident");

            for attr in &field.attrs {
                if attr.path().is_ident("id") {
                    return Ok(field_name.clone());
                }
            }
        }

        // Fall back to field named "id"
        for field in &named.named {
            let field_name = field.ident.as_ref().expect("Named field should have ident");
            if field_name == "id" {
                return Ok(field_name.clone());
            }
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "No ID field found. Struct must have either a field marked with #[id] or a field named 'id'"
    )
    .to_compile_error()
    .into())
}

/// Derive macro for implementing the `Entity` trait.
///
/// Generates `Entity` implementation for a struct that has an `EntityId` field.
/// The ID field is identified by either a `#[id]` attribute or by being named `id`.
/// Non-ID fields must implement `Default`.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_macros::Entity;
/// use event_sauce_core::EntityId;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Entity, Serialize, Deserialize)]
/// struct User {
///     #[id]
///     id: EntityId,
///     name: String,
///     email: String,
/// }
/// ```
///
/// # Requirements
///
/// - Must be a struct with named fields
/// - Must have a field of type `EntityId` (identified by `#[id]` attr or named `id`)
/// - All non-ID fields must implement `Default`
///
/// # Panics
///
/// Will fail to compile if not a struct with named fields or no ID field is found.
#[proc_macro_derive(Entity, attributes(id))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Extract fields
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return syn::Error::new_spanned(name, "Entity can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let named_fields = match fields {
        Fields::Named(named) => &named.named,
        _ => {
            return syn::Error::new_spanned(
                name,
                "Entity can only be derived for structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    // Find the ID field
    let id_field_name = match find_id_field_flexible(fields) {
        Ok(name) => name,
        Err(err) => return err,
    };

    // Generate field initializers: id field gets the argument, others get Default::default()
    let field_inits = named_fields.iter().map(|f| {
        let fname = f.ident.as_ref().expect("Named field should have ident");
        if fname == &id_field_name {
            quote! { #fname: id }
        } else {
            quote! { #fname: Default::default() }
        }
    });

    let gen = quote! {
        impl event_sauce_core::Entity for #name {
            fn new(id: event_sauce_core::EntityId) -> Self {
                Self {
                    #(#field_inits),*
                }
            }

            fn entity_id(&self) -> event_sauce_core::EntityId {
                self.#id_field_name
            }
        }

        impl event_sauce_core::DefaultEntity for #name {}
    };

    gen.into()
}

/// Derive macro for `AggregateError` trait
///
/// This macro automatically implements the `AggregateError` marker trait
/// for error types. Use this with `thiserror::Error` for clean error handling.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_macros::AggregateError;
/// use thiserror::Error;
///
/// #[derive(AggregateError, Debug, Error)]
/// enum BankAccountError {
///     #[error("Insufficient funds: {balance}")]
///     InsufficientFunds { balance: i64 },
/// }
///
/// // Now BankAccountError implements AggregateError automatically
/// ```
///
/// # Requirements
///
/// - Works with any type (struct or enum)
/// - Typically used with enums deriving `thiserror::Error`
/// - The type should implement `std::error::Error` (usually via `#[derive(Error)]`)
///
/// # Usage
///
/// Combine with `thiserror::Error` for domain-specific errors:
///
/// ```ignore
/// #[derive(AggregateError, Debug, Error)]
/// enum OrderError {
///     #[error("Order not found: {0}")]
///     NotFound(String),
///
///     #[error("Invalid quantity: {0}")]
///     InvalidQuantity(u32),
/// }
/// ```
#[proc_macro_derive(AggregateError)]
pub fn derive_aggregate_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Generate the implementation
    let gen = quote! {
        impl event_sauce_core::AggregateError for #name {}
    };

    gen.into()
}

/// Attributes for the `#[aggregate_error(...)]` attribute
#[derive(Debug, FromMeta)]
struct AggregateErrorMacroAttrs {
    aggregate: String,
}

/// Attribute macro for aggregate errors that use the specification pattern.
///
/// This macro injects a `SpecificationFailed` variant into the error enum
/// with `#[from]` for automatic `SpecificationError<T>` conversion, and
/// implements the `AggregateError` marker trait.
///
/// # Usage
///
/// ```ignore
/// #[aggregate_error(aggregate = "Order")]
/// #[derive(Debug, thiserror::Error)]
/// enum OrderError {
///     #[error("Order already completed")]
///     OrderAlreadyCompleted,
/// }
/// ```
///
/// Expands to:
///
/// ```ignore
/// #[derive(Debug, thiserror::Error)]
/// enum OrderError {
///     #[error("Order already completed")]
///     OrderAlreadyCompleted,
///     /// Specification validation failure
///     #[error("{0}")]
///     SpecificationFailed(#[from] event_sauce_core::SpecificationError<Order>),
/// }
/// impl event_sauce_core::AggregateError for OrderError {}
/// ```
///
/// # Panics
///
/// Will fail to compile if not applied to an enum.
#[proc_macro_attribute]
pub fn aggregate_error(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the "for = Type" attribute
    let attr_tokens: proc_macro2::TokenStream = attr.into();
    let nested_meta = match darling::ast::NestedMeta::parse_meta_list(attr_tokens) {
        Ok(meta) => meta,
        Err(err) => return err.to_compile_error().into(),
    };

    let attrs = match AggregateErrorMacroAttrs::from_list(&nested_meta) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };

    let aggregate_type = Ident::new(&attrs.aggregate, proc_macro2::Span::call_site());

    // Parse the input enum
    let mut input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();

    // Add the SpecificationFailed variant to the enum
    match &mut input.data {
        Data::Enum(data) => {
            // Build the new variant tokens
            let variant: syn::Variant = syn::parse_quote! {
                /// Specification validation failure
                #[error("{0}")]
                SpecificationFailed(#[from] event_sauce_core::SpecificationError<#aggregate_type>)
            };
            data.variants.push(variant);
        }
        _ => {
            return syn::Error::new_spanned(&name, "#[aggregate_error] can only be used on enums")
                .to_compile_error()
                .into();
        }
    }

    // Emit the modified enum + AggregateError impl
    let gen = quote! {
        #input
        impl event_sauce_core::AggregateError for #name {}
    };

    gen.into()
}

/// Attribute macro for creating specification structs from functions.
///
/// Transforms a predicate function into a struct implementing `Specification<T>`.
///
/// # Usage
///
/// ## Simple static message
///
/// ```ignore
/// #[specification("Account must be active")]
/// fn is_active(account: &Account) -> bool {
///     account.status == AccountStatus::Active
/// }
/// // Generates: struct IsActive; impl Specification<Account> for IsActive { ... }
/// ```
///
/// ## With context fields referencing spec fields
///
/// ```ignore
/// #[specification("Insufficient funds", requested = amount)]
/// fn has_sufficient_funds(account: &Account, amount: i64) -> bool {
///     account.balance >= amount
/// }
/// // Generates: struct HasSufficientFunds { pub amount: i64 }
/// // error_message: "Insufficient funds: requested=<amount>"
/// ```
///
/// ## With context referencing candidate and spec fields
///
/// ```ignore
/// #[specification("Insufficient funds", balance = account.balance, requested = amount)]
/// fn has_sufficient_funds(account: &Account, amount: i64) -> bool {
///     account.balance >= amount
/// }
/// // error_message: "Insufficient funds: balance=<balance>, requested=<amount>"
/// ```
///
/// # Panics
///
/// Will fail to compile if the function doesn't have at least one `&T` parameter.
#[proc_macro_attribute]
pub fn specification(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_tokens: proc_macro2::TokenStream = attr.into();
    let input_fn = parse_macro_input!(item as syn::ItemFn);

    // Parse the attribute: "message" or "message", key = expr, ...
    let parsed = match parse_specification_attr(attr_tokens) {
        Ok(p) => p,
        Err(err) => return err.to_compile_error().into(),
    };

    let fn_name = &input_fn.sig.ident;
    let struct_name = Ident::new(&to_pascal_case(&fn_name.to_string()), fn_name.span());

    // Extract function parameters
    let params: Vec<_> = input_fn.sig.inputs.iter().collect();
    if params.is_empty() {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "#[specification] function must have at least one &T parameter",
        )
        .to_compile_error()
        .into();
    }

    // First param is the candidate (e.g., account: &Account)
    let (candidate_name, candidate_type) = match extract_ref_param(params[0]) {
        Ok(v) => v,
        Err(err) => return err.to_compile_error().into(),
    };

    // Extra params become struct fields
    let extra_params: Vec<_> = params[1..].to_vec();
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;

    if extra_params.is_empty() {
        generate_unit_spec(
            &parsed,
            fn_vis,
            &struct_name,
            &candidate_name,
            &candidate_type,
            fn_body,
        )
    } else {
        generate_parameterized_spec(
            &parsed,
            fn_vis,
            &struct_name,
            &candidate_name,
            &candidate_type,
            fn_body,
            &extra_params,
        )
    }
}

/// Generate a unit struct specification (no extra params)
fn generate_unit_spec(
    parsed: &SpecificationAttrData,
    fn_vis: &syn::Visibility,
    struct_name: &Ident,
    candidate_name: &Ident,
    candidate_type: &syn::Type,
    fn_body: &syn::Block,
) -> TokenStream {
    let message = &parsed.message;
    let error_message_body =
        build_error_message_body(message, &parsed.context, &[], candidate_name);

    // If the error message doesn't reference the candidate, prefix with _ to avoid warning
    let err_msg_param = if parsed.context.is_empty() {
        Ident::new(&format!("_{candidate_name}"), candidate_name.span())
    } else {
        candidate_name.clone()
    };

    let gen = quote! {
        #fn_vis struct #struct_name;

        impl event_sauce_core::Specification<#candidate_type> for #struct_name {
            fn is_satisfied_by(&self, #candidate_name: &#candidate_type) -> bool
                #fn_body

            fn error_message(&self, #err_msg_param: &#candidate_type) -> String {
                #error_message_body
            }
        }

        event_sauce_core::__impl_spec_ops!(#struct_name, #candidate_type);
    };

    gen.into()
}

/// Generate a parameterized struct specification (with extra params as fields)
fn generate_parameterized_spec(
    parsed: &SpecificationAttrData,
    fn_vis: &syn::Visibility,
    struct_name: &Ident,
    candidate_name: &Ident,
    candidate_type: &syn::Type,
    fn_body: &syn::Block,
    extra_params: &[&syn::FnArg],
) -> TokenStream {
    let message = &parsed.message;

    let field_defs: Vec<_> = extra_params
        .iter()
        .filter_map(|p| match p {
            syn::FnArg::Typed(pat_type) => {
                let pat = &pat_type.pat;
                let ty = &pat_type.ty;
                Some(quote! { pub #pat: #ty })
            }
            syn::FnArg::Receiver(_) => None,
        })
        .collect();

    let field_names: Vec<_> = extra_params
        .iter()
        .filter_map(|p| match p {
            syn::FnArg::Typed(pat_type) => Some(pat_type.pat.as_ref()),
            syn::FnArg::Receiver(_) => None,
        })
        .collect();

    let error_message_body =
        build_error_message_body(message, &parsed.context, &field_names, candidate_name);

    // If no context expr references the candidate, prefix with _ to avoid warning
    let err_msg_param = if context_references_candidate(&parsed.context, candidate_name) {
        candidate_name.clone()
    } else {
        Ident::new(&format!("_{candidate_name}"), candidate_name.span())
    };

    let gen = quote! {
        #fn_vis struct #struct_name {
            #(#field_defs),*
        }

        impl event_sauce_core::Specification<#candidate_type> for #struct_name {
            fn is_satisfied_by(&self, #candidate_name: &#candidate_type) -> bool {
                #(let #field_names = &self.#field_names;)*
                let _ = (#(&#field_names),*);
                #fn_body
            }

            fn error_message(&self, #err_msg_param: &#candidate_type) -> String {
                #(let #field_names = &self.#field_names;)*
                #error_message_body
            }
        }

        event_sauce_core::__impl_spec_ops!(#struct_name, #candidate_type);
    };

    gen.into()
}

/// Build the error message body token stream from context pairs
fn build_error_message_body(
    message: &str,
    context: &[(Ident, proc_macro2::TokenStream)],
    field_names: &[&syn::Pat],
    candidate_name: &Ident,
) -> proc_macro2::TokenStream {
    if context.is_empty() {
        quote! { #message.to_string() }
    } else {
        let ctx_parts: Vec<_> = context
            .iter()
            .map(|(key, expr)| {
                let key_str = key.to_string();
                let resolved = resolve_context_expr(expr, field_names, candidate_name);
                quote! { format!("{}={}", #key_str, #resolved) }
            })
            .collect();
        quote! {
            let ctx = [#(#ctx_parts),*].join(", ");
            format!("{}: {}", #message, ctx)
        }
    }
}

/// Check if any context expression references the candidate parameter name
fn context_references_candidate(
    context: &[(Ident, proc_macro2::TokenStream)],
    candidate_name: &Ident,
) -> bool {
    let candidate_str = candidate_name.to_string();
    context
        .iter()
        .any(|(_, expr)| expr.to_string().contains(&candidate_str))
}

/// Parsed specification attribute data
struct SpecificationAttrData {
    message: String,
    context: Vec<(Ident, proc_macro2::TokenStream)>,
}

/// Parse `"message"` or `"message", key = expr, ...`
fn parse_specification_attr(
    tokens: proc_macro2::TokenStream,
) -> Result<SpecificationAttrData, syn::Error> {
    let mut iter = tokens.into_iter().peekable();

    // First token should be a string literal
    let message = match iter.next() {
        Some(proc_macro2::TokenTree::Literal(lit)) => {
            let s = lit.to_string();
            // Strip quotes
            s.trim_matches('"').to_string()
        }
        other => {
            return Err(syn::Error::new(
                other
                    .as_ref()
                    .map_or(proc_macro2::Span::call_site(), proc_macro2::TokenTree::span),
                "Expected a string literal as first argument to #[specification]",
            ));
        }
    };

    let mut context = Vec::new();

    // Parse optional ", key = expr" pairs
    while iter.peek().is_some() {
        // Expect comma
        match iter.next() {
            Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ',' => {}
            _ => break,
        }

        // Check if remaining tokens look like a key = expr or end of stream
        if iter.peek().is_none() {
            break;
        }

        // Read key ident
        let Some(proc_macro2::TokenTree::Ident(key)) = iter.next() else {
            break;
        };

        // Expect '='
        match iter.next() {
            Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '=' => {}
            _ => {
                return Err(syn::Error::new(
                    key.span(),
                    "Expected '=' after context key",
                ));
            }
        }

        // Read expr tokens until comma or end
        let mut expr_tokens = Vec::new();
        while let Some(tok) = iter.peek() {
            if let proc_macro2::TokenTree::Punct(p) = tok {
                if p.as_char() == ',' {
                    break;
                }
            }
            expr_tokens.push(iter.next().unwrap());
        }

        let expr: proc_macro2::TokenStream = expr_tokens.into_iter().collect();
        context.push((key, expr));
    }

    Ok(SpecificationAttrData { message, context })
}

/// Convert `snake_case` to `PascalCase`
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Extract the name and type from a `&T` function parameter
fn extract_ref_param(param: &syn::FnArg) -> Result<(Ident, syn::Type), syn::Error> {
    match param {
        syn::FnArg::Typed(pat_type) => {
            let name = match pat_type.pat.as_ref() {
                syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                _ => {
                    return Err(syn::Error::new_spanned(
                        &pat_type.pat,
                        "Expected a simple identifier pattern",
                    ));
                }
            };

            // Extract the inner type from &T
            match pat_type.ty.as_ref() {
                syn::Type::Reference(type_ref) => Ok((name, *type_ref.elem.clone())),
                _ => Err(syn::Error::new_spanned(
                    &pat_type.ty,
                    "First parameter must be a reference type (&T)",
                )),
            }
        }
        syn::FnArg::Receiver(_) => Err(syn::Error::new_spanned(
            param,
            "#[specification] functions cannot have self parameters",
        )),
    }
}

/// Resolve a context expression:
/// - If expr is a bare ident matching an extra param -> `self.ident`
/// - If expr references candidate param -> passed through as-is
fn resolve_context_expr(
    expr: &proc_macro2::TokenStream,
    _field_names: &[&syn::Pat],
    _candidate_name: &Ident,
) -> proc_macro2::TokenStream {
    // Just pass through - the let bindings in is_satisfied_by/error_message
    // handle the resolution (spec fields are bound as local vars,
    // candidate param is the function parameter)
    expr.clone()
}

/// Attribute macro for aggregate transformation.
///
/// This macro generates `Entity`, `Aggregate`, and optionally `DefaultEntity` trait
/// implementations for a struct. The struct retains its original form.
///
/// # Usage
///
/// ## Standard aggregate (with `DefaultEntity`)
///
/// ```ignore
/// #[aggregate(event = "ProductEvent", error = "ProductError")]
/// struct Product {
///     #[id]
///     id: EntityId,
///     name: String,
///     price: i64,
/// }
/// ```
///
/// ## Init-event aggregate (without `DefaultEntity`)
///
/// ```ignore
/// #[aggregate(event = "OrderEvent", error = "OrderError", init)]
/// struct Order {
///     #[id]
///     id: EntityId,
///     user_id: EntityId,  // no Option needed — set by init event
/// }
/// ```
///
/// This generates:
/// - `Entity` trait implementation (using `#[id]` field)
/// - `Aggregate` trait implementation (with Event + Error types)
/// - `DefaultEntity` marker (unless `init` is set)
///
/// When `init` is set, `Entity::new()` uses the default panicking implementation.
/// The aggregate must be constructed via `UninitAggregateRoot::apply_init()`.
///
/// # Attributes
///
/// - `#[id]` - Marks the field containing the entity ID (type must be `EntityId`)
/// - `event = "..."` - The event enum type name
/// - `error = "..."` - The error type name (optional, defaults to `()`)
/// - `init` - Skip `DefaultEntity` and `Entity::new()`; aggregate uses init events
///
/// # Panics
///
/// This macro may panic if:
/// - Not applied to a struct with named fields
/// - No ID field is found
#[proc_macro_attribute]
#[allow(clippy::too_many_lines)]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute arguments
    let attr_tokens: proc_macro2::TokenStream = attr.into();
    let input = parse_macro_input!(item as DeriveInput);

    // Extract aggregate name and visibility
    let aggregate_name = &input.ident;

    // Parse aggregate attributes from attribute tokens
    let nested_meta = match darling::ast::NestedMeta::parse_meta_list(attr_tokens) {
        Ok(meta) => meta,
        Err(err) => return err.to_compile_error().into(),
    };

    let aggregate_attrs = match AggregateAttrs::from_list(&nested_meta) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };

    let event_type = Ident::new(&aggregate_attrs.event, proc_macro2::Span::call_site());
    let error_type: proc_macro2::TokenStream = if let Some(e) = aggregate_attrs.error {
        let ident = Ident::new(&e, proc_macro2::Span::call_site());
        quote! { #ident }
    } else {
        quote! { () }
    };

    // Extract fields
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return syn::Error::new_spanned(
                aggregate_name,
                "#[aggregate] can only be used on structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let named_fields = match fields {
        Fields::Named(named) => &named.named,
        _ => {
            return syn::Error::new_spanned(
                aggregate_name,
                "#[aggregate] only supports structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    // Find ID field
    let id_field_name = match find_id_field_flexible(fields) {
        Ok(name) => name,
        Err(err) => return err,
    };

    // Strip #[id] attributes from the struct fields before emitting
    let mut cleaned_input = input.clone();
    if let Data::Struct(ref mut data) = cleaned_input.data {
        if let Fields::Named(ref mut named) = data.fields {
            for field in &mut named.named {
                field.attrs.retain(|attr| !attr.path().is_ident("id"));
            }
        }
    }

    let is_init = aggregate_attrs.init;

    // Generate Entity impl — for init aggregates, only entity_id (default panicking new())
    // For DefaultEntity aggregates, also override new() with field defaults
    let entity_impl = if is_init {
        quote! {
            impl event_sauce_core::Entity for #aggregate_name {
                fn entity_id(&self) -> event_sauce_core::EntityId {
                    self.#id_field_name
                }
            }
        }
    } else {
        let field_inits = named_fields.iter().map(|f| {
            let fname = f.ident.as_ref().expect("Named field should have ident");
            if fname == &id_field_name {
                quote! { #fname: id }
            } else {
                quote! { #fname: Default::default() }
            }
        });

        quote! {
            impl event_sauce_core::Entity for #aggregate_name {
                fn new(id: event_sauce_core::EntityId) -> Self {
                    Self {
                        #(#field_inits),*
                    }
                }

                fn entity_id(&self) -> event_sauce_core::EntityId {
                    self.#id_field_name
                }
            }

            impl event_sauce_core::DefaultEntity for #aggregate_name {}
        }
    };

    // Generate aggregate_type() override using stringify! (stable, deterministic)
    let aggregate_type_override = if let Some(custom_name) = &aggregate_attrs.type_name {
        let name_lit = syn::LitStr::new(custom_name, proc_macro2::Span::call_site());
        quote! {
            fn aggregate_type() -> event_sauce_core::AggregateType {
                event_sauce_core::AggregateType::new(#name_lit)
            }
        }
    } else {
        quote! {
            fn aggregate_type() -> event_sauce_core::AggregateType {
                event_sauce_core::AggregateType::new(stringify!(#aggregate_name))
            }
        }
    };

    // Generate is_encrypted() override if the `encrypted` flag is set
    let is_encrypted_override = if aggregate_attrs.encrypted {
        quote! {
            fn is_encrypted() -> bool {
                true
            }
        }
    } else {
        quote! {}
    };

    // Generate DeletedState type
    let deleted_state_type = if let Some(ds) = &aggregate_attrs.deleted_state {
        let ds_ident = Ident::new(ds, proc_macro2::Span::call_site());
        quote! { type DeletedState = #ds_ident; }
    } else {
        quote! { type DeletedState = Self; }
    };

    // Emit the cleaned struct + Entity impl + Aggregate impl
    let gen = quote! {
        #cleaned_input

        #entity_impl

        impl event_sauce_core::Aggregate for #aggregate_name {
            type Event = #event_type;
            type Error = #error_type;
            #deleted_state_type

            #aggregate_type_override
            #is_encrypted_override
        }
    };

    gen.into()
}

/// Derive macro for implementing `AggregateId` and `EntityIdFor` on a newtype ID.
///
/// Generates type-safe ID wrappers that carry aggregate type information,
/// enabling compile-time validation when loading aggregates.
///
/// # Usage
///
/// ```ignore
/// use event_sauce_macros::AggregateId;
/// use event_sauce_core::EntityId;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AggregateId)]
/// #[aggregate_id(Group)]
/// pub struct GroupId(pub EntityId);
/// ```
///
/// Generates:
/// - `impl AggregateId for GroupId` with `type Aggregate = Group`
/// - `impl From<EntityId> for GroupId` and `impl From<GroupId> for EntityId`
/// - `impl Deref<Target = EntityId> for GroupId`
/// - `impl Display for GroupId` (delegates to inner UUID)
///
/// # Requirements
///
/// - Must be a tuple struct with exactly one field of type `EntityId`
/// - Must have `#[aggregate_id(AggregateType)]` attribute specifying the aggregate
///
/// # Panics
///
/// Will fail to compile if not a tuple struct with one field.
#[proc_macro_derive(AggregateId, attributes(aggregate_id))]
pub fn derive_aggregate_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Extract the aggregate type from #[aggregate_id(Type)]
    let aggregate_type = match extract_aggregate_id_type(&input.attrs) {
        Ok(t) => t,
        Err(err) => return err,
    };

    // Verify it's a tuple struct with exactly one field
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {}
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "AggregateId can only be derived for tuple structs with exactly one EntityId field",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "AggregateId can only be derived for tuple structs",
            )
            .to_compile_error()
            .into();
        }
    }

    let aggregate_ident = Ident::new(&aggregate_type, proc_macro2::Span::call_site());

    let gen = quote! {
        impl event_sauce_core::AggregateId for #name {
            type Aggregate = #aggregate_ident;

            fn as_entity_id(&self) -> event_sauce_core::EntityId {
                self.0
            }
        }

        impl From<event_sauce_core::EntityId> for #name {
            fn from(id: event_sauce_core::EntityId) -> Self {
                Self(id)
            }
        }

        impl From<#name> for event_sauce_core::EntityId {
            fn from(id: #name) -> Self {
                id.0
            }
        }

        impl std::ops::Deref for #name {
            type Target = event_sauce_core::EntityId;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
    };

    gen.into()
}

/// Extract the aggregate type name from `#[aggregate_id(Type)]` attribute.
fn extract_aggregate_id_type(attrs: &[Attribute]) -> Result<String, TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("aggregate_id") {
            // Parse the tokens inside the parentheses
            let tokens = match &attr.meta {
                Meta::List(list) => list.tokens.clone(),
                _ => {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "Expected #[aggregate_id(AggregateType)]",
                    )
                    .to_compile_error()
                    .into());
                }
            };

            // The tokens should be a single ident
            let ident: Ident = match syn::parse2(tokens) {
                Ok(ident) => ident,
                Err(err) => return Err(err.to_compile_error().into()),
            };

            return Ok(ident.to_string());
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing #[aggregate_id(AggregateType)] attribute",
    )
    .to_compile_error()
    .into())
}
