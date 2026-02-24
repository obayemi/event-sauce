//! # event-sauce-macros
//!
//! Derive macros for event-sauce to reduce boilerplate.
//!
//! Provides: #[derive(Aggregate)], #[derive(AggregateState)], #[derive(Event)], #[derive(AggregateId)]

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
    id: String,
    event: String,
    #[darling(default)]
    error: Option<String>,
}

/// Attributes for the #[event(...)] container attribute
#[derive(Debug, FromMeta)]
struct EventAttrs {
    version: u64,
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

    // Generate Into implementations for tuple variants
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

    // Generate apply_event method if aggregate type is specified
    let apply_event_impl = if let Some(aggregate_type_str) = &attrs.aggregate {
        let aggregate_type = Ident::new(aggregate_type_str, proc_macro2::Span::call_site());

        // Generate match arms for apply_event that implement full validation flow:
        // 1. validate() - pre-conditions
        // 2. apply() - state transformation
        // 3. post_validate() - post-conditions/invariants
        let apply_arms = variants.iter().map(|variant| {
            let variant_name = &variant.ident;

            // For tuple variants, extract the inner event and call validation -> apply -> post_validate
            match &variant.fields {
                Fields::Unnamed(_) => {
                    quote! {
                        #name::#variant_name(e) => {
                            use event_sauce_core::ApplyEvent;
                            e.validate(self)?;
                            e.apply(self);
                            e.post_validate(self)?;
                        },
                    }
                }
                _ => {
                    // For named fields, generate empty arm (no validation)
                    quote! {
                        #name::#variant_name { .. } => {},
                    }
                }
            }
        });

        quote! {
            impl #aggregate_type {
                /// Auto-generated method that applies events to the aggregate
                /// by delegating to each event's ApplyEvent implementation.
                ///
                /// This method runs the full event application lifecycle:
                /// 1. Validates pre-conditions using `validate()`
                /// 2. Applies state changes using `apply()`
                /// 3. Validates post-conditions using `post_validate()`
                ///
                /// # Errors
                ///
                /// Returns an error if validation (pre or post) fails.
                pub fn apply_event(&mut self, event: &#name) -> std::result::Result<(), <Self as event_sauce_core::Aggregate>::Error> {
                    match event {
                        #(#apply_arms)*
                    }
                    Ok(())
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

            fn event_version(&self) -> u64 {
                #version
            }

            fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
                match self {
                    #(#occurred_at_arms)*
                }
            }
        }

        // Generate apply_event impl if state type is specified
        #apply_event_impl

        // Generate Into implementations for each variant
        #(#into_impls)*

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

/// Find the ID field - looks for `#[aggregate_id]` attribute or falls back to field named "id"
fn find_id_field_flexible(fields: &Fields) -> Result<Ident, TokenStream> {
    if let Fields::Named(named) = fields {
        // First, try to find field with #[aggregate_id] attribute
        for field in &named.named {
            let field_name = field.ident.as_ref().expect("Named field should have ident");

            for attr in &field.attrs {
                if attr.path().is_ident("aggregate_id") {
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
        "No ID field found. Struct must have either a field marked with #[aggregate_id] or a field named 'id'"
    )
    .to_compile_error()
    .into())
}

/// Derive macro for implementing the `AggregateId` trait.
///
/// This macro provides a zero-boilerplate way to create custom aggregate IDs that
/// implement the `AggregateId` trait.
///
/// # What it generates
///
/// - `AggregateId` trait implementation with `to_uuid()` and `from_uuid()` methods
/// - `Display` implementation (customizable with `#[display]` attribute)
/// - The trait provides default implementations for `new()` and `nil()`
///
/// # Examples
///
/// ## Basic usage
///
/// ```ignore
/// use event_sauce_macros::AggregateId;
/// use uuid::Uuid;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
/// #[repr(transparent)]
/// struct CounterId(Uuid);
///
/// let counter_id = CounterId::new();
/// let uuid = counter_id.to_uuid();
/// println!("{}", counter_id);
/// ```
///
/// ## With custom display format
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
/// #[repr(transparent)]
/// #[display("Counter-{}")]
/// struct CounterId(Uuid);
///
/// let counter_id = CounterId::new();
/// assert!(format!("{}", counter_id).starts_with("Counter-"));
/// ```
///
/// # Requirements
///
/// - Must be a tuple struct with exactly one field of type `Uuid` (or path ending in `Uuid`)
/// - Add `#[repr(transparent)]` for zero-cost abstraction
/// - Add standard derives: `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize`
/// - Must be `Copy` if you want to use it efficiently (recommended)
///
/// # Panics
///
/// Will fail to compile if not a tuple struct with exactly one `Uuid` field.
#[proc_macro_derive(AggregateId, attributes(display))]
pub fn derive_aggregate_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Check that this is a tuple struct with one field
    let field_type = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "AggregateId can only be derived for tuple structs with exactly one field.\n\
                     Example: #[derive(AggregateId)] struct CounterId(Uuid);",
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
    };

    // Check if the field type is Uuid (or contains Uuid in the path)
    let field_type_str = quote! { #field_type }.to_string();
    let is_uuid = field_type_str.contains("Uuid");

    if !is_uuid {
        return syn::Error::new_spanned(
            field_type,
            "The derive(AggregateId) macro expects the inner type to be Uuid.\n\
             Example: #[derive(AggregateId)] struct CounterId(Uuid);",
        )
        .to_compile_error()
        .into();
    }

    // Parse display format attribute if present
    let mut display_format = None;
    for attr in &input.attrs {
        if attr.path().is_ident("display") {
            if let Ok(value) = attr.parse_args::<syn::LitStr>() {
                display_format = Some(value.value());
            }
        }
    }

    let display_impl = if let Some(format) = display_format {
        quote! {
            impl std::fmt::Display for #name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, #format, self.0)
                }
            }
        }
    } else {
        // Default: just show the UUID
        quote! {
            impl std::fmt::Display for #name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    std::fmt::Display::fmt(&self.0, f)
                }
            }
        }
    };

    // Generate the implementation
    let gen = quote! {
        // Implement the AggregateId trait
        impl event_sauce_core::AggregateId for #name {
            fn to_uuid(&self) -> uuid::Uuid {
                self.0
            }

            fn from_uuid(uuid: uuid::Uuid) -> Self {
                Self(uuid)
            }
        }

        // Implement Display
        #display_impl
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

/// Attribute macro for aggregate transformation
///
/// This macro transforms an aggregate struct with business fields into:
/// - A State struct with mirrored fields
/// - A wrapper struct with infrastructure (version, `pending_events`)
/// - Deref/DerefMut implementations for transparent field access
/// - Full Aggregate trait implementation
///
/// # Usage
///
/// ```ignore
/// #[aggregate(id = "ProductId", event = "ProductEvent", error = "ProductError")]
/// struct Product {
///     #[aggregate_id]
///     id: ProductId,
///     name: String,
///     price: i64,
/// }
/// ```
///
/// This generates:
/// - `ProductState` struct with the business fields
/// - `Product` wrapper struct with state + infrastructure
/// - Full trait implementations
///
/// # Attributes
///
/// - `#[aggregate_id]` - Marks the field containing the aggregate ID
///
/// # Panics
///
/// This macro may panic if:
/// - The filtered derive tokens cannot be parsed into a valid token stream
#[proc_macro_attribute]
#[allow(clippy::too_many_lines)]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute arguments
    let attr_tokens: proc_macro2::TokenStream = attr.into();
    let input = parse_macro_input!(item as DeriveInput);

    // Extract aggregate name and visibility
    let aggregate_name = &input.ident;
    let vis = &input.vis;

    // Parse aggregate attributes from attribute tokens
    let nested_meta = match darling::ast::NestedMeta::parse_meta_list(attr_tokens) {
        Ok(meta) => meta,
        Err(err) => return err.to_compile_error().into(),
    };

    let aggregate_attrs = match AggregateAttrs::from_list(&nested_meta) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };

    let id_type = Ident::new(&aggregate_attrs.id, proc_macro2::Span::call_site());
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

    // Extract field definitions (all business fields - no ID filtering yet)
    let all_fields = match fields {
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

    // Find ID field (if user provided it) or we'll generate it
    let user_provided_id = find_id_field_flexible(fields).ok();

    // Generate State struct name
    let state_name = Ident::new(
        &format!("{aggregate_name}State"),
        proc_macro2::Span::call_site(),
    );

    // Filter out ID field from state fields (state should have NO infrastructure)
    let state_fields: Vec<_> = if let Some(ref id_field_name) = user_provided_id {
        all_fields
            .iter()
            .filter(|f| f.ident.as_ref() != Some(id_field_name))
            .collect()
    } else {
        // No ID field provided by user, use all fields for state
        all_fields.iter().collect()
    };

    // Check for Default derive
    let has_default = input.attrs.iter().any(|attr| {
        if let Meta::List(list) = &attr.meta {
            if list.path.is_ident("derive") {
                return list.tokens.to_string().contains("Default");
            }
        }
        false
    });

    // Generate State Default impl by calling Default on all fields (excluding ID)
    let state_default_impl = if has_default {
        // Extract field names and generate Default::default() for each (excluding ID)
        let field_defaults = state_fields.iter().map(|f| {
            let name = &f.ident;
            quote! { #name: Default::default() }
        });

        quote! {
            impl Default for #state_name {
                fn default() -> Self {
                    Self {
                        #(#field_defaults),*
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    // Preserve other derives from the original struct (excluding Default)
    let preserved_derives: Vec<_> = input
        .attrs
        .iter()
        .filter_map(|attr| {
            if let Meta::List(list) = &attr.meta {
                if list.path.is_ident("derive") {
                    let tokens_str = list.tokens.to_string();
                    let derives: Vec<&str> = tokens_str.split(',').map(str::trim).collect();
                    let filtered: Vec<_> =
                        derives.into_iter().filter(|&d| d != "Default").collect();
                    if !filtered.is_empty() {
                        let filtered_str = filtered.join(", ");
                        let tokens: proc_macro2::TokenStream = filtered_str.parse().unwrap();
                        return Some(quote! { #[derive(#tokens)] });
                    }
                }
            }
            None
        })
        .collect();

    // Generate the complete output
    let gen = quote! {
        // Generate the State struct (business logic only - NO ID)
        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
        #(#preserved_derives)*
        #vis struct #state_name {
            #(#state_fields),*
        }

        #state_default_impl

        // Generate the wrapper aggregate struct (with ID as infrastructure)
        #[derive(::serde::Serialize, ::serde::Deserialize, Debug)]
        #vis struct #aggregate_name {
            id: #id_type,                          // ID stored in wrapper (infrastructure)
            state: #state_name,                    // State is pure business logic
            version: event_sauce_core::Version,
            pending_events: Vec<#event_type>,
        }

        impl #aggregate_name {
            /// Returns a reference to the internal state
            pub fn state_ref(&self) -> &#state_name {
                &self.state
            }

            /// Returns a mutable reference to the internal state
            pub fn state_mut(&mut self) -> &mut #state_name {
                &mut self.state
            }
        }

        // Note: We don't implement Default for the aggregate wrapper
        // because aggregate IDs typically don't have a sensible default value.
        // Users should explicitly create aggregates with Aggregate::new(id).

        // Implement Deref for transparent field access
        impl std::ops::Deref for #aggregate_name {
            type Target = #state_name;

            fn deref(&self) -> &Self::Target {
                &self.state
            }
        }

        // Implement DerefMut for transparent mutable field access
        impl std::ops::DerefMut for #aggregate_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.state
            }
        }

        // Implement Aggregate trait
        impl event_sauce_core::Aggregate for #aggregate_name {
            type Id = #id_type;
            type Event = #event_type;
            type Error = #error_type;
            type State = #state_name;

            fn new(id: Self::Id) -> Self {
                Self {
                    id,
                    state: #state_name::default(),
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }

            fn aggregate_id(&self) -> &Self::Id {
                &self.id
            }

            fn version(&self) -> event_sauce_core::Version {
                self.version
            }

            fn pending_events(&self) -> &[Self::Event] {
                &self.pending_events
            }

            fn clear_pending_events(&mut self) {
                self.pending_events.clear();
            }

            fn apply<E: Into<Self::Event>>(&mut self, event: E) -> std::result::Result<(), Self::Error> {
                let event = event.into();
                self.apply_internal(&event)?;
                self.pending_events.push(event);
                Ok(())
            }

            fn apply_internal(&mut self, event: &Self::Event) -> std::result::Result<(), Self::Error> {
                self.apply_event(event)?;
                self.version = self.version.next();
                Ok(())
            }

            fn state(&self) -> &Self::State {
                &self.state
            }

            fn from_snapshot(id: Self::Id, version: event_sauce_core::Version, state: Self::State) -> Self {
                Self {
                    id,
                    state,
                    version,
                    pending_events: Vec::new(),
                }
            }
        }
    };

    gen.into()
}
