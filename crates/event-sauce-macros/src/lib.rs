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
    version: i32,
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

            fn event_version(&self) -> i32 {
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

/// Derive macro for creating strongly-typed `AggregateId` wrappers.
///
/// This macro provides a zero-boilerplate way to create custom aggregate IDs with
/// transparent access to all `AggregateId` methods via `Deref`.
///
/// # What it generates
///
/// - `Deref` implementation for transparent method access
/// - `AsRef<AggregateId>` for reference conversion
/// - `From<AggregateId>` and `Into<AggregateId>` for conversions
/// - `Display` implementation (customizable with `#[display]` attribute)
/// - `new()` constructor that generates a random ID
///
/// # Examples
///
/// ## Basic usage (just 3 lines!)
///
/// ```ignore
/// use event_sauce_macros::AggregateId;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AggregateId)]
/// #[repr(transparent)]
/// struct UserId(event_sauce_core::AggregateId);
///
/// let user_id = UserId::new();
/// // Can call AggregateId methods directly via Deref:
/// let uuid = user_id.to_uuid();
/// println!("{}", user_id);
/// ```
///
/// ## With custom display format
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AggregateId)]
/// #[repr(transparent)]
/// #[display("User-{}")]
/// struct UserId(event_sauce_core::AggregateId);
///
/// let user_id = UserId::new();
/// assert!(format!("{}", user_id).starts_with("User-"));
/// ```
///
/// # Requirements
///
/// - Must be a tuple struct with exactly one field of type `AggregateId`
/// - Add `#[repr(transparent)]` for zero-cost abstraction
/// - Add standard derives: `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`
///
/// # Panics
///
/// Will fail to compile if not a tuple struct with exactly one `AggregateId` field.
#[proc_macro_derive(AggregateId, attributes(display))]
pub fn derive_aggregate_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let vis = &input.vis;

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
                     Example: #[derive(AggregateId)] struct UserId(AggregateId);"
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "AggregateId can only be derived for tuple structs"
            )
            .to_compile_error()
            .into();
        }
    };

    // Check if the field type is AggregateId
    let field_type_str = quote! { #field_type }.to_string();
    let is_aggregate_id = field_type_str.contains("AggregateId");

    if !is_aggregate_id {
        return syn::Error::new_spanned(
            field_type,
            "The derive(AggregateId) macro expects the inner type to be AggregateId.\n\
             Example: #[derive(AggregateId)] struct UserId(AggregateId);"
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
        // Implement Deref for transparent access to AggregateId methods
        impl std::ops::Deref for #name {
            type Target = event_sauce_core::AggregateId;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        // Implement AsRef for conversion to &AggregateId
        impl std::convert::AsRef<event_sauce_core::AggregateId> for #name {
            fn as_ref(&self) -> &event_sauce_core::AggregateId {
                &self.0
            }
        }

        // Implement From<AggregateId> for easy construction
        impl std::convert::From<event_sauce_core::AggregateId> for #name {
            fn from(id: event_sauce_core::AggregateId) -> Self {
                Self(id)
            }
        }

        // Implement Into<AggregateId> for easy conversion
        impl std::convert::From<#name> for event_sauce_core::AggregateId {
            fn from(id: #name) -> Self {
                id.0
            }
        }

        // Implement Display
        #display_impl

        // Add a convenient constructor
        impl #name {
            #vis fn new() -> Self {
                Self(event_sauce_core::AggregateId::new())
            }
        }
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

        // Implement Default if both state and ID have Default
        impl Default for #aggregate_name
        where
            #state_name: Default,
            #id_type: Default,
        {
            fn default() -> Self {
                Self {
                    id: #id_type::default(),
                    state: #state_name::default(),
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }
        }

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
            type Event = #event_type;
            type Id = #id_type;
            type Error = #error_type;
            type State = #state_name;

            fn new(id: Self::Id) -> Self {
                Self {
                    id,                                         // ID stored in wrapper
                    state: #state_name::default(),              // State is pure business logic
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }

            fn aggregate_id(&self) -> &Self::Id {
                &self.id                                        // ID accessed from wrapper
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
