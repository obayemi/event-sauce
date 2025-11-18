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
    #[darling(default)]
    name: Option<String>,
}

/// Derive macro for Aggregate trait
#[proc_macro_derive(
    Aggregate,
    attributes(aggregate, aggregate_id, aggregate_version, aggregate_events)
)]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the struct name
    let name = &input.ident;

    // Parse the #[aggregate(...)] attribute
    let (id_type, event_type, error_type_opt) = match extract_aggregate_attrs(&input.attrs) {
        Ok(attrs) => attrs,
        Err(err) => return err,
    };

    // Handle optional error type - if None, use unit type ()
    let error_type: proc_macro2::TokenStream = if let Some(ident) = error_type_opt {
        quote! { #ident }
    } else {
        quote! { () }
    };

    // Extract field information
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "Aggregate can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // Find the fields marked with attributes
    let (id_field, version_field, events_field) = match extract_field_names(fields) {
        Ok(fields) => fields,
        Err(err) => return err,
    };

    // Generate the implementation with Default bound for new()
    let gen = quote! {
        impl event_sauce_core::Aggregate for #name
        where
            Self: Default,
        {
            type Event = #event_type;
            type Id = #id_type;
            type Error = #error_type;

            fn new(id: Self::Id) -> Self {
                let mut aggregate = Self::default();
                aggregate.#id_field = id;
                aggregate.#version_field = event_sauce_core::Version::initial();
                aggregate.#events_field = Vec::new();
                aggregate
            }

            fn aggregate_id(&self) -> &Self::Id {
                &self.#id_field
            }

            fn version(&self) -> event_sauce_core::Version {
                self.#version_field
            }

            fn pending_events(&self) -> &[Self::Event] {
                &self.#events_field
            }

            fn clear_pending_events(&mut self) {
                self.#events_field.clear();
            }

            fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
                let event = event.into();
                self.apply_internal(&event)?;
                self.#events_field.push(event);
                Ok(())
            }

            fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
                // The apply_event method is expected to return Result<(), Self::Error>.
                // If the Event derive macro is used with aggregate="...", it will generate
                // an apply_event method that handles validation, apply, and post_validate.
                // Otherwise, the user provides their own apply_event that should return Ok(()).
                self.apply_event(event)?;
                self.#version_field = self.#version_field.next();
                Ok(())
            }
        }
    };

    gen.into()
}

/// Extract aggregate attributes from the #[aggregate(...)] attribute
fn extract_aggregate_attrs(
    attrs: &[Attribute],
) -> Result<(Ident, Ident, Option<Ident>), TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("aggregate") {
            let nested = match &attr.meta {
                Meta::List(list) => &list.tokens,
                _ => continue,
            };

            // Parse the attribute arguments
            let aggregate_attrs: AggregateAttrs =
                match darling::ast::NestedMeta::parse_meta_list(nested.clone()) {
                    Ok(nested) => match AggregateAttrs::from_list(&nested) {
                        Ok(attrs) => attrs,
                        Err(err) => return Err(err.write_errors().into()),
                    },
                    Err(err) => return Err(err.to_compile_error().into()),
                };

            let id_type = Ident::new(&aggregate_attrs.id, proc_macro2::Span::call_site());
            let event_type = Ident::new(&aggregate_attrs.event, proc_macro2::Span::call_site());
            let error_type = aggregate_attrs
                .error
                .map(|e| Ident::new(&e, proc_macro2::Span::call_site()));

            return Ok((id_type, event_type, error_type));
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing #[aggregate(id = \"...\", event = \"...\")] attribute",
    )
    .to_compile_error()
    .into())
}

/// Extract field names from the struct fields
fn extract_field_names(fields: &Fields) -> Result<(Ident, Ident, Ident), TokenStream> {
    let mut id_field = None;
    let mut version_field = None;
    let mut events_field = None;

    if let Fields::Named(named) = fields {
        for field in &named.named {
            let field_name = field.ident.as_ref().expect("Named field should have ident");

            for attr in &field.attrs {
                if attr.path().is_ident("aggregate_id") {
                    id_field = Some(field_name.clone());
                } else if attr.path().is_ident("aggregate_version") {
                    version_field = Some(field_name.clone());
                } else if attr.path().is_ident("aggregate_events") {
                    events_field = Some(field_name.clone());
                }
            }
        }
    }

    let id_field = id_field.ok_or_else(|| {
        TokenStream::from(
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "Missing #[aggregate_id] field attribute",
            )
            .to_compile_error(),
        )
    })?;

    let version_field = version_field.ok_or_else(|| {
        TokenStream::from(
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "Missing #[aggregate_version] field attribute",
            )
            .to_compile_error(),
        )
    })?;

    let events_field = events_field.ok_or_else(|| {
        TokenStream::from(
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "Missing #[aggregate_events] field attribute",
            )
            .to_compile_error(),
        )
    })?;

    Ok((id_field, version_field, events_field))
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

/// Derive macro for `AggregateState` - generates wrapper aggregate from state-only struct
///
/// Generates a wrapper aggregate with infrastructure fields (version, `pending_events`)
/// while keeping business logic in the state struct.
///
/// # Features
///
///   - Automatic naming: `CounterState` → `CounterAggregate`
///   - Custom wrapper name via `#[aggregate(name = "...")]`
///   - Optional error type (defaults to `()`)
///   - Automatic `Deref`/`DerefMut` for state access
#[allow(clippy::too_many_lines)]
#[proc_macro_derive(AggregateState, attributes(aggregate, aggregate_id))]
pub fn derive_aggregate_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the state struct name and visibility
    let state_name = &input.ident;
    let vis = &input.vis;

    // Parse the #[aggregate(...)] attribute
    let (id_type, event_type, error_type, wrapper_name) =
        match extract_aggregate_state_attrs(&input.attrs, state_name) {
            Ok(attrs) => attrs,
            Err(err) => return err,
        };

    // Extract fields to find the ID field
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "AggregateState can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // Find the ID field
    let id_field = match find_id_field(fields) {
        Ok(field) => field,
        Err(err) => return err,
    };

    // Generate the wrapper aggregate struct
    let gen = quote! {
        // Generate the wrapper aggregate
        // Note: Derive Serialize/Deserialize to support snapshotting
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        #vis struct #wrapper_name {
            state: #state_name,
            version: event_sauce_core::Version,
            pending_events: Vec<#event_type>,
        }

        impl #wrapper_name {
            /// Creates a new aggregate with default state
            pub fn new() -> Self
            where
                #state_name: Default,
            {
                Self {
                    state: #state_name::default(),
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }

            /// Creates a new aggregate from an existing state
            pub fn from_state(state: #state_name) -> Self {
                Self {
                    state,
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }

            /// Returns a reference to the internal state
            pub fn state(&self) -> &#state_name {
                &self.state
            }

            /// Returns a mutable reference to the internal state
            pub fn state_mut(&mut self) -> &mut #state_name {
                &mut self.state
            }

            // NOTE: apply_event is auto-generated by the Event macro
            // when you use #[event(aggregate = "YourAggregate")].
            // The Event macro generates an implementation that includes
            // validation, state application, and post-validation.
        }

        // Implement Default if the state implements Default
        impl Default for #wrapper_name
        where
            #state_name: Default,
        {
            fn default() -> Self {
                Self::new()
            }
        }

        // Implement Deref for transparent field access
        impl std::ops::Deref for #wrapper_name {
            type Target = #state_name;

            fn deref(&self) -> &Self::Target {
                &self.state
            }
        }

        // Implement DerefMut for transparent mutable field access
        impl std::ops::DerefMut for #wrapper_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.state
            }
        }

        // Implement Aggregate trait
        impl event_sauce_core::Aggregate for #wrapper_name {
            type Event = #event_type;
            type Id = #id_type;
            type Error = #error_type;

            fn new(id: Self::Id) -> Self {
                let mut state = #state_name::default();
                state.#id_field = id;
                Self {
                    state,
                    version: event_sauce_core::Version::initial(),
                    pending_events: Vec::new(),
                }
            }

            fn aggregate_id(&self) -> &Self::Id {
                &self.state.#id_field
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
        }
    };

    gen.into()
}

/// Extract aggregate state attributes and generate wrapper name
fn extract_aggregate_state_attrs(
    attrs: &[Attribute],
    state_name: &Ident,
) -> Result<(Ident, Ident, proc_macro2::TokenStream, Ident), TokenStream> {
    // Extract aggregate attrs using existing function
    let (id_type, event_type, error_type_opt) = extract_aggregate_attrs(attrs)?;

    // Generate wrapper name from state name
    let state_name_str = state_name.to_string();
    let wrapper_name_str = if let Some(name_without_state) = state_name_str.strip_suffix("State") {
        format!("{name_without_state}Aggregate")
    } else {
        format!("{state_name_str}Aggregate")
    };

    // Check if name override is provided
    for attr in attrs {
        if attr.path().is_ident("aggregate") {
            let nested = match &attr.meta {
                Meta::List(list) => &list.tokens,
                _ => continue,
            };

            if let Ok(nested_meta) = darling::ast::NestedMeta::parse_meta_list(nested.clone()) {
                if let Ok(aggregate_attrs) = AggregateAttrs::from_list(&nested_meta) {
                    if let Some(custom_name) = aggregate_attrs.name {
                        let wrapper_name = Ident::new(&custom_name, proc_macro2::Span::call_site());
                        let error_type = if let Some(ident) = error_type_opt {
                            quote! { #ident }
                        } else {
                            quote! { () }
                        };
                        return Ok((id_type, event_type, error_type, wrapper_name));
                    }
                }
            }
        }
    }

    let wrapper_name = Ident::new(&wrapper_name_str, proc_macro2::Span::call_site());
    let error_type = if let Some(ident) = error_type_opt {
        quote! { #ident }
    } else {
        quote! { () }
    };

    Ok((id_type, event_type, error_type, wrapper_name))
}

/// Find the ID field marked with `#[aggregate_id]`
fn find_id_field(fields: &Fields) -> Result<Ident, TokenStream> {
    if let Fields::Named(named) = fields {
        for field in &named.named {
            let field_name = field.ident.as_ref().expect("Named field should have ident");

            for attr in &field.attrs {
                if attr.path().is_ident("aggregate_id") {
                    return Ok(field_name.clone());
                }
            }
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing #[aggregate_id] field attribute. State struct must have one field marked with #[aggregate_id]"
    )
    .to_compile_error()
    .into())
}

/// Derive macro for `AggregateId` trait
///
/// This macro automatically implements the `AggregateId` trait and `Display` trait
/// for newtype structs wrapping types that implement `Display`.
///
/// # Examples
///
/// ```ignore
/// use uuid::Uuid;
/// use event_sauce_macros::AggregateId;
///
/// #[derive(AggregateId, Debug, Clone, PartialEq, Eq, Hash)]
/// struct UserId(Uuid);
///
/// // Now UserId implements AggregateId and Display automatically
/// let id = UserId(Uuid::new_v4());
/// println!("{}", id); // Displays the UUID
/// ```
///
/// # Requirements
///
/// The macro works with:
/// - Tuple structs with a single field (e.g., `struct Id(Uuid)`)
/// - The inner type must implement `Display` for automatic Display implementation
/// - The struct must implement `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash`
///
/// # Panics
///
/// Will fail to compile if the struct is not a tuple struct with exactly one field.
#[proc_macro_derive(AggregateId)]
pub fn derive_aggregate_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Check that this is a tuple struct with one field
    match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                // Valid: tuple struct with one field
            }
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "AggregateId can only be derived for tuple structs with exactly one field (e.g., struct Id(Uuid))"
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "AggregateId can only be derived for structs")
                .to_compile_error()
                .into();
        }
    }

    // Generate the implementation
    let gen = quote! {
        impl event_sauce_core::AggregateId for #name {
            fn to_uuid(&self) -> uuid::Uuid {
                self.0
            }
        }

        // Auto-implement Display by delegating to the inner type
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
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
