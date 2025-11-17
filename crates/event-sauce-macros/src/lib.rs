//! # event-sauce-macros
//!
//! Derive macros for event-sauce to reduce boilerplate.
//!
//! Provides: #[derive(Aggregate)], #[derive(Event)], #[derive(Projection)]

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Attribute, Meta, Ident};
use darling::FromMeta;

/// Attributes for the #[aggregate(...)] container attribute
#[derive(Debug, FromMeta)]
struct AggregateAttrs {
    id: String,
    event: String,
    error: String,
}

/// Derive macro for Aggregate trait
#[proc_macro_derive(Aggregate, attributes(aggregate, aggregate_id, aggregate_version, aggregate_events))]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the struct name
    let name = &input.ident;

    // Parse the #[aggregate(...)] attribute
    let attrs = extract_aggregate_attrs(&input.attrs);
    let (id_type, event_type, error_type) = match attrs {
        Ok((id, event, error)) => (id, event, error),
        Err(err) => return err,
    };

    // Extract field information
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "Aggregate can only be derived for structs"
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

    // Generate the implementation
    let gen = quote! {
        impl event_sauce_core::Aggregate for #name {
            type Event = #event_type;
            type Id = #id_type;
            type Error = #error_type;

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

            fn apply<E: Into<Self::Event>>(&mut self, event: E) {
                let event = event.into();
                self.apply_internal(&event);
                self.#events_field.push(event);
            }

            fn apply_internal(&mut self, event: &Self::Event) {
                self.apply_event(event);
                self.#version_field = self.#version_field.next();
            }
        }
    };

    gen.into()
}

/// Extract aggregate attributes from the #[aggregate(...)] attribute
fn extract_aggregate_attrs(attrs: &[Attribute]) -> Result<(Ident, Ident, Ident), TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("aggregate") {
            let nested = match &attr.meta {
                Meta::List(list) => &list.tokens,
                _ => continue,
            };

            // Parse the attribute arguments
            let aggregate_attrs: AggregateAttrs = match darling::ast::NestedMeta::parse_meta_list(nested.clone()) {
                Ok(nested) => match AggregateAttrs::from_list(&nested) {
                    Ok(attrs) => attrs,
                    Err(err) => return Err(err.write_errors().into()),
                },
                Err(err) => return Err(err.to_compile_error().into()),
            };

            let id_type = Ident::new(&aggregate_attrs.id, proc_macro2::Span::call_site());
            let event_type = Ident::new(&aggregate_attrs.event, proc_macro2::Span::call_site());
            let error_type = Ident::new(&aggregate_attrs.error, proc_macro2::Span::call_site());

            return Ok((id_type, event_type, error_type));
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing #[aggregate(id = \"...\", event = \"...\", error = \"...\")] attribute"
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
        TokenStream::from(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Missing #[aggregate_id] field attribute"
        )
        .to_compile_error())
    })?;

    let version_field = version_field.ok_or_else(|| {
        TokenStream::from(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Missing #[aggregate_version] field attribute"
        )
        .to_compile_error())
    })?;

    let events_field = events_field.ok_or_else(|| {
        TokenStream::from(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Missing #[aggregate_events] field attribute"
        )
        .to_compile_error())
    })?;

    Ok((id_field, version_field, events_field))
}

/// Attributes for the #[event(...)] container attribute
#[derive(Debug, FromMeta)]
struct EventAttrs {
    version: i32,
    #[darling(default)]
    type_prefix: Option<String>,
}

/// Derive macro for Event trait
#[proc_macro_derive(Event, attributes(event))]
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
            return syn::Error::new_spanned(
                &input.ident,
                "Event can only be derived for enums"
            )
            .to_compile_error()
            .into();
        }
    };

    // Generate event_type match arms
    let type_prefix = attrs.type_prefix.clone()
        .unwrap_or_else(|| {
            // Extract the base name by removing "Event" suffix if present
            let name_str = name.to_string();
            name_str.strip_suffix("Event")
                .map(std::string::ToString::to_string)
                .unwrap_or(name_str)
        });

    let event_type_arms = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let variant_str = variant_name.to_string();
        let event_type_name = format!("{type_prefix}{variant_str}");

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

    // Generate the implementation
    let gen = quote! {
        impl event_sauce_core::DomainEvent for #name {
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

        // Generate Into implementations for each variant
        #(#into_impls)*
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
            let event_attrs: EventAttrs = match darling::ast::NestedMeta::parse_meta_list(nested.clone()) {
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
        "Missing #[event(version = ...)] attribute"
    )
    .to_compile_error()
    .into())
}
