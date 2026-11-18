use super::SnapshotDecl;
use darling::FromMeta;
use quote::quote;

fn parse(meta: proc_macro2::TokenStream) -> darling::Result<SnapshotDecl> {
    SnapshotDecl::from_meta(&syn::parse2(meta).unwrap())
}

fn squeezed(tokens: &proc_macro2::TokenStream) -> String {
    tokens.to_string().split_whitespace().collect()
}

fn expanded(decl: &SnapshotDecl) -> String {
    squeezed(&decl.to_override(&quote!(::core_crate)))
}

#[test]
fn every_parses_its_count() {
    assert!(matches!(
        parse(quote!(snapshots(every = 5))),
        Ok(SnapshotDecl::Every(n)) if n.get() == 5
    ));
}

#[test]
fn every_zero_is_rejected() {
    let err = parse(quote!(snapshots(every = 0))).unwrap_err();
    assert!(err.to_string().contains("greater than 0"));
}

#[test]
fn always_and_never_parse() {
    assert!(matches!(
        parse(quote!(snapshots(always))),
        Ok(SnapshotDecl::Always)
    ));
    assert!(matches!(
        parse(quote!(snapshots(never))),
        Ok(SnapshotDecl::Never)
    ));
}

#[test]
fn with_parses_a_path() {
    let Ok(SnapshotDecl::With(path)) = parse(quote!(snapshots(with = "a::B"))) else {
        panic!("expected With");
    };
    assert_eq!(squeezed(&quote!(#path)), "a::B");
}

#[test]
fn with_rejects_an_invalid_path() {
    assert!(parse(quote!(snapshots(with = "not a path"))).is_err());
}

#[test]
fn malformed_declarations_report_the_usage() {
    for meta in [
        quote!(snapshots),
        quote!(snapshots()),
        quote!(snapshots(always, never)),
        quote!(snapshots(sometimes)),
        quote!(snapshots(always = 1)),
        quote!(snapshots("always")),
    ] {
        let err = parse(meta).unwrap_err();
        assert!(err.to_string().contains("exactly one of"), "{err}");
    }
}

#[test]
fn every_expands_to_a_const_every_n_events() {
    let out = expanded(&SnapshotDecl::Every(7.try_into().unwrap()));
    assert!(out.contains("Some(&const{::core_crate::EveryNEvents::new("));
    assert!(out.contains("NonZeroU32::new(7u32)"));
    assert!(out.contains("fnsnapshot_strategy()"));
}

#[test]
fn always_and_never_expand_to_their_unit_strategies() {
    assert!(expanded(&SnapshotDecl::Always).contains("Some(&::core_crate::AlwaysSnapshot)"));
    assert!(expanded(&SnapshotDecl::Never).contains("Some(&::core_crate::NeverSnapshot)"));
}

#[test]
fn with_expands_to_a_reference_to_the_path() {
    let out = expanded(&SnapshotDecl::With(syn::parse_quote!(my::STRAT)));
    assert!(out.contains("Some(&my::STRAT)"));
    assert!(!out.contains("staticSTRATEGY"));
}
