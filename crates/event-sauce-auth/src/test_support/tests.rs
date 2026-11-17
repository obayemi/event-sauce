use super::*;

#[test]
fn a_fake_phc_carries_the_lowercase_hex_sha256_of_the_password() {
    assert_eq!(
        fake_phc(&secret("pw")),
        "$fake$c29tZXNhbHRzYWx0$30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4"
    );
}
