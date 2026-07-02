//! Canonical Form C (Nexus Sync Piece 1, S535 spec §2).
//!
//! ONE byte form for `.md` note content, implemented exactly twice fleet-wide:
//! here (Rust daemon) and in the server's `sync_routes_p1.py` (Python).
//! Divergence between the two IS a new unstable-hash family member, so both
//! suites load the SAME vendored vector file and pin its sha256.
//!
//! Form C (spec §2.2):
//! 1. Strip ALL leading U+FEFF (BOM) characters (lstrip-all, so double-BOM is
//!    still a fixpoint). Interior U+FEFF is content and is preserved.
//! 2. Universal newline normalization: replace "\r\n" -> "\n" THEN "\r" -> "\n",
//!    in that order. Post-condition: no 0x0D byte survives, which makes C
//!    idempotent BY CONSTRUCTION (the B3' fixpoint resolution; a single-pass
//!    "\r\n"->"\n" maps "\r\r\n" to "\r\n" and is NOT a fixpoint).
//! 3. Trailing newline is NOT normalized (neither added nor removed).
//! 4. NO NFC normalization of content (dropped per the round-2 verdict).
//!
//! Preconditions enforced here as errors (the shared vector suite requires
//! error outcomes from this same entry point): strict UTF-8 (never lossy) and
//! NUL rejection.

use thiserror::Error;

/// Why content cannot be canonicalized. Both variants are boundary rejections
/// (spec §2.2 hard preconditions); the shared vector suite exercises them
/// through this same entry point (vectors `non_utf8` and `nul`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CanonicalizeError {
    #[error("content is not valid UTF-8 (strict decode is a Form C precondition; lossy decode is banned)")]
    NonUtf8,
    #[error("content contains a NUL byte (rejected at every sync boundary)")]
    ContainsNul,
}

/// Apply Canonical Form C to already-decoded text. See module docs for the
/// normative definition. Idempotent by construction: after the composite
/// newline replacement no `\r` survives, and lstrip-all leaves no leading BOM.
pub fn canonicalize(text: &str) -> Result<String, CanonicalizeError> {
    if text.contains('\0') {
        return Err(CanonicalizeError::ContainsNul);
    }
    // 1. Strip ALL leading U+FEFF (not just one) so double-BOM is a fixpoint.
    //    Interior U+FEFF is preserved (it is content).
    let no_bom = text.trim_start_matches('\u{feff}');
    // 2. Universal newlines: "\r\n" -> "\n" FIRST, then bare "\r" -> "\n".
    //    Order matters: this maps "\r\r\n" to "\n\n" in ONE application.
    Ok(no_bom.replace("\r\n", "\n").replace('\r', "\n"))
}

/// Strict-decode raw bytes, then apply Form C. The decode NEVER falls back to
/// lossy replacement — a non-UTF-8 input is an error, mirroring the server's
/// 422 boundary (spec §2.2).
pub fn canonicalize_bytes(input: &[u8]) -> Result<String, CanonicalizeError> {
    let text = std::str::from_utf8(input).map_err(|_| CanonicalizeError::NonUtf8)?;
    canonicalize(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;
    use proptest::prelude::*;
    use sha2::{Digest, Sha256};

    /// Pinned sha256 of the vendored cross-language vector file. The server
    /// repo pins the SAME constant over its byte-identical copy, so silent
    /// drift between the two copies fails tests in BOTH repos (spec §2.3).
    const VECTOR_FILE_SHA256: &str =
        "b70b5415c1235c626fdc4ba634a5182d192d777e9a0beb829acb5707f117478a";

    fn vector_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/vectors/canonical_form_c_vectors.json")
    }

    #[derive(serde::Deserialize)]
    struct VectorFile {
        vectors: Vec<Vector>,
    }

    #[derive(serde::Deserialize)]
    struct Vector {
        name: String,
        input_b64: String,
        #[serde(default)]
        expect_b64: Option<String>,
        #[serde(default)]
        error: bool,
    }

    #[test]
    fn test_canonical_form_c_vectors_rust() {
        let raw = std::fs::read(vector_path()).expect("vendored vector file must exist");
        let actual_sha = hex::encode(Sha256::digest(&raw));
        assert_eq!(
            actual_sha, VECTOR_FILE_SHA256,
            "vendored vector file drifted from the pinned cross-language copy"
        );

        let vf: VectorFile = serde_json::from_slice(&raw).expect("vector file must parse");
        assert_eq!(vf.vectors.len(), 15, "the normative list has 15 vectors");

        for v in &vf.vectors {
            let input = B64.decode(&v.input_b64).expect("input_b64 must decode");
            match canonicalize_bytes(&input) {
                Ok(out) => {
                    assert!(!v.error, "vector {}: expected an error, got Ok", v.name);
                    let expect_b64 = v
                        .expect_b64
                        .as_ref()
                        .unwrap_or_else(|| panic!("vector {}: missing expect_b64", v.name));
                    let expect = B64.decode(expect_b64).expect("expect_b64 must decode");
                    assert_eq!(
                        out.as_bytes(),
                        &expect[..],
                        "vector {}: wrong canonical bytes",
                        v.name
                    );
                    // Every non-error vector additionally asserts the fixpoint
                    // property: canonicalize(expected) == expected (spec §2.3).
                    let again = canonicalize(&out).expect("canonical output must re-canonicalize");
                    assert_eq!(
                        again, out,
                        "vector {}: expected form is not a fixpoint",
                        v.name
                    );
                }
                Err(e) => {
                    assert!(v.error, "vector {}: unexpected error {:?}", v.name, e);
                }
            }
        }
    }

    proptest! {
        /// Idempotency + no-CR property over arbitrary valid-UTF-8 strings
        /// (spec D2 acceptance: test_canonicalize_idempotent_property_rust).
        #[test]
        fn test_canonicalize_idempotent_property_rust(s in any::<String>()) {
            match canonicalize(&s) {
                Ok(c) => {
                    prop_assert!(!c.contains('\r'), "no 0x0D byte may survive");
                    let c2 = canonicalize(&c).expect("canonical output re-canonicalizes");
                    prop_assert_eq!(c2, c, "canonicalize must be idempotent");
                }
                Err(CanonicalizeError::ContainsNul) => {
                    prop_assert!(s.contains('\0'), "ContainsNul only for NUL inputs");
                }
                Err(CanonicalizeError::NonUtf8) => {
                    // Unreachable: &str input is always valid UTF-8.
                    prop_assert!(false, "NonUtf8 impossible for &str input");
                }
            }
        }
    }

    /// Strict-decode precondition: canonicalize_bytes rejects non-UTF-8
    /// instead of ever decoding lossily (errors="replace" is banned on every
    /// sync path, spec §2.2).
    #[test]
    fn test_canonicalize_bytes_rejects_non_utf8() {
        assert_eq!(
            canonicalize_bytes(&[0xff, 0xfe, 0x41]),
            Err(CanonicalizeError::NonUtf8)
        );
    }
}
