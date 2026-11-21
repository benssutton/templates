//! Request ids, on tower-http's `SetRequestId` / `PropagateRequestId`.
//!
//! An inbound id is adopted only if it is 1-128 chars of `[A-Za-z0-9._-]`;
//! anything else is removed before `SetRequestId` runs, so a fresh id is
//! generated. That keeps log lines and response headers free of attacker
//! controlled bytes. The ingest task opens its own span per delivery with
//! `new_id()`.

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use tower_http::request_id::{MakeRequestId, RequestId};

/// Longest inbound id adopted.
pub const MAX_LEN: usize = 128;

/// 32 lowercase hex chars (a v4 UUID without dashes).
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub fn is_acceptable(id: &[u8]) -> bool {
    (1..=MAX_LEN).contains(&id.len())
        && id
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Generates `new_id()` for requests without an acceptable id.
#[derive(Debug, Clone, Copy, Default)]
pub struct NewRequestId;

impl MakeRequestId for NewRequestId {
    fn make_request_id<B>(&mut self, _: &axum::http::Request<B>) -> Option<RequestId> {
        HeaderValue::from_str(&new_id()).ok().map(RequestId::new)
    }
}

/// Drop an unacceptable inbound id so `SetRequestId` replaces it.
pub fn drop_unacceptable(header: &HeaderName, mut req: Request) -> Request {
    let acceptable = req
        .headers()
        .get(header)
        .is_none_or(|v| is_acceptable(v.as_bytes()));
    if !acceptable {
        req.headers_mut().remove(header);
    }
    req
}

/// The request's id as a string ("" if none was set).
pub fn of<B>(req: &axum::http::Request<B>) -> &str {
    req.extensions()
        .get::<RequestId>()
        .and_then(|id| id.header_value().to_str().ok())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_are_32_lowercase_hex_and_acceptable() {
        let id = new_id();
        assert_eq!(id.len(), 32);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert!(is_acceptable(id.as_bytes()));
        assert_ne!(id, new_id());
    }

    #[test]
    fn only_bounded_safe_ids_are_acceptable() {
        assert!(is_acceptable(b"req-1.a_B"));
        assert!(is_acceptable("a".repeat(MAX_LEN).as_bytes()));
        assert!(!is_acceptable(b""));
        assert!(!is_acceptable("a".repeat(MAX_LEN + 1).as_bytes()));
        assert!(!is_acceptable(b"has space"));
        assert!(!is_acceptable(b"semi;colon"));
        assert!(!is_acceptable("caf\u{e9}".as_bytes()));
    }
}
