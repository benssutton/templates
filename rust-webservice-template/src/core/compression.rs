//! zstd and gzip content coding, negotiated from `Accept-Encoding` and
//! `Content-Encoding` (RFC 9110).
//!
//! `negotiate` runs outside tower-http's `CompressionLayer` and
//! `RequestDecompressionLayer`. It answers strict mode's 406 itself, so that
//! response is never compressed; refuses request codings other than zstd and
//! gzip with 415; and rewrites `Accept-Encoding` to the winning coding
//! alone so the compression layer follows this module's rules (a `*` entry
//! counts; HEAD responses are never encoded; a missing header means
//! identity). While compression is enabled it adds `Vary: Accept-Encoding`
//! to every text-based response, compressed or not, so caches keep the
//! variants apart.

use crate::settings::Settings;
use axum::Json;
use axum::body::HttpBody;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, Response as HttpResponse, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use tower_http::compression::predicate::Predicate;
use tower_http::compression::{CompressionLayer, CompressionLevel};

/// Marks a request whose body arrived zstd- or gzip-encoded, so a failed
/// body read can be reported as a malformed encoding.
#[derive(Clone, Copy, Debug)]
pub struct CompressedRequest;

/// q-values for the two codings this service offers.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Weights {
    zstd: f32,
    gzip: f32,
}

impl Weights {
    fn accepts_zstd(self) -> bool {
        self.zstd > 0.0
    }

    /// The `Accept-Encoding` the compression layer sees: only the winning
    /// coding, so its own q-value handling cannot pick differently. zstd wins
    /// ties; `None` when neither coding is acceptable.
    fn winning_coding(self) -> Option<HeaderValue> {
        if self.zstd > 0.0 && self.zstd >= self.gzip {
            Some(HeaderValue::from_static("zstd"))
        } else if self.gzip > 0.0 {
            Some(HeaderValue::from_static("gzip"))
        } else {
            None
        }
    }
}

fn q(params: &str) -> f32 {
    for param in params.split(';') {
        if let Some((key, value)) = param.split_once('=')
            && key.trim().eq_ignore_ascii_case("q")
        {
            return value
                .trim()
                .parse::<f32>()
                .ok()
                .filter(|q| q.is_finite())
                .map_or(0.0, |q| q.clamp(0.0, 1.0));
        }
    }
    1.0
}

/// `None` when the request has no `Accept-Encoding`. An explicit entry
/// beats `*`; a later duplicate overrides an earlier one.
fn weights(headers: &HeaderMap) -> Option<Weights> {
    let mut values = headers.get_all(header::ACCEPT_ENCODING).iter().peekable();
    values.peek()?;
    let (mut zstd, mut gzip, mut star) = (None, None, None);
    for value in values {
        let Ok(value) = value.to_str() else { continue };
        for item in value.split(',') {
            let (name, params) = item.split_once(';').unwrap_or((item, ""));
            let slot = match name.trim().to_ascii_lowercase().as_str() {
                "zstd" => &mut zstd,
                "gzip" => &mut gzip,
                "*" => &mut star,
                _ => continue,
            };
            *slot = Some(q(params));
        }
    }
    let star = star.unwrap_or(0.0);
    Some(Weights {
        zstd: zstd.unwrap_or(star),
        gzip: gzip.unwrap_or(star),
    })
}

fn is_text_based(content_type: &str) -> bool {
    let media = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    // Compressing an event stream would hold events back in the encoder.
    media != "text/event-stream"
        && (media.starts_with("text/")
            || media == "application/json"
            || media == "application/xml"
            || media.ends_with("+json")
            || media.ends_with("+xml"))
}

fn is_text(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(is_text_based)
}

fn exempt(path: &str) -> bool {
    matches!(path, "/health/live" | "/health/ready" | "/metrics" | "/mcp")
        || path.starts_with("/mcp/")
}

enum RequestCoding {
    Identity,
    Encoded(&'static str),
    Unsupported,
}

fn request_coding(headers: &HeaderMap) -> RequestCoding {
    let mut values = headers.get_all(header::CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return RequestCoding::Identity;
    };
    if values.next().is_some() {
        return RequestCoding::Unsupported; // stacked across header lines
    }
    match value.to_str().map(|v| v.trim().to_ascii_lowercase()) {
        Ok(v) if v.is_empty() || v == "identity" => RequestCoding::Identity,
        Ok(v) if v == "zstd" => RequestCoding::Encoded("zstd"),
        Ok(v) if v == "gzip" => RequestCoding::Encoded("gzip"),
        _ => RequestCoding::Unsupported,
    }
}

fn detail(status: StatusCode, detail: &str) -> Response {
    (status, Json(json!({ "detail": detail }))).into_response()
}

fn varies_on_accept_encoding(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::VARY)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("accept-encoding"))
}

fn with_vary(mut response: Response) -> Response {
    if is_text(response.headers()) && !varies_on_accept_encoding(response.headers()) {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("accept-encoding"));
    }
    response
}

/// State for `negotiate`.
#[derive(Clone, Copy, Debug)]
pub struct Negotiation {
    enabled: bool,
    require_zstd: bool,
}

impl Negotiation {
    pub fn new(settings: &Settings) -> Self {
        Self {
            enabled: settings.compression_enabled,
            require_zstd: settings.compression_require_zstd,
        }
    }

    /// Responses only vary by `Accept-Encoding` when they can be encoded.
    fn vary(self, response: Response) -> Response {
        if self.enabled {
            with_vary(response)
        } else {
            response
        }
    }
}

pub async fn negotiate(
    State(policy): State<Negotiation>,
    mut req: Request,
    next: Next,
) -> Response {
    let weights = weights(req.headers());
    if policy.require_zstd
        && req.method() != Method::OPTIONS
        && !exempt(req.uri().path())
        && !weights.is_some_and(Weights::accepts_zstd)
    {
        return policy.vary(detail(
            StatusCode::NOT_ACCEPTABLE,
            "Accept-Encoding must allow zstd",
        ));
    }
    match request_coding(req.headers()) {
        RequestCoding::Unsupported => {
            let mut response = detail(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Unsupported Content-Encoding",
            );
            response.headers_mut().insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("zstd, gzip"),
            );
            return policy.vary(response);
        }
        RequestCoding::Encoded(coding) => {
            // Normalised, because the decompression layer matches exact bytes.
            req.headers_mut()
                .insert(header::CONTENT_ENCODING, HeaderValue::from_static(coding));
            req.extensions_mut().insert(CompressedRequest);
        }
        RequestCoding::Identity => {
            req.headers_mut().remove(header::CONTENT_ENCODING);
        }
    }
    let accepted = if req.method() == Method::HEAD {
        None
    } else {
        weights.and_then(Weights::winning_coding)
    };
    // Only zstd and gzip are compiled in, so any other coding the client
    // listed is dropped along with the losing one.
    req.headers_mut().remove(header::ACCEPT_ENCODING);
    if let Some(value) = accepted {
        req.headers_mut().insert(header::ACCEPT_ENCODING, value);
    }
    policy.vary(next.run(req).await)
}

/// Which responses the compression layer may encode: none while compression
/// is disabled (so the layer adds no `Vary` either), otherwise text-based,
/// not 204 or 304, and at least `min_bytes` when the size is known up front
/// (a streamed body of unknown size is compressed). A known-empty body is
/// never compressed, even in strict mode: encoding it would only add a frame.
#[derive(Clone, Copy, Debug)]
pub struct Compressible {
    enabled: bool,
    min_bytes: u64,
}

impl Predicate for Compressible {
    fn should_compress<B>(&self, response: &HttpResponse<B>) -> bool
    where
        B: HttpBody,
    {
        let status = response.status();
        self.enabled
            && status != StatusCode::NO_CONTENT
            && status != StatusCode::NOT_MODIFIED
            && is_text(response.headers())
            && response
                .body()
                .size_hint()
                .exact()
                .is_none_or(|n| n > 0 && n >= self.min_bytes)
    }
}

/// zstd and gzip only (the other codings are not compiled in). Disabled
/// compression keeps the layer but turns both codings off, so the stack's
/// type does not depend on configuration.
pub fn response_layer(settings: &Settings) -> CompressionLayer<Compressible> {
    let min_bytes = if settings.compression_require_zstd {
        0
    } else {
        settings.compression_min_bytes
    };
    CompressionLayer::new()
        .gzip(settings.compression_enabled)
        .zstd(settings.compression_enabled)
        .quality(CompressionLevel::Precise(settings.compression_zstd_level))
        .compress_when(Compressible {
            enabled: settings.compression_enabled,
            min_bytes,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Path;
    use axum::routing::get;
    use tower::ServiceExt;

    type Pair = Option<(f32, f32)>;

    fn weights_of(value: Option<&str>) -> Pair {
        let mut headers = HeaderMap::new();
        if let Some(v) = value {
            headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_str(v).unwrap());
        }
        weights(&headers).map(|w| (w.zstd, w.gzip))
    }

    #[test]
    fn accept_encoding_weights() {
        let cases: [(Option<&str>, Pair); 13] = [
            (None, None),
            (Some(""), Some((0.0, 0.0))),
            (Some("identity, br"), Some((0.0, 0.0))),
            (Some("zstd"), Some((1.0, 0.0))),
            (Some("gzip;q=0.5"), Some((0.0, 0.5))),
            (Some("*"), Some((1.0, 1.0))),
            (Some("*;q=0.5, gzip"), Some((0.5, 1.0))),
            (Some("*, zstd;q=0"), Some((0.0, 1.0))),
            (Some("zstd;q=abc"), Some((0.0, 0.0))),
            (Some("zstd;q=nan"), Some((0.0, 0.0))),
            (Some("zstd;q=2"), Some((1.0, 0.0))),
            (Some("ZSTD ; Q=0.1"), Some((0.1, 0.0))),
            (Some("zstd;q=0, zstd"), Some((1.0, 0.0))),
        ];
        for (input, expected) in cases {
            assert_eq!(weights_of(input), expected, "{input:?}");
        }
    }

    #[test]
    fn rewritten_header_names_only_the_winner() {
        let cases: [(&str, Option<&str>); 8] = [
            ("zstd", Some("zstd")),
            ("gzip", Some("gzip")),
            ("zstd;q=0.5, gzip", Some("gzip")),
            ("zstd;q=0.5, gzip;q=0.5", Some("zstd")),
            // Distinct q-values must not round into a tie.
            ("zstd;q=0.9996, gzip", Some("gzip")),
            ("zstd;q=0.0001", Some("zstd")),
            ("*, zstd;q=0", Some("gzip")),
            ("identity", None),
        ];
        for (accept, expected) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static(accept));
            let winner = weights(&headers).and_then(Weights::winning_coding);
            assert_eq!(
                winner.as_ref().map(|v| v.to_str().unwrap()),
                expected,
                "{accept}"
            );
        }
    }

    #[test]
    fn text_based_content_types() {
        for ct in [
            "application/json",
            "application/json; charset=utf-8",
            "text/plain",
            "text/html",
            "application/problem+json",
            "application/xml",
            "image/svg+xml",
        ] {
            assert!(is_text_based(ct), "{ct}");
        }
        for ct in [
            "text/event-stream",
            "application/octet-stream",
            "application/vnd.apache.arrow.stream",
            "image/png",
            "",
        ] {
            assert!(!is_text_based(ct), "{ct}");
        }
    }

    async fn response_for(settings: &Settings, accept: &str, n: usize) -> Response {
        let app = Router::new()
            .route(
                "/n/{n}",
                get(|Path(n): Path<usize>| async move {
                    ([(header::CONTENT_TYPE, "application/json")], "a".repeat(n))
                }),
            )
            .layer(response_layer(settings));
        let request = axum::http::Request::get(format!("/n/{n}"))
            .header(header::ACCEPT_ENCODING, accept)
            .body(Body::empty())
            .unwrap();
        app.oneshot(request).await.unwrap().map(Body::new)
    }

    async fn encoding_for(min_bytes: u64, n: usize) -> Option<String> {
        let settings = Settings {
            compression_min_bytes: min_bytes,
            ..Settings::default()
        };
        response_for(&settings, "zstd", n)
            .await
            .headers()
            .get(header::CONTENT_ENCODING)
            .map(|v| v.to_str().unwrap().to_owned())
    }

    #[tokio::test]
    async fn threshold_boundary() {
        assert_eq!(encoding_for(1024, 1023).await, None);
        assert_eq!(encoding_for(1024, 1024).await.as_deref(), Some("zstd"));
        assert_eq!(encoding_for(0, 1).await.as_deref(), Some("zstd"));
    }

    #[tokio::test]
    async fn strict_mode_leaves_an_empty_body_uncompressed() {
        let settings = Settings {
            compression_require_zstd: true,
            ..Settings::default()
        };
        let response = response_for(&settings, "zstd", 0).await;
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn disabled_layer_neither_encodes_nor_varies() {
        let settings = Settings {
            compression_enabled: false,
            compression_min_bytes: 0,
            ..Settings::default()
        };
        let response = response_for(&settings, "zstd", 2048).await;
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert!(response.headers().get(header::VARY).is_none());
    }

    #[tokio::test]
    async fn gzip_still_works_with_a_zstd_only_level() {
        use std::io::Read;
        let settings = Settings {
            compression_zstd_level: 19,
            ..Settings::default()
        };
        let response = response_for(&settings, "gzip", 2048).await;
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut out = String::new();
        flate2::read::GzDecoder::new(body.as_ref())
            .read_to_string(&mut out)
            .unwrap();
        assert_eq!(out, "a".repeat(2048));
    }
}
