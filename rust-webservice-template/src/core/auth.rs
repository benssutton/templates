//! Authentication seam: HS256 bearer JWTs, off by default
//! (`AUTH_ENABLED=false`). When on, every route needs
//! `Authorization: Bearer <jwt>` except the orchestrator probes
//! (`/health/live`, `/health/ready`); CORS preflights are answered before
//! this layer runs. A failure is 401 `{"detail": "Not authenticated"}` with
//! `WWW-Authenticate: Bearer`. Verified claims are added to the request
//! extensions as `Claims` for handlers that need the caller's identity.

use crate::error::AppError;
use crate::settings::Settings;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Paths that stay open so orchestrators can probe without a token.
pub const PUBLIC_PATHS: [&str; 2] = ["/health/live", "/health/ready"];

/// The registered claims the seam checks; extend with your own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Option<String>,
    pub exp: u64,
    pub iss: Option<String>,
    pub aud: Option<serde_json::Value>,
}

pub struct Authenticator {
    key: DecodingKey,
    validation: Validation,
}

impl Authenticator {
    /// `None` when auth is off. An unusable key is an error, never "auth off":
    /// the seam fails closed.
    pub fn from_settings(s: &Settings) -> anyhow::Result<Option<Arc<Self>>> {
        if !s.auth_enabled {
            return Ok(None);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp"]);
        validation.leeway = 0;
        if let Some(iss) = &s.auth_jwt_issuer {
            validation.set_issuer(&[iss]);
            validation.set_required_spec_claims(&["exp", "iss"]);
        }
        match &s.auth_jwt_audience {
            Some(aud) => validation.set_audience(&[aud]),
            None => validation.validate_aud = false,
        }
        let key = DecodingKey::from_secret(s.auth_jwt_secret.expose_secret().as_bytes())
            .map_err(|e| anyhow::anyhow!("AUTH_JWT_SECRET is not a usable HMAC key: {e}"))?;
        Ok(Some(Arc::new(Self { key, validation })))
    }

    pub fn verify(&self, token: &str) -> Result<Claims, AppError> {
        jsonwebtoken::decode::<Claims>(token, &self.key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| AppError::Unauthorized(e.to_string()))
    }
}

fn bearer(req: &Request) -> Option<&str> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

pub async fn middleware(
    State(auth): State<Arc<Authenticator>>,
    mut req: Request,
    next: Next,
) -> Response {
    if PUBLIC_PATHS.contains(&req.uri().path()) {
        return next.run(req).await;
    }
    let Some(token) = bearer(&req) else {
        return AppError::Unauthorized("missing bearer token".into()).into_response();
    };
    match auth.verify(token) {
        Ok(claims) => {
            req.extensions_mut().insert(claims);
            next.run(req).await
        }
        Err(err) => err.into_response(),
    }
}
