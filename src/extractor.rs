//! Synchronous identity extraction from request parts. The body is never read.

use axum::{
    extract::ConnectInfo,
    response::{IntoResponse, Response},
};
use http::{StatusCode, request::Parts};
use std::{fmt, net::SocketAddr};

/// Identity extraction failures. No untrusted identifier is included in responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError {
    /// Server setup error: `into_make_service_with_connect_info` was not used.
    MissingConnectInfo,
    /// An application-supplied extractor could not find a usable identity.
    MissingIdentity,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MissingConnectInfo => "rate limit connection information unavailable",
            Self::MissingIdentity => "rate limit identity unavailable",
        })
    }
}

impl std::error::Error for IdentityError {}

impl IntoResponse for IdentityError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::MissingConnectInfo => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MissingIdentity => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}

/// Extract a non-secret, application-validated identifier without reading the body.
///
/// Implementations must be fast and non-blocking. Run authentication before this
/// layer and extract its trusted identity from request extensions. Do not use raw
/// API keys or trust client-supplied forwarding headers without validating proxies.
pub trait KeyExtractor: Send + Sync {
    fn extract(&self, parts: &Parts) -> Result<String, IdentityError>;
}

impl<F> KeyExtractor for F
where
    F: Fn(&Parts) -> Result<String, IdentityError> + Send + Sync,
{
    fn extract(&self, parts: &Parts) -> Result<String, IdentityError> {
        self(parts)
    }
}

/// Uses `ConnectInfo<SocketAddr>`; ignores source ports and forwarding headers.
///
/// IPv4-mapped IPv6 addresses are canonicalized to the same key as IPv4.
/// Missing connection information is a server configuration error (HTTP 500),
/// not a silent shared `unknown` bucket and not a fail-open decision.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerIpKeyExtractor;

impl KeyExtractor for PeerIpKeyExtractor {
    fn extract(&self, parts: &Parts) -> Result<String, IdentityError> {
        parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip().to_canonical().to_string())
            .ok_or(IdentityError::MissingConnectInfo)
    }
}
