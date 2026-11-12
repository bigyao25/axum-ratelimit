//! Framework-independent sliding-window settings.

use std::{fmt, time::Duration};

const MAX_EXACT_INTEGER: u128 = (1_u128 << 53) - 1;

/// Invalid settings. A zero request limit is valid and denies all requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    ZeroWindow,
    WindowTooLarge,
    LimitTooLarge,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroWindow => "the rate limit window must be greater than zero",
            Self::WindowTooLarge => {
                "the rate limit window exceeds the exact Redis microsecond range"
            }
            Self::LimitTooLarge => "the request limit exceeds the exact Redis integer range",
        })
    }
}

impl std::error::Error for ConfigError {}

/// Quota rules, deliberately independent of Axum request/response types.
///
/// Equal namespace, window, limit and client ID share a counter in the same
/// store (or Redis prefix). Use different namespaces for independent routes.
/// Identity extraction and rejection responses are configured on
/// [`crate::RateLimitLayer`], not on the storage configuration.
///
/// ```
/// use axum_ratelimit::RateLimitConfig;
/// let config = RateLimitConfig::default()
///     .namespace("login")
///     .max_requests(5)
///     .window_secs(60);
/// assert!(config.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub namespace: String,
    pub max_requests: usize,
    /// Window duration, including sub-second precision.
    pub window_secs: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_owned(),
            max_requests: 10,
            window_secs: Duration::from_secs(100),
        }
    }
}

impl RateLimitConfig {
    /// Validate the common exact numeric range of the built-in backends.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.window_secs.is_zero() {
            return Err(ConfigError::ZeroWindow);
        }
        if self.window_secs.as_nanos().div_ceil(1_000) > MAX_EXACT_INTEGER {
            return Err(ConfigError::WindowTooLarge);
        }
        if self.max_requests as u128 > MAX_EXACT_INTEGER {
            return Err(ConfigError::LimitTooLarge);
        }
        Ok(())
    }

    pub fn namespace(mut self, value: impl Into<String>) -> Self {
        self.namespace = value.into();
        self
    }

    pub fn max_requests(mut self, value: usize) -> Self {
        self.max_requests = value;
        self
    }

    pub fn window_secs(self, value: u64) -> Self {
        self.window(Duration::from_secs(value))
    }

    /// Set a window without discarding fractional seconds.
    pub fn window(mut self, value: Duration) -> Self {
        self.window_secs = value;
        self
    }
}
