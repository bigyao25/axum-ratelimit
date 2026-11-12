use axum_ratelimit::{ConfigError, RateLimitConfig, RateLimitLayer, store::MemoryStore};
use std::time::Duration;

#[test]
fn defaults_and_builders() {
    let defaults = RateLimitConfig::default();
    assert_eq!(defaults.namespace, "default");
    assert_eq!(defaults.max_requests, 10);
    assert_eq!(defaults.window_secs, Duration::from_secs(100));
    let config = defaults.namespace("login").max_requests(5).window_secs(60);
    assert_eq!(config.namespace, "login");
    assert_eq!(config.max_requests, 5);
    assert_eq!(config.window_secs, Duration::from_secs(60));
    assert!(config.validate().is_ok());
}

#[test]
fn zero_quota_is_valid_but_zero_window_is_not() {
    assert!(
        RateLimitConfig::default()
            .max_requests(0)
            .validate()
            .is_ok()
    );
    assert_eq!(
        RateLimitConfig::default().window_secs(0).validate(),
        Err(ConfigError::ZeroWindow)
    );
}

#[test]
fn fractional_windows_preserve_duration() {
    let config = RateLimitConfig::default().window(Duration::from_nanos(1));
    assert_eq!(config.window_secs, Duration::from_nanos(1));
    assert!(config.validate().is_ok());
}

#[test]
fn exact_redis_numeric_boundary_including_round_up() {
    let max = (1_u64 << 53) - 1;
    assert!(
        RateLimitConfig::default()
            .window(Duration::from_micros(max))
            .validate()
            .is_ok()
    );
    assert_eq!(
        RateLimitConfig::default()
            .window(Duration::from_micros(max) + Duration::from_nanos(1))
            .validate(),
        Err(ConfigError::WindowTooLarge)
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn limit_numeric_boundary() {
    assert!(
        RateLimitConfig::default()
            .max_requests((1_usize << 53) - 1)
            .validate()
            .is_ok()
    );
    assert_eq!(
        RateLimitConfig::default()
            .max_requests(1_usize << 53)
            .validate(),
        Err(ConfigError::LimitTooLarge)
    );
}

#[test]
fn fallible_constructor_reports_invalid_settings() {
    let result = RateLimitLayer::try_new(
        RateLimitConfig::default().window_secs(0),
        MemoryStore::new(),
    );
    assert!(matches!(result, Err(ConfigError::ZeroWindow)));
}

#[test]
#[should_panic(expected = "invalid rate limit configuration")]
fn infallible_constructor_panics_for_invalid_settings() {
    let _ = RateLimitLayer::new(
        RateLimitConfig::default().window_secs(0),
        MemoryStore::new(),
    );
}

#[test]
fn layer_and_store_are_send_sync_and_layer_does_not_require_clone_store() {
    fn send_sync<T: Send + Sync>() {}
    fn cloneable<T: Clone>() {}
    send_sync::<RateLimitLayer>();
    send_sync::<MemoryStore>();
    cloneable::<RateLimitLayer>();
    let layer = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new());
    assert_eq!(layer.clone().config(), layer.config());
}
