use axum_ratelimit::{
    RateLimitConfig,
    store::{MemoryStore, RateLimitStore},
};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn quota_and_independent_clients() {
    let store = MemoryStore::new();
    let config = RateLimitConfig::default().max_requests(2);
    for id in ["a", "b"] {
        assert!(!store.is_limited(id, &config).await.unwrap());
        assert!(!store.is_limited(id, &config).await.unwrap());
        assert!(store.is_limited(id, &config).await.unwrap());
    }
}

#[tokio::test]
async fn rules_isolate_namespace_window_and_quota() {
    let store = MemoryStore::new();
    let base = RateLimitConfig::default().max_requests(1);
    assert!(!store.is_limited("id", &base).await.unwrap());
    assert!(store.is_limited("id", &base).await.unwrap());
    for other in [
        base.clone().namespace("different"),
        base.clone().window_secs(99),
        base.clone().max_requests(2),
    ] {
        assert!(!store.is_limited("id", &other).await.unwrap());
    }
}

#[tokio::test]
async fn namespace_and_identifier_delimiters_do_not_collide() {
    let store = MemoryStore::new();
    let a = RateLimitConfig::default().max_requests(1).namespace("a:b");
    let b = RateLimitConfig::default().max_requests(1).namespace("a");
    assert!(!store.is_limited("c", &a).await.unwrap());
    assert!(!store.is_limited("b:c", &b).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shared_store_admits_exact_quota() {
    let store = Arc::new(MemoryStore::new());
    let config = Arc::new(RateLimitConfig::default().max_requests(17));
    let barrier = Arc::new(tokio::sync::Barrier::new(128));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..128 {
        let (store, config, barrier) = (store.clone(), config.clone(), barrier.clone());
        tasks.spawn(async move {
            barrier.wait().await;
            usize::from(!store.is_limited("same", &config).await.unwrap())
        });
    }
    let mut admitted = 0;
    while let Some(result) = tasks.join_next().await {
        admitted += result.unwrap();
    }
    assert_eq!(admitted, 17);
}

#[tokio::test]
async fn detailed_snapshot_and_zero_quota() {
    let store = MemoryStore::new();
    let config = RateLimitConfig::default().max_requests(2);
    let first = store.check("id", &config).await.unwrap();
    assert!(first.allowed);
    assert_eq!(first.metadata.unwrap().remaining, 1);
    assert_eq!(first.metadata.unwrap().retry_after, None);
    assert!(store.check("id", &config).await.unwrap().allowed);
    let denied = store.check("id", &config).await.unwrap();
    assert!(!denied.allowed);
    assert_eq!(denied.metadata.unwrap().remaining, 0);
    assert!(denied.metadata.unwrap().retry_after.unwrap() <= config.window_secs);
    let zero = store.check("id", &config.max_requests(0)).await.unwrap();
    assert!(!zero.allowed);
    assert_eq!(zero.metadata.unwrap().retry_after, None);
}

#[tokio::test]
async fn arc_box_trait_objects_forward_detailed_and_observed_calls() {
    let store: Arc<Box<dyn RateLimitStore>> = Arc::new(Box::new(MemoryStore::new()));
    let config = RateLimitConfig::default().max_requests(1);
    let allowed = store.check_observed("id", &config, true).await.unwrap();
    assert!(allowed.decision.allowed);
    assert_eq!(allowed.decision.metadata.unwrap().remaining, 0);
    assert!(!allowed.fail_open);
    let denied = store.check_observed("id", &config, false).await.unwrap();
    assert!(!denied.decision.allowed);
    assert_eq!(denied.decision.metadata, None);
    assert!(store.is_limited("id", &config).await.unwrap());
}

#[tokio::test]
async fn invalid_configurations_are_errors_in_every_path() {
    let store = MemoryStore::new();
    let config = RateLimitConfig::default().window_secs(0);
    assert!(store.is_limited("id", &config).await.is_err());
    assert!(store.check("id", &config).await.is_err());
    assert!(store.check_observed("id", &config, false).await.is_err());
    assert!(store.check_observed("id", &config, true).await.is_err());
}

#[tokio::test]
async fn expired_buckets_can_be_reclaimed_without_a_background_worker() {
    let store = MemoryStore::new();
    let config = RateLimitConfig::default().window(Duration::from_millis(1));
    assert!(!store.is_limited("id", &config).await.unwrap());
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(store.prune_expired(), 1);
    assert_eq!(store.prune_expired(), 0);
    assert!(!store.is_limited("id", &config).await.unwrap());
}
