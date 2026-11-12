//! Optional, backend-agnostic hooks for completed rate-limit checks.
//!
//! Events contain no client identifiers or raw backend errors. Keep namespaces
//! low-cardinality and free of sensitive values. No exporter or task is started.

use crate::store::{RateLimitDecision, RateLimitMetadata};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// The actual storage/admission path, independent of downstream HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitOutcome {
    Allowed,
    Limited,
    StoreError,
    FailOpen,
}

/// Coarse error categories, without backend messages or connection details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitErrorKind {
    Timeout,
    Connection,
    Backend,
    Other,
}

/// Borrowed event emitted once after a completed check and before the handler.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RateLimitEvent<'a> {
    pub namespace: &'a str,
    pub outcome: RateLimitOutcome,
    /// Monotonic elapsed time for the check, excluding the callback and handler.
    pub check_duration: Duration,
    pub max_requests: usize,
    pub window: Duration,
    /// Only metadata already requested for response headers; never queried again.
    pub metadata: Option<RateLimitMetadata>,
    pub error_kind: Option<RateLimitErrorKind>,
}

/// Synchronous observer shared across services.
///
/// Callbacks run after storage locks are released and cannot change admission.
/// Keep them fast and non-blocking: no disk/network I/O, contended mutex waits or
/// blocking channel sends. Prefer atomics or an application-owned bounded queue.
/// Applications own async export, queue-full, backpressure and drop policies.
///
/// After an unwinding panic the observer is disabled. In-flight callbacks can
/// still finish. Panic hooks still run; `panic=abort` and double panics cannot be
/// recovered. Panicking is not a supported event delivery strategy.
pub trait RateLimitObserver: Send + Sync {
    fn on_event(&self, event: &RateLimitEvent<'_>);
}

impl<F> RateLimitObserver for F
where
    F: Fn(&RateLimitEvent<'_>) + Send + Sync,
{
    fn on_event(&self, event: &RateLimitEvent<'_>) {
        self(event);
    }
}

impl<T: RateLimitObserver + ?Sized> RateLimitObserver for Arc<T> {
    fn on_event(&self, event: &RateLimitEvent<'_>) {
        (**self).on_event(event);
    }
}

/// Store extension result. Custom stores normally return `decision.into()`.
/// A fail-open result must admit, omit unreliable metadata and, when known,
/// report a coarse error kind. Only mark real backend failures as fail-open.
#[derive(Debug)]
pub struct ObservedRateLimitDecision {
    pub decision: RateLimitDecision,
    pub fail_open: bool,
    pub error_kind: Option<RateLimitErrorKind>,
}

impl From<RateLimitDecision> for ObservedRateLimitDecision {
    fn from(decision: RateLimitDecision) -> Self {
        Self {
            decision,
            fail_open: false,
            error_kind: None,
        }
    }
}

pub(crate) struct ObserverState {
    observer: Arc<dyn RateLimitObserver>,
    disabled: AtomicBool,
}

impl ObserverState {
    pub(crate) fn new(observer: impl RateLimitObserver + 'static) -> Self {
        Self {
            observer: Arc::new(observer),
            disabled: AtomicBool::new(false),
        }
    }

    fn emit(&self, event: &RateLimitEvent<'_>) {
        if self.disabled.load(Ordering::Acquire) {
            return;
        }
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.observer.on_event(event);
        })) {
            // Do not serialize the hot path. Callbacks already running may finish.
            self.disabled.store(true, Ordering::Release);
            // A payload destructor may panic too. Do not intentionally leak it.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
        }
    }
}

async fn check_observed(
    store: &dyn crate::store::RateLimitStore,
    id: &str,
    config: &crate::config::RateLimitConfig,
    metadata_requested: bool,
    observer: &ObserverState,
) -> Result<RateLimitDecision, crate::store::StoreError> {
    let start = std::time::Instant::now();
    let observed = store.check_observed(id, config, metadata_requested).await;
    let check_duration = start.elapsed();
    let (outcome, metadata, error_kind) = match &observed {
        Ok(value) => (
            if value.fail_open {
                RateLimitOutcome::FailOpen
            } else if value.decision.allowed {
                RateLimitOutcome::Allowed
            } else {
                RateLimitOutcome::Limited
            },
            value.decision.metadata,
            value.error_kind,
        ),
        Err(error) => (RateLimitOutcome::StoreError, None, Some(error.kind())),
    };
    let event = RateLimitEvent {
        namespace: &config.namespace,
        outcome,
        check_duration,
        max_requests: config.max_requests,
        window: config.window_secs,
        metadata,
        error_kind,
    };
    // Observe only completed checks; cancellation is not a completed decision.
    observer.emit(&event);
    observed.map(|value| value.decision)
}

/// Instantiated only when observation is enabled.
pub(crate) struct ObservedStore {
    pub store: Arc<dyn crate::store::RateLimitStore>,
    pub observer: Arc<ObserverState>,
}

impl crate::store::RateLimitStore for ObservedStore {
    fn is_limited<'a>(
        &'a self,
        key: &'a str,
        config: &'a crate::config::RateLimitConfig,
    ) -> futures_util::future::BoxFuture<'a, Result<bool, crate::store::StoreError>> {
        Box::pin(async move {
            check_observed(
                self.store.as_ref(),
                key,
                config,
                false,
                self.observer.as_ref(),
            )
            .await
            .map(|decision| !decision.allowed)
        })
    }

    fn check<'a>(
        &'a self,
        key: &'a str,
        config: &'a crate::config::RateLimitConfig,
    ) -> futures_util::future::BoxFuture<'a, Result<RateLimitDecision, crate::store::StoreError>>
    {
        Box::pin(check_observed(
            self.store.as_ref(),
            key,
            config,
            true,
            self.observer.as_ref(),
        ))
    }
}
