use crate::{
    ConfigError, IdentityError, KeyExtractor, PeerIpKeyExtractor, RateLimitConfig,
    headers::ResponseHeaders,
    observability::{ObservedStore, ObserverState, RateLimitObserver},
    store::{RateLimitDecision, RateLimitStore},
};
use axum::response::{IntoResponse, Response};
use futures_util::future::BoxFuture;
use http::{Request, StatusCode, request::Parts};
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Layer;
use tower_service::Service;

type RejectionHandler = dyn Fn(&str, &RateLimitConfig, &Parts) -> Response + Send + Sync;

/// Cloneable Axum/Tower layer. Clones share stores, configuration and observer state.
///
/// No `Clone` bound is imposed on custom stores. `MemoryStore` is owned once in an
/// `Arc`. To share a store across separately configured layers, pass an explicit
/// `Arc<MemoryStore>` to each constructor.
#[derive(Clone)]
pub struct RateLimitLayer {
    store: Arc<dyn RateLimitStore>,
    config: Arc<RateLimitConfig>,
    extractor: Arc<dyn KeyExtractor>,
    exceeded: Arc<RejectionHandler>,
    response_headers: Option<Arc<ResponseHeaders>>,
    observer: Option<Arc<ObserverState>>,
}

impl RateLimitLayer {
    /// Panics on invalid rules. Use [`Self::try_new`] for fallible construction.
    #[track_caller]
    pub fn new(config: RateLimitConfig, store: impl RateLimitStore + 'static) -> Self {
        Self::try_new(config, store).expect("invalid rate limit configuration")
    }

    pub fn try_new(
        config: RateLimitConfig,
        store: impl RateLimitStore + 'static,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            store: Arc::new(store),
            config: Arc::new(config),
            extractor: Arc::new(PeerIpKeyExtractor),
            exceeded: Arc::new(|_, _, _| {
                (StatusCode::TOO_MANY_REQUESTS, "Too many requests").into_response()
            }),
            response_headers: None,
            observer: None,
        })
    }

    /// Set an infallible identity function. The function must validate its input;
    /// never return raw credentials or accept arbitrary client identities.
    pub fn id<F>(self, extract: F) -> Self
    where
        F: Fn(&Parts) -> String + Send + Sync + 'static,
    {
        self.key_extractor(move |parts: &Parts| Ok::<_, IdentityError>(extract(parts)))
    }

    /// Set a fallible identity function. Failures do not touch the store or handler.
    pub fn try_id<F>(self, extract: F) -> Self
    where
        F: Fn(&Parts) -> Result<String, IdentityError> + Send + Sync + 'static,
    {
        self.key_extractor(extract)
    }

    pub fn key_extractor(mut self, extractor: impl KeyExtractor + 'static) -> Self {
        self.extractor = Arc::new(extractor);
        self
    }

    /// Customize quota-exhausted responses. Capturing closures are supported.
    /// Automatic Retry-After is only added when this response has status 429.
    pub fn exceeded<F, R>(mut self, handler: F) -> Self
    where
        F: Fn(&str, &RateLimitConfig, &Parts) -> R + Send + Sync + 'static,
        R: IntoResponse + 'static,
    {
        self.exceeded =
            Arc::new(move |id, config, parts| handler(id, config, parts).into_response());
        self
    }

    /// Opt into quota fields (off by default).
    ///
    /// `RateLimit` / `RateLimit-Policy` follow IETF draft
    /// `draft-ietf-httpapi-ratelimit-headers-11`, not a finalized RFC. Existing
    /// fields are preserved independently. Retry-After rounds up to seconds and
    /// is only attached to this layer's own 429 response when a delay is known.
    /// Custom stores without metadata and fail-open responses omit quota fields.
    ///
    /// Disabled: calls `is_limited` once. Enabled: calls `check` once. There is
    /// never a second storage check for metadata, headers or observation.
    pub fn response_headers(mut self, enabled: bool) -> Self {
        self.response_headers = enabled.then(|| Arc::new(ResponseHeaders::new(&self.config)));
        self
    }

    /// Observe completed checks without installing exporters or background tasks.
    /// No observation clocks or events are created when this is not configured.
    pub fn observe(mut self, observer: impl RateLimitObserver + 'static) -> Self {
        self.observer = Some(Arc::new(ObserverState::new(observer)));
        self
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let store: Arc<dyn RateLimitStore> = match &self.observer {
            Some(observer) => Arc::new(ObservedStore {
                store: self.store.clone(),
                observer: observer.clone(),
            }),
            None => self.store.clone(),
        };
        RateLimitService {
            inner,
            store,
            config: self.config.clone(),
            extractor: self.extractor.clone(),
            exceeded: self.exceeded.clone(),
            response_headers: self.response_headers.clone(),
        }
    }
}

/// Service produced by [`RateLimitLayer`].
///
/// `poll_ready` delegates downstream. The ready service (not a fresh, unready
/// clone) is moved into the future. Its readiness reservation is held while the
/// storage check runs and released by dropping that service on rejection or
/// cancellation. Use an outer timeout/concurrency bound for expensive backends.
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    store: Arc<dyn RateLimitStore>,
    config: Arc<RateLimitConfig>,
    extractor: Arc<dyn KeyExtractor>,
    exceeded: Arc<RejectionHandler>,
    response_headers: Option<Arc<ResponseHeaders>>,
}

impl<S, B> Service<Request<B>> for RateLimitService<S>
where
    S: Service<Request<B>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // Tower readiness belongs to this instance, not to its clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let store = self.store.clone();
        let config = self.config.clone();
        let extractor = self.extractor.clone();
        let exceeded = self.exceeded.clone();
        let response_headers = self.response_headers.clone();
        Box::pin(async move {
            // Keep the body opaque; B does not need Sync and is never buffered.
            let (parts, body) = request.into_parts();
            let id = match extractor.extract(&parts) {
                Ok(id) => id,
                Err(error) => return Ok(error.into_response()),
            };
            let decision = if response_headers.is_some() {
                store.check(&id, &config).await
            } else {
                store
                    .is_limited(&id, &config)
                    .await
                    .map(|limited| RateLimitDecision {
                        allowed: !limited,
                        metadata: None,
                    })
            };
            let metadata = match decision {
                Ok(decision) if !decision.allowed => {
                    let mut response = exceeded(&id, &config, &parts);
                    if let Some(headers) = &response_headers
                        && let Some(metadata) = decision.metadata
                    {
                        let own_429 = response.status() == StatusCode::TOO_MANY_REQUESTS;
                        headers.apply(response.headers_mut(), metadata, own_429);
                    }
                    return Ok(response);
                }
                Err(_) => {
                    return Ok((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Rate limit storage unavailable",
                    )
                        .into_response());
                }
                Ok(decision) => decision.metadata,
            };
            let mut response = inner.call(Request::from_parts(parts, body)).await?;
            if let Some(headers) = &response_headers
                && let Some(metadata) = metadata
            {
                headers.apply(response.headers_mut(), metadata, false);
            }
            Ok(response)
        })
    }
}
