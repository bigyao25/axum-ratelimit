use axum::{Router, routing::get};
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(layer: RateLimitLayer) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(layer);
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server { address, task }
}

async fn http_get(address: SocketAddr) -> u16 {
    tokio::time::timeout(Duration::from_secs(3), async move {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        let response = String::from_utf8(bytes).unwrap();
        response
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tcp_connections_provide_connect_info_and_share_quota_across_ports() {
    let server = start(RateLimitLayer::new(
        RateLimitConfig::default().max_requests(2),
        MemoryStore::new(),
    ))
    .await;
    assert_eq!(http_get(server.address).await, 200);
    assert_eq!(http_get(server.address).await, 200);
    assert_eq!(http_get(server.address).await, 429);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_listeners_share_explicit_memory_store() {
    let store = Arc::new(MemoryStore::new());
    let config = RateLimitConfig::default().max_requests(1);
    let first = start(RateLimitLayer::new(config.clone(), store.clone())).await;
    let second = start(RateLimitLayer::new(config, store)).await;
    assert_eq!(http_get(first.address).await, 200);
    assert_eq!(http_get(second.address).await, 429);
}
