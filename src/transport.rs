//! Thin wrappers around `rmcp`'s stdio and Streamable HTTP transports.
//!
//! Each wrapper also starts the [`crate::worker::run_workers`] worker pool
//! against a real `Peer` from a running rmcp service, so durable tool
//! execution kicks in automatically.

use std::sync::Arc;

use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::transport::stdio;
use tokio_util::sync::CancellationToken;

use crate::backend::TaqubaTaskBackend;
use crate::error::{Error, Result};
use crate::worker::run_workers;

/// Serve an MCP `ServerHandler` over the stdio transport with the
/// taquba-mcp worker pool wired up.
///
/// Canonical local-MCP deployment: the agent client spawns this
/// binary as a subprocess and talks over stdin/stdout. This function:
///
/// 1. Hands the handler to rmcp's stdio transport.
/// 2. Clones the resulting `Peer` and starts the durable worker pool.
/// 3. Blocks on `service.waiting()`.
/// 4. On shutdown, cancels the workers and waits for in-flight jobs to
///    drain.
pub async fn serve_stdio<H>(handler: H, backend: Arc<TaqubaTaskBackend>) -> Result<()>
where
    H: ServerHandler + Clone + Send + Sync + 'static,
{
    let service = handler
        .clone()
        .serve(stdio())
        .await
        .map_err(|e| Error::Configuration(format!("rmcp stdio serve failed: {e}")))?;
    let peer = service.peer().clone();
    let shutdown = CancellationToken::new();
    let workers = tokio::spawn(run_workers(backend, handler, peer, shutdown.clone()));
    let quit_reason = service
        .waiting()
        .await
        .map_err(|e| Error::Configuration(format!("rmcp service join failed: {e}")))?;
    tracing::info!(
        ?quit_reason,
        "rmcp stdio service finished; draining workers"
    );
    shutdown.cancel();
    match workers.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "worker pool returned an error"),
        Err(e) => tracing::warn!(error = %e, "worker pool task panicked"),
    }
    Ok(())
}

/// Serve an MCP `ServerHandler` over the Streamable HTTP transport.
///
/// `addr` is a `tokio::net::TcpListener` bind address (e.g.
/// `"0.0.0.0:8080"`). `handler_factory` is called once per HTTP session
/// by rmcp's `StreamableHttpService`; the factory must produce a
/// handler that shares the same [`TaqubaTaskBackend`] across sessions
/// (typical: clone an `Arc<TaqubaTaskBackend>` into each new handler).
///
/// # Worker `Peer` caveat
///
/// rmcp peers are per-session. The durable worker pool, by contrast, is
/// process-wide: a job submitted in session A may complete after session
/// A has disconnected, or while session B is also active. v0.1 hands the
/// workers a single background `Peer` produced from an internal sink
/// transport; tool handlers that call `ctx.peer.send_notification(...)`
/// during task execution will see their notifications silently dropped
/// rather than routed to a specific session. Tools that only return a
/// `CallToolResult` (no notifications) work fine.
pub async fn serve_streamable_http<H, F>(
    handler_factory: F,
    backend: Arc<TaqubaTaskBackend>,
    addr: &str,
) -> Result<()>
where
    H: ServerHandler + Clone + Send + Sync + 'static,
    F: Fn() -> H + Send + Sync + 'static,
{
    use rmcp::service::serve_directly;
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    // Background service, gives us a real `Peer` for the worker pool.
    // `serve_directly` skips the MCP `initialize` handshake (the sink/empty
    // transport would never deliver one); the resulting `Peer` is what the
    // worker pool threads into each synthesized `RequestContext`.
    let background_handler = (handler_factory)();
    let background_service = serve_directly(
        background_handler.clone(),
        (tokio::io::empty(), tokio::io::sink()),
        None,
    );
    let peer = background_service.peer().clone();

    let shutdown = CancellationToken::new();
    let workers = tokio::spawn(run_workers(
        backend.clone(),
        background_handler,
        peer,
        shutdown.clone(),
    ));

    let session_ct = shutdown.child_token();
    let http_service = StreamableHttpService::new(
        move || Ok((handler_factory)()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_cancellation_token(session_ct),
    );

    let router = axum::Router::new().nest_service("/mcp", http_service);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Configuration(format!("bind {addr} failed: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| Error::Configuration(format!("local_addr failed: {e}")))?;
    tracing::info!(addr = %local_addr, "taquba-mcp streamable-http serving on /mcp");

    let server_shutdown = shutdown.clone();
    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            server_shutdown.cancelled().await;
        })
        .await;
    if let Err(e) = serve_result {
        tracing::warn!(error = %e, "axum serve returned an error");
    }
    shutdown.cancel();
    match workers.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "worker pool returned an error"),
        Err(e) => tracing::warn!(error = %e, "worker pool task panicked"),
    }
    Ok(())
}
