//! A durable backend for [`rmcp`]'s task subsystem, built on the
//! [Taquba](https://crates.io/crates/taquba) task queue.
//!
//! `rmcp` 1.7 implements the MCP 2025-11-25 task spec (SEP-1319) with an
//! in-memory [`rmcp::task_manager::OperationProcessor`]: every task lives in
//! a `HashMap` and disappears on process restart. `taquba-mcp` replaces that
//! with a [`Queue`](taquba::Queue)-backed implementation so task state, retry
//! attempts, and results survive crashes.
//!
//! # When this fits
//!
//! - An MCP server that exposes tools with
//!   `execution(task_support = "required" | "optional")` and needs those tasks
//!   to survive process restarts.
//! - A self-hosted MCP deployment that already pays for object storage (S3,
//!   GCS, Azure Blob, MinIO, or a local path) and wants one fewer database to
//!   operate.
//!
//! # When this does not fit
//!
//! - A purely synchronous MCP server (all tools have the default
//!   `task_support = "forbidden"`). Use `rmcp` directly: there is no
//!   long-running state to protect.
//! - A worker fleet spread across multiple machines. Taquba (and therefore
//!   `taquba-mcp`) is single-writer per object-store path.
//!
//! # Sketch
//!
//! ```ignore
//! use std::sync::Arc;
//! use rmcp::{ServerHandler, ServiceExt, transport::stdio};
//! use rmcp::handler::server::router::tool::ToolRouter;
//! use taquba::object_store::memory::InMemory;
//! use taquba_mcp::{TaqubaTaskBackend, taquba_task_handler};
//!
//! #[derive(Clone)]
//! struct MyServer {
//!     tool_router: ToolRouter<MyServer>,
//!     tasks: Arc<TaqubaTaskBackend>,
//! }
//!
//! // ... `#[tool_router]` impl with `#[tool(execution(task_support = "required"))]` tools ...
//!
//! #[rmcp::tool_handler]
//! impl ServerHandler for MyServer {
//!     taquba_task_handler!(tasks);
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let backend = TaqubaTaskBackend::builder()
//!     .object_store(Arc::new(InMemory::new()))
//!     .prefix("mcp-data")
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the `echo_and_slow` example for a runnable version.
//!
//! # Architecture
//!
//! `taquba-mcp` opens a single Taquba [`Queue`](taquba::Queue) under the
//! configured object-store prefix. Each task-supporting tool gets its own
//! named queue; calls to those tools enqueue jobs whose payloads carry the
//! MCP `CallToolRequestParams`. A worker pool claims jobs, invokes the tool's
//! handler, and writes the final `CallToolResult` to
//! `<prefix>/results/<task_id>.json` for the next `tasks/result` request.
//!
//! Synchronous tools (`task_support = "forbidden"`, the default) flow through
//! `rmcp`'s normal `call_tool` path and never touch the queue.
//!
//! # Stability
//!
//! `taquba-mcp` is pre-1.0. Minor version bumps may break source compatibility
//! and may also break the on-object-store layout (key prefixes for results,
//! audit entries, and the underlying Taquba queue). Patch releases preserve both.

#![warn(missing_docs)]

mod audit;
mod backend;
mod error;
mod handler;
mod result_store;
mod transport;
mod worker;

#[cfg(test)]
mod integration_tests;

pub use backend::{TaqubaTaskBackend, TaqubaTaskBackendBuilder, TaqubaTaskBackendConfig};
pub use error::{Error, Result};
pub use transport::{serve_stdio, serve_streamable_http};
pub use worker::RetryPolicy;

/// Re-exports of [`taquba`]'s time-source types. The backend's
/// [`TaqubaTaskBackendBuilder::clock`](crate::TaqubaTaskBackendBuilder::clock)
/// accepts any [`Clock`]; tests can pass a [`MockClock`] to advance time
/// deterministically, while production callers leave the default
/// [`SystemClock`].
pub use taquba::{Clock, MockClock, SystemClock};

/// Re-export of [`taquba`] so consumers can depend on a single version.
pub use taquba;

/// Re-export of [`rmcp`] so consumers can depend on a single version.
pub use rmcp;
