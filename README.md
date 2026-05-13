# taquba-mcp

A durable backend for [rmcp](https://crates.io/crates/rmcp)'s task subsystem,
built on the [Taquba](https://crates.io/crates/taquba) task queue.

`rmcp` 1.7 implements the MCP 2025-11-25 task spec (SEP-1319) with an
in-memory `OperationProcessor`: every task lives in a `HashMap` and disappears
on process restart. `taquba-mcp` replaces that with a `Queue`-backed
implementation so task state, retry attempts, and results survive crashes.

## When this fits

- An MCP server that exposes tools with
  `execution(task_support = "required" | "optional")` and needs those tasks
  to survive process restarts.
- A self-hosted MCP deployment that already pays for object storage (S3, GCS,
  Azure Blob, MinIO, or a local path) and wants one fewer database to operate.

## When this does not fit

- A purely synchronous MCP server (all tools have the default
  `task_support = "forbidden"`). Use `rmcp` directly: there is no
  long-running state to protect.
- A worker fleet spread across multiple machines. Taquba (and therefore
  `taquba-mcp`) is single-writer per object-store path.

## Install

```bash
cargo add taquba-mcp
cargo add taquba
cargo add rmcp --features server,macros,transport-io
cargo add tokio --features full
```

For production, opt in to exactly one cloud backend feature:

```bash
cargo add taquba-mcp --features aws    # S3 / MinIO
cargo add taquba-mcp --features gcp    # Google Cloud Storage
cargo add taquba-mcp --features azure  # Azure Blob
```

## Sketch

```rust,ignore
use std::sync::Arc;
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::ErrorData as McpError;
use taquba::object_store::memory::InMemory;
use taquba_mcp::{TaqubaTaskBackend, taquba_task_handler};

#[derive(Clone)]
struct MyServer {
    tool_router: ToolRouter<MyServer>,
    tasks: Arc<TaqubaTaskBackend>,
}

#[tool_router]
impl MyServer {
    fn new(tasks: Arc<TaqubaTaskBackend>) -> Self {
        Self { tool_router: Self::tool_router(), tasks }
    }

    #[tool(
        description = "Sum two numbers, slowly",
        execution(task_support = "required"),
    )]
    async fn slow_add(&self, /* ... */) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text("done")]))
    }
}

#[tool_handler]
impl ServerHandler for MyServer {
    taquba_task_handler!(tasks);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(InMemory::new());
    let tasks = TaqubaTaskBackend::builder()
        .object_store(store)
        .prefix("my-server-data")
        .build()
        .await?;

    let _server = MyServer::new(tasks);
    // ... serve over stdio or Streamable HTTP ...
    Ok(())
}
```

See [`examples/echo_and_slow.rs`](examples/echo_and_slow.rs) for a runnable
server. For a one-command end-to-end run,
[`examples/echo_and_slow_client.rs`](examples/echo_and_slow_client.rs) spawns
that server over stdio and walks the full task lifecycle:

```bash
cargo run --example echo_and_slow_client
```

## Architecture

`taquba-mcp` opens a single Taquba `Queue` under the configured object-store
prefix. Each task-supporting tool gets its own named queue; calls to those
tools enqueue jobs whose payloads carry the MCP `CallToolRequestParams`. A
worker pool claims jobs, invokes the tool's handler, and writes the final
`CallToolResult` to `<prefix>/results/<task_id>.json` for the next
`tasks/result` request.

Synchronous tools (`task_support = "forbidden"`, the default) flow through
`rmcp`'s normal `call_tool` path and never touch the queue.

### On-store layout

```
<prefix>/queue/...              # Taquba's SlateDB queue store
<prefix>/results/<task_id>.json # completed task results
<prefix>/audit/<ulid>.json      # one blob per audit event
```

## Why not `#[task_handler]`?

`rmcp` 1.7 ships a `#[task_handler]` proc macro that wires `ServerHandler`'s
five task methods to an `OperationProcessor` (its in-memory reference impl).
The macro expansion calls `.lock().await` once on a `Mutex<OperationProcessor>`
and then chains synchronous method calls. Our taquba-backed operations are
inherently async (queue I/O, object-store I/O), so we ship our own
`taquba_task_handler!` declarative macro instead. It expands to the same five
methods, with proper awaits, delegating to a `TaqubaTaskBackend` you supply.

## Stability

`taquba-mcp` is pre-1.0. Minor version bumps may break source compatibility
and may also break the on-object-store layout (key prefixes for results,
audit entries, and the underlying Taquba queue). Patch releases preserve both.

## License

Dual-licensed under either

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))
