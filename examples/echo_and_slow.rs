//! End-to-end demo: an MCP server with one synchronous tool and one
//! task-required tool, served over stdio with durable task execution
//! wired through [`TaqubaTaskBackend`].
//!
//! Run with:
//!
//! ```bash
//! cargo run --example echo_and_slow
//! ```
//!
//! On its own this just waits for an MCP client on stdin. For a one-command
//! end-to-end run, use the [`echo_and_slow_client`] example, which spawns
//! this server as a child process and walks the task lifecycle. You can
//! also point any stdio-capable MCP client (e.g. Claude Desktop) at this
//! binary.
//!
//! [`echo_and_slow_client`]: echo_and_slow_client

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::{ErrorData as McpError, ServerHandler, schemars, tool, tool_handler, tool_router};
use taquba::object_store::memory::InMemory;
use taquba_mcp::{TaqubaTaskBackend, serve_stdio, taquba_task_handler};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct AddArgs {
    a: i32,
    b: i32,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct EchoArgs {
    message: String,
}

#[derive(Clone)]
struct DemoServer {
    #[allow(dead_code)]
    tool_router: ToolRouter<DemoServer>,
    tasks: Arc<TaqubaTaskBackend>,
}

#[tool_router]
impl DemoServer {
    fn new(tasks: Arc<TaqubaTaskBackend>) -> Self {
        Self {
            tool_router: Self::tool_router(),
            tasks,
        }
    }

    /// Sync tool: returns immediately, no task involvement. The default
    /// `task_support = "forbidden"` keeps this on rmcp's normal
    /// `call_tool` path.
    #[tool(description = "Echo a message back")]
    async fn echo(
        &self,
        Parameters(EchoArgs { message }): Parameters<EchoArgs>,
    ) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text(message)]))
    }

    /// Task-required tool: clients MUST invoke via `tasks/create`.
    /// taquba-mcp enqueues the call, returns a task id immediately, and
    /// the worker pool runs the body durably.
    #[tool(
        description = "Sum two numbers after a delay (task-based)",
        execution(task_support = "required")
    )]
    async fn slow_add(
        &self,
        Parameters(AddArgs { a, b }): Parameters<AddArgs>,
    ) -> Result<CallToolResult, McpError> {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        Ok(CallToolResult::success(vec![Content::text(
            (a + b).to_string(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for DemoServer {
    taquba_task_handler!(tasks);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let store = Arc::new(InMemory::new());
    let tasks = TaqubaTaskBackend::builder()
        .object_store(store)
        .prefix("demo")
        .build()
        .await?;

    let server = DemoServer::new(tasks.clone());
    serve_stdio(server, tasks).await?;
    Ok(())
}
