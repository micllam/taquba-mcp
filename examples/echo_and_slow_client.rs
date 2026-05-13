//! End-to-end client for the [`echo_and_slow`] server example.
//!
//! This is the easy way to run the demo: it spawns the server example as a
//! child process over stdio and walks the full MCP task lifecycle, so a
//! single command exercises the whole stack.
//!
//! ```bash
//! cargo run --example echo_and_slow_client
//! ```
//!
//! It calls the synchronous `echo` tool, then the task-required `slow_add`
//! tool: `tasks/create` -> poll `tasks/info` -> `tasks/result`.
//!
//! [`echo_and_slow`]: echo_and_slow

use anyhow::{Result, anyhow};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientRequest, GetTaskInfoParams, GetTaskResultParams,
    JsonObject, Request, ServerResult, TaskStatus,
};
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{ServiceExt, object};
use tokio::process::Command;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Spawn the `echo_and_slow` server example as a child process and talk
    // to it over its stdin/stdout.
    let client = ()
        .serve(TokioChildProcess::new(Command::new("cargo").configure(
            |cmd| {
                cmd.arg("run")
                    .arg("-q")
                    .arg("--example")
                    .arg("echo_and_slow");
            },
        ))?)
        .await?;

    // 1) Synchronous call. `echo` has the default task_support = forbidden,
    //    so it flows through the normal `tools/call` path.
    let echo = client
        .call_tool(
            CallToolRequestParams::new("echo")
                .with_arguments(object!({ "message": "hi from the taquba-mcp client" })),
        )
        .await?;
    tracing::info!("echo -> {echo:#?}");

    // 2) Task call. `slow_add` is task_support = required, so we attach a
    //    `task` object. The server enqueues the call and returns a `Task`
    //    with a `task_id` immediately.
    let create = client
        .send_request(ClientRequest::CallToolRequest(Request::new(
            CallToolRequestParams::new("slow_add")
                .with_arguments(object!({ "a": 40, "b": 2 }))
                .with_task(JsonObject::new()),
        )))
        .await?;
    let ServerResult::CreateTaskResult(create) = create else {
        return Err(anyhow!("expected CreateTaskResult, got {create:?}"));
    };
    let task_id = create.task.task_id.clone();
    tracing::info!(
        "slow_add enqueued as task {task_id} (status = {:?})",
        create.task.status
    );

    // 3) Poll `tasks/info` until the server reports a terminal status.
    let final_status = loop {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let info = client
            .send_request(ClientRequest::GetTaskInfoRequest(Request::new(
                GetTaskInfoParams {
                    meta: None,
                    task_id: task_id.clone(),
                },
            )))
            .await?;
        let ServerResult::GetTaskResult(info) = info else {
            return Err(anyhow!("expected GetTaskResult, got {info:?}"));
        };
        tracing::info!("status = {:?}", info.task.status);

        match info.task.status {
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => {
                break info.task.status;
            }
            _ => {}
        }
    };

    if final_status != TaskStatus::Completed {
        return Err(anyhow!("task ended in {final_status:?}"));
    }

    // 4) Fetch the payload (the worker's stored `CallToolResult`).
    let payload = client
        .send_request(ClientRequest::GetTaskResultRequest(Request::new(
            GetTaskResultParams {
                meta: None,
                task_id: task_id.clone(),
            },
        )))
        .await?;
    let call_result: CallToolResult = match payload {
        ServerResult::CallToolResult(r) => r,
        ServerResult::CustomResult(c) => serde_json::from_value(c.0)?,
        other => return Err(anyhow!("unexpected task result: {other:?}")),
    };
    tracing::info!("slow_add result -> {call_result:#?}");

    client.cancel().await?;
    Ok(())
}
