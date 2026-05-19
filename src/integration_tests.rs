//! End-to-end integration tests exercising the taquba primitives through
//! the full taquba-mcp stack: `enqueue_task` -> worker pool -> tool
//! invocation -> result blob, plus `wait_for_completion`-backed
//! `get_task_result` and `CancelOutcome`-driven `cancel_task`.
//!
//! These run a real worker pool against an in-memory object store and
//! drive the backend's task methods with `RequestContext`s built from a
//! `serve_directly` peer (which skips the MCP `initialize` handshake).

use std::sync::Arc;
use std::time::Duration;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, CancelTaskParams, Content, GetTaskInfoParams,
    GetTaskResultParams, RequestId, TaskStatus,
};
use rmcp::service::{Peer, RequestContext, RoleServer, RunningService, serve_directly};
use rmcp::{ErrorData as McpError, ServerHandler, schemars, tool, tool_handler, tool_router};
use taquba::object_store::memory::InMemory;
use tokio_util::sync::CancellationToken;

use crate::worker::run_workers;
use crate::{Clock, MockClock, TaqubaTaskBackend, taquba_task_handler};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct DoubleArgs {
    n: i64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct SleepArgs {
    ms: u64,
}

#[derive(Clone)]
struct TestServer {
    #[allow(dead_code)]
    tool_router: ToolRouter<TestServer>,
    tasks: Arc<TaqubaTaskBackend>,
}

#[tool_router]
impl TestServer {
    fn new(tasks: Arc<TaqubaTaskBackend>) -> Self {
        Self {
            tool_router: Self::tool_router(),
            tasks,
        }
    }

    /// Fast task tool — returns immediately.
    #[tool(description = "Double a number", execution(task_support = "required"))]
    async fn double(
        &self,
        Parameters(DoubleArgs { n }): Parameters<DoubleArgs>,
    ) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text(
            (n * 2).to_string(),
        )]))
    }

    /// Cooperatively cancellable; watches the `CancellationToken` rmcp
    /// extracts from `RequestContext::ct`, which taquba-mcp's worker wired
    /// from `JobRecord::cancel_token`.
    #[tool(
        description = "Sleep, cooperatively cancellable",
        execution(task_support = "required")
    )]
    async fn cooperative_sleep(
        &self,
        Parameters(SleepArgs { ms }): Parameters<SleepArgs>,
        ct: CancellationToken,
    ) -> Result<CallToolResult, McpError> {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(ms)) => {
                Ok(CallToolResult::success(vec![Content::text("slept")]))
            }
            _ = ct.cancelled() => {
                Err(McpError::internal_error("cancelled by client", None))
            }
        }
    }

    /// Ignores the cancellation token entirely; always runs to completion.
    #[tool(
        description = "Sleep, ignoring cancellation",
        execution(task_support = "required")
    )]
    async fn stubborn_sleep(
        &self,
        Parameters(SleepArgs { ms }): Parameters<SleepArgs>,
    ) -> Result<CallToolResult, McpError> {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(CallToolResult::success(vec![Content::text("slept anyway")]))
    }
}

#[tool_handler]
impl ServerHandler for TestServer {
    taquba_task_handler!(tasks);
}

/// Test harness. `serve_directly` skips the MCP `initialize` handshake and
/// yields a real `Peer`; used both to build `RequestContext`s for the
/// backend's task methods and to feed the worker pool.
struct Harness {
    backend: Arc<TaqubaTaskBackend>,
    peer: Peer<RoleServer>,
    _service: RunningService<RoleServer, TestServer>,
    shutdown: CancellationToken,
    workers: Option<tokio::task::JoinHandle<crate::Result<()>>>,
}

/// Options for [`Harness::build`].
#[derive(Default)]
struct HarnessOpts {
    start_workers: bool,
    clock: Option<Arc<dyn Clock>>,
    disable_reaper: bool,
}

impl HarnessOpts {
    /// Start the worker pool. The default leaves enqueued jobs `Pending`.
    fn with_workers(mut self) -> Self {
        self.start_workers = true;
        self
    }

    /// Override the time source. Threaded into both Taquba and taquba-mcp
    /// so the queue and the audit/result/in-flight timestamps share one
    /// view of "now."
    fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Suppress the in-process reaper. Use when the test wants to drive
    /// the sweep manually (e.g. retention tests pairing this with
    /// [`with_clock`](Self::with_clock) + a [`MockClock`]).
    fn disable_reaper(mut self) -> Self {
        self.disable_reaper = true;
        self
    }
}

impl Harness {
    async fn build(opts: HarnessOpts) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "off".into()),
            )
            .try_init();
        let store = Arc::new(InMemory::new());
        let mut builder = TaqubaTaskBackend::builder()
            .object_store(store)
            .prefix("itest");
        if let Some(clock) = opts.clock {
            builder = builder.clock(clock);
        }
        if opts.disable_reaper {
            builder = builder.disable_reaper();
        }
        let backend = builder.build().await.expect("backend builds");
        let server = TestServer::new(backend.clone());
        let service = serve_directly(
            server.clone(),
            (tokio::io::empty(), tokio::io::sink()),
            None,
        );
        let peer = service.peer().clone();
        let shutdown = CancellationToken::new();
        let workers = opts.start_workers.then(|| {
            tokio::spawn(run_workers(
                backend.clone(),
                server,
                peer.clone(),
                shutdown.clone(),
            ))
        });
        Self {
            backend,
            peer,
            _service: service,
            shutdown,
            workers,
        }
    }

    fn ctx(&self, id: &str) -> RequestContext<RoleServer> {
        RequestContext::new(RequestId::String(id.into()), self.peer.clone())
    }

    async fn enqueue(&self, tool: &str, args: serde_json::Value) -> String {
        let mut params = CallToolRequestParams::new(tool.to_string());
        if let serde_json::Value::Object(map) = args {
            params = params.with_arguments(map);
        }
        self.backend
            .enqueue_task(params, self.ctx("enqueue"))
            .await
            .expect("enqueue_task succeeds")
            .task
            .task_id
    }

    async fn task_status(&self, task_id: &str) -> rmcp::model::Task {
        self.backend
            .get_task_info(
                GetTaskInfoParams {
                    meta: None,
                    task_id: task_id.to_string(),
                },
                self.ctx("info"),
            )
            .await
            .expect("get_task_info succeeds")
            .task
    }

    /// Poll `get_task_info` until the task is being actively run by a
    /// worker (`snapshot_task` reports `status_message = "running"`).
    async fn wait_until_running(&self, task_id: &str) {
        for _ in 0..400 {
            if let Ok(info) = self
                .backend
                .get_task_info(
                    GetTaskInfoParams {
                        meta: None,
                        task_id: task_id.to_string(),
                    },
                    self.ctx("poll"),
                )
                .await
            {
                if info.task.status_message.as_deref() == Some("running") {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("task {task_id} never reached the running state");
    }

    async fn get_result(
        &self,
        task_id: &str,
    ) -> std::result::Result<rmcp::model::GetTaskPayloadResult, McpError> {
        self.backend
            .get_task_result(
                GetTaskResultParams {
                    meta: None,
                    task_id: task_id.to_string(),
                },
                self.ctx("result"),
            )
            .await
    }

    async fn cancel(&self, task_id: &str) -> std::result::Result<TaskStatus, McpError> {
        self.backend
            .cancel_task(
                CancelTaskParams {
                    meta: None,
                    task_id: task_id.to_string(),
                },
                self.ctx("cancel"),
            )
            .await
            .map(|result| result.task.status)
    }

    async fn shutdown(self) {
        self.shutdown.cancel();
        if let Some(workers) = self.workers {
            let _ = tokio::time::timeout(Duration::from_secs(5), workers).await;
        }
    }
}

/// `enqueue_task` -> worker pool -> tool runs -> result blob, surfaced via
/// the `wait_for_completion`-backed `get_task_result`.
#[tokio::test(flavor = "multi_thread")]
async fn task_runs_and_wait_for_completion_returns_the_result() {
    let h = Harness::build(HarnessOpts::default().with_workers()).await;
    let task_id = h.enqueue("double", serde_json::json!({ "n": 21 })).await;

    let payload = h
        .get_result(&task_id)
        .await
        .expect("get_task_result succeeds");
    let rendered = serde_json::to_string(&payload.0).expect("payload serializes");
    assert!(
        rendered.contains("42"),
        "expected the doubled value in the payload, got: {rendered}"
    );

    h.shutdown().await;
}

/// `cancel_task` on a still-`Pending` job: `CancelOutcome::Removed`; the
/// queue job is removed and a terminal `Cancelled` blob is recorded.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_pending_task_removes_it_and_records_cancelled() {
    // No worker pool, so the enqueued job stays `Pending`.
    let h = Harness::build(HarnessOpts::default()).await;
    let task_id = h.enqueue("double", serde_json::json!({ "n": 1 })).await;

    assert_eq!(
        h.cancel(&task_id).await.expect("cancel_task succeeds"),
        TaskStatus::Cancelled,
    );
    // The Cancelled blob is visible via get_task_info.
    assert_eq!(h.task_status(&task_id).await.status, TaskStatus::Cancelled);

    h.shutdown().await;
}

/// `cancel_task` on a `Claimed` job: `CancelOutcome::Requested`; the
/// cancellation token fires into the tool's `RequestContext::ct`, the
/// tool short-circuits, and the worker records a terminal `Cancelled`
/// outcome without retrying.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_claimed_task_is_cooperative_and_tool_short_circuits() {
    let h = Harness::build(HarnessOpts::default().with_workers()).await;
    // A long sleep so the worker is mid-execution when we cancel.
    let task_id = h
        .enqueue("cooperative_sleep", serde_json::json!({ "ms": 60_000 }))
        .await;
    h.wait_until_running(&task_id).await;

    assert_eq!(
        h.cancel(&task_id).await.expect("cancel_task succeeds"),
        TaskStatus::Cancelled,
    );

    // The tool watched `ct`, returned an error on cancellation, and the
    // worker recorded a terminal Cancelled result; surfaced here as an
    // error from get_task_result.
    let err = h
        .get_result(&task_id)
        .await
        .expect_err("a cancelled task surfaces as an error from get_task_result");
    assert!(
        err.message.contains("cancelled"),
        "expected a cancellation error, got: {}",
        err.message,
    );

    h.shutdown().await;
}

/// Cancellation is cooperative: a tool that ignores the token still runs
/// to completion, and its success result is what `get_task_result`
/// returns, even though `cancel_task` reported `Cancelled`.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_is_cooperative_stubborn_tool_still_completes() {
    let h = Harness::build(HarnessOpts::default().with_workers()).await;
    // Long enough that the cancel reliably lands while the tool is still
    // running (the test asserts the cancel hits a `Claimed` job), but
    // short enough not to drag the suite.
    let task_id = h
        .enqueue("stubborn_sleep", serde_json::json!({ "ms": 2_000 }))
        .await;
    h.wait_until_running(&task_id).await;

    // The cancel is delivered (CancelOutcome::Requested)...
    assert_eq!(
        h.cancel(&task_id).await.expect("cancel_task succeeds"),
        TaskStatus::Cancelled,
    );

    // ...but the tool ignored the token and ran to completion, so the
    // recorded result is the success value.
    let payload = h
        .get_result(&task_id)
        .await
        .expect("the stubborn tool completes despite the cancel");
    let rendered = serde_json::to_string(&payload.0).expect("payload serializes");
    assert!(
        rendered.contains("slept anyway"),
        "expected the completion value, got: {rendered}"
    );

    h.shutdown().await;
}

/// Retention sweep with virtualised time: after a task completes, advancing
/// the `MockClock` past `result_retention` and calling the result-store
/// reaper deletes the blob.
#[tokio::test(flavor = "multi_thread")]
async fn result_blob_expires_and_reaper_sweeps_it() {
    let clock = MockClock::new(1_700_000_000_000);
    let h = Harness::build(
        HarnessOpts::default()
            .with_workers()
            .with_clock(Arc::new(clock.clone()))
            .disable_reaper(),
    )
    .await;
    let task_id = h.enqueue("double", serde_json::json!({ "n": 7 })).await;

    // Let the worker complete the task. `double` is synchronous, so this
    // returns as soon as the blob is written and the queue job is acked.
    let _ = h.get_result(&task_id).await.expect("task completes");

    // The result blob is present immediately after completion.
    assert!(
        h.backend
            .result_store()
            .get(&task_id)
            .await
            .expect("result store get succeeds")
            .is_some(),
        "result blob should be present immediately after completion",
    );

    // Advance virtual time past the default 24h retention window. The
    // worker stamped `expires_at_ms` from the same `MockClock`, so the
    // record is now eligible for sweep.
    clock.advance(Duration::from_secs(25 * 60 * 60));

    let deleted = h
        .backend
        .result_store()
        .reap()
        .await
        .expect("reap succeeds");
    assert!(
        deleted >= 1,
        "reap should delete at least one expired blob, got {deleted}",
    );

    // ...and the blob is gone.
    assert!(
        h.backend
            .result_store()
            .get(&task_id)
            .await
            .expect("result store get succeeds")
            .is_none(),
        "result blob should be gone after reap",
    );

    h.shutdown().await;
}

/// `cancel_task` on an unknown id: `CancelOutcome::NotFound` ->
/// `resource_not_found`.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_unknown_task_is_not_found() {
    let h = Harness::build(HarnessOpts::default()).await;
    let err = h
        .cancel("no-such-task")
        .await
        .expect_err("cancelling an unknown task is an error");
    assert!(
        err.message.contains("not found"),
        "expected a not-found error, got: {}",
        err.message,
    );
    h.shutdown().await;
}
