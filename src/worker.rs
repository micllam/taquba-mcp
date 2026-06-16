//! Worker pool that consumes task jobs from Taquba and invokes tool handlers.
//!
//! Tasks land in a queue named after the tool. One [`Worker`] task per queue
//! per configured concurrency unit (`workers_per_tool`) blocks on
//! [`Queue::claim_with_wait`](taquba::Queue::claim_with_wait), invokes the
//! handler, writes the result blob, and acks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rmcp::ServerHandler;
use rmcp::model::{CallToolRequestParams, ListToolsResult, TaskSupport, Tool};
use rmcp::service::{Peer, RequestContext, RoleServer};
use taquba::{AckEffects, JobRecord, PRIORITY_NORMAL, Queue, QueueConfig};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::audit::{AuditEntry, AuditEvent, AuditLog};
use crate::backend::TaqubaTaskBackend;
use crate::error::{Error, Result};
use crate::pointer::{TaskPointer, pointer_key};
use crate::result_store::{FinishedStatus, ResultRecord, ResultStore, expires_at_ms};

/// Retry policy applied to durable tool invocations.
///
/// Maps onto `taquba::QueueConfig`'s retry parameters, applying the same
/// policy to every task-eligible tool.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Maximum number of attempts before the job is dead-lettered.
    pub max_attempts: u32,
    /// Initial backoff between retries.
    pub initial_backoff: Duration,
    /// Maximum backoff between retries.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential()
    }
}

impl RetryPolicy {
    /// A reasonable default: exponential backoff up to 5 attempts.
    pub fn exponential() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(60),
        }
    }

    /// Disable retries; one attempt only. A failure dead-letters immediately.
    pub fn never() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::from_secs(0),
            max_backoff: Duration::from_secs(0),
        }
    }

    /// Override the maximum attempt count.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Override the initial backoff between retries.
    pub fn initial_backoff(mut self, d: Duration) -> Self {
        self.initial_backoff = d;
        self
    }

    /// Override the maximum backoff between retries.
    pub fn max_backoff(mut self, d: Duration) -> Self {
        self.max_backoff = d;
        self
    }

    pub(crate) fn to_queue_config(&self) -> QueueConfig {
        QueueConfig {
            max_attempts: self.max_attempts,
            retry_backoff_base: self.initial_backoff,
            retry_backoff_max: self.max_backoff,
            default_priority: PRIORITY_NORMAL,
            ..QueueConfig::default()
        }
    }
}

/// Run worker tasks for every task-eligible tool exposed by `handler`. Blocks
/// until `shutdown` fires, then drains in-flight work and returns.
///
/// `peer` is borrowed from the running rmcp service (e.g. via
/// `RunningService::peer().clone()`). It is placed in every synthesized
/// `RequestContext` so tool handlers that talk back to the client over
/// notifications still work.
///
/// Most users won't call this directly: [`crate::serve_stdio`] and
/// [`crate::serve_streamable_http`] start it for you.
pub async fn run_workers<H>(
    backend: Arc<TaqubaTaskBackend>,
    handler: H,
    peer: Peer<RoleServer>,
    shutdown: CancellationToken,
) -> Result<()>
where
    H: ServerHandler + Clone + Send + Sync + 'static,
{
    let tools = discover_task_tools(&handler, &peer).await?;
    if tools.is_empty() {
        tracing::info!("no task-eligible tools registered; worker pool idle");
        shutdown.cancelled().await;
        return Ok(());
    }
    tracing::info!(tool_count = tools.len(), "starting taquba-mcp worker pool",);

    let mut tasks = JoinSet::new();
    for tool in tools {
        for worker_index in 0..backend.config().workers_per_tool {
            let backend = backend.clone();
            let handler = handler.clone();
            let peer = peer.clone();
            let shutdown = shutdown.clone();
            let tool_name = tool.name.to_string();
            tasks.spawn(async move {
                run_tool_worker(backend, handler, peer, tool_name, worker_index, shutdown).await
            });
        }
    }

    if backend.config().enable_reaper {
        let backend_for_reaper = backend.clone();
        let shutdown_for_reaper = shutdown.clone();
        tasks.spawn(async move { run_reaper(backend_for_reaper, shutdown_for_reaper).await });
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "worker task returned an error"),
            Err(e) => tracing::error!(error = %e, "worker task panicked or was cancelled"),
        }
    }

    Ok(())
}

async fn discover_task_tools<H>(handler: &H, peer: &Peer<RoleServer>) -> Result<Vec<Tool>>
where
    H: ServerHandler + Send + Sync,
{
    // `RequestContext::new(id, peer)` is the only public way to build a
    // context for an out-of-band `list_tools` call. The bootstrap id is
    // never sent over the wire; it only flows through the handler's
    // synthesized `list_tools` body (built by `#[tool_handler]`), which
    // ignores the id entirely.
    let request_id = rmcp::model::RequestId::String(Arc::from("taquba-mcp:bootstrap"));
    let context = RequestContext::new(request_id, peer.clone());
    let ListToolsResult { tools, .. } = handler.list_tools(None, context).await.map_err(|e| {
        Error::Configuration(format!("list_tools failed during worker bootstrap: {e}"))
    })?;

    Ok(tools
        .into_iter()
        .filter(|tool| {
            matches!(
                tool.task_support(),
                TaskSupport::Optional | TaskSupport::Required,
            )
        })
        .collect())
}

async fn run_tool_worker<H>(
    backend: Arc<TaqubaTaskBackend>,
    handler: H,
    peer: Peer<RoleServer>,
    tool_name: String,
    worker_index: usize,
    shutdown: CancellationToken,
) -> Result<()>
where
    H: ServerHandler + Clone + Send + Sync + 'static,
{
    let queue = backend.queue();
    let result_store = backend.result_store();
    let audit_log = backend.audit_log();
    let lease = queue.queue_lease_duration(&tool_name);
    tracing::info!(tool = %tool_name, worker = worker_index, "tool worker online");

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let claim = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            res = queue.claim_with_wait(&tool_name, lease, Duration::from_secs(5)) => res,
        };
        let job = match claim {
            Ok(Some(job)) => job,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(tool = %tool_name, error = %e, "claim error; backing off");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        process_job(
            &backend,
            &handler,
            &peer,
            &result_store,
            &audit_log,
            job,
            queue,
        )
        .await;
    }

    tracing::info!(tool = %tool_name, worker = worker_index, "tool worker shutting down");
    Ok(())
}

async fn process_job<H>(
    backend: &Arc<TaqubaTaskBackend>,
    handler: &H,
    peer: &Peer<RoleServer>,
    result_store: &ResultStore,
    audit_log: &AuditLog,
    job: JobRecord,
    queue: &Queue,
) where
    H: ServerHandler + Send + Sync,
{
    let task_id = job.id.clone();
    let tool_name = job
        .headers
        .get("tool")
        .cloned()
        .unwrap_or_else(|| job.queue.clone());

    let request: CallToolRequestParams = match serde_json::from_slice(&job.payload) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(task_id = %task_id, error = %e, "task payload deserialization failed");
            write_failed(
                result_store,
                audit_log,
                backend,
                &task_id,
                &tool_name,
                format!("payload deserialization failed: {e}"),
            )
            .await;
            // Drop unrecoverable job to the DLQ; it can never succeed.
            if let Err(e) = queue
                .dead_letter(job, "payload deserialization failed")
                .await
            {
                tracing::error!(task_id = %task_id, error = %e, "dead_letter failed");
            }
            return;
        }
    };

    // Hold on to the cooperative-cancellation token before `job` is consumed
    // by ack / nack / dead_letter below.
    let cancel_token = job.cancel_token.clone();

    backend.mark_in_flight(&task_id, &tool_name).await;
    let _ = audit_log
        .record(AuditEntry {
            event: AuditEvent::Started,
            task_id: task_id.clone(),
            tool: tool_name.clone(),
            at_ms: backend.now_ms(),
            error: None,
        })
        .await;

    let request_id = rmcp::model::RequestId::String(Arc::from(task_id.as_str()));
    let mut context = RequestContext::new(request_id, peer.clone());
    // Place Taquba's cooperative cancellation token into the tool's request
    // context. An MCP `tasks/cancel` routes to `Queue::cancel`, which fires
    // this token; a tool that watches `ctx.ct` can then short-circuit.
    if let Some(token) = cancel_token.clone() {
        context.ct = token;
    }

    let outcome = handler.call_tool(request, context).await;
    backend.clear_in_flight(&task_id).await;

    let was_cancelled = cancel_token.as_ref().is_some_and(|t| t.is_cancelled());

    match outcome {
        Ok(call_result) => {
            let now = backend.now_ms();
            let record = ResultRecord {
                task_id: task_id.clone(),
                tool: tool_name.clone(),
                status: FinishedStatus::Completed,
                result: Some(call_result),
                error: None,
                completed_at_ms: now,
                expires_at_ms: expires_at_ms(now, backend.config().result_retention),
            };
            if let Err(e) = result_store.put(&record).await {
                tracing::error!(task_id = %task_id, error = %e, "result blob write failed");
            }
            let _ = audit_log
                .record(AuditEntry {
                    event: AuditEvent::Completed,
                    task_id: task_id.clone(),
                    tool: tool_name.clone(),
                    at_ms: now,
                    error: None,
                })
                .await;
            // Settle the job and commit the terminal pointer in one
            // transaction: the result blob above is provisional until this
            // ack commits, and the pointer is what readers consult.
            let effects = pointer_effects(
                &task_id,
                FinishedStatus::Completed,
                now,
                record.expires_at_ms,
            );
            if let Err(e) = queue.ack_with(&job, effects).await {
                tracing::error!(task_id = %task_id, error = %e, "ack failed");
            }
        }
        Err(e) => {
            let error_message = e.to_string();
            if was_cancelled {
                // Cancellation was delivered (the token fired) and the tool
                // returned an error; treat it as a terminal Cancelled
                // outcome, not a retryable failure. `ack` the job: it is
                // "handled", we just won't retry it. The Cancelled result
                // blob is the queryable record of the outcome, and the
                // pointer is committed atomically with the ack below.
                let now = backend.now_ms();
                let expires = expires_at_ms(now, backend.config().result_retention);
                write_cancelled(result_store, audit_log, &task_id, &tool_name, now, expires).await;
                let effects = pointer_effects(&task_id, FinishedStatus::Cancelled, now, expires);
                if let Err(ae) = queue.ack_with(&job, effects).await {
                    tracing::error!(task_id = %task_id, error = %ae, "ack (cancelled) failed");
                }
            } else {
                let next_attempts = job.attempts.saturating_add(1);
                let terminal = next_attempts >= job.max_attempts;
                if terminal {
                    write_failed(
                        result_store,
                        audit_log,
                        backend,
                        &task_id,
                        &tool_name,
                        error_message.clone(),
                    )
                    .await;
                    if let Err(de) = queue.dead_letter(job, &error_message).await {
                        tracing::error!(task_id = %task_id, error = %de, "dead_letter failed");
                    }
                } else {
                    let _ = audit_log
                        .record(AuditEntry {
                            event: AuditEvent::Failed,
                            task_id: task_id.clone(),
                            tool: tool_name.clone(),
                            at_ms: backend.now_ms(),
                            error: Some(error_message.clone()),
                        })
                        .await;
                    if let Err(ne) = queue.nack(job, &error_message).await {
                        tracing::error!(task_id = %task_id, error = %ne, "nack failed");
                    }
                }
            }
        }
    }
}

async fn write_failed(
    result_store: &ResultStore,
    audit_log: &AuditLog,
    backend: &Arc<TaqubaTaskBackend>,
    task_id: &str,
    tool: &str,
    error_message: String,
) {
    let now = backend.now_ms();
    let record = ResultRecord {
        task_id: task_id.to_string(),
        tool: tool.to_string(),
        status: FinishedStatus::Failed,
        result: None,
        error: Some(error_message.clone()),
        completed_at_ms: now,
        expires_at_ms: expires_at_ms(now, backend.config().result_retention),
    };
    if let Err(e) = result_store.put(&record).await {
        tracing::error!(task_id = %task_id, error = %e, "failed result blob write failed");
    }
    let _ = audit_log
        .record(AuditEntry {
            event: AuditEvent::Failed,
            task_id: task_id.to_string(),
            tool: tool.to_string(),
            at_ms: now,
            error: Some(error_message),
        })
        .await;
}

/// Build the [`AckEffects`] for an ack-path terminal: a single KV write of the
/// task's terminal [`TaskPointer`], committed atomically with the ack. A
/// serialization failure (not expected for this tiny record) degrades to an
/// empty-effects ack rather than leaving the job unsettled.
fn pointer_effects(
    task_id: &str,
    status: FinishedStatus,
    completed_at_ms: u64,
    expires_at_ms: u64,
) -> AckEffects {
    let mut effects = AckEffects::default();
    let pointer = TaskPointer {
        status,
        completed_at_ms,
        expires_at_ms,
    };
    match pointer.to_bytes() {
        Ok(bytes) => {
            effects.kv_writes.insert(pointer_key(task_id), bytes);
        }
        Err(e) => {
            tracing::error!(task_id = %task_id, error = %e, "terminal pointer serialize failed")
        }
    }
    effects
}

/// Write a terminal `Cancelled` result blob and audit entry for a task whose
/// tool short-circuited (returned an error) after its cooperative
/// cancellation token fired. `now` / `expires_at_ms` are passed in so the
/// blob and the pointer committed alongside the ack share one timestamp.
async fn write_cancelled(
    result_store: &ResultStore,
    audit_log: &AuditLog,
    task_id: &str,
    tool: &str,
    now: u64,
    expires_at_ms: u64,
) {
    let record = ResultRecord {
        task_id: task_id.to_string(),
        tool: tool.to_string(),
        status: FinishedStatus::Cancelled,
        result: None,
        error: None,
        completed_at_ms: now,
        expires_at_ms,
    };
    if let Err(e) = result_store.put(&record).await {
        tracing::error!(task_id = %task_id, error = %e, "cancelled result blob write failed");
    }
    let _ = audit_log
        .record(AuditEntry {
            event: AuditEvent::Cancelled,
            task_id: task_id.to_string(),
            tool: tool.to_string(),
            at_ms: now,
            error: None,
        })
        .await;
}

async fn run_reaper(backend: Arc<TaqubaTaskBackend>, shutdown: CancellationToken) -> Result<()> {
    let interval = backend.config().reaper_interval;
    let audit_retention = backend.config().audit_retention;
    let result_store = backend.result_store();
    let audit_log = backend.audit_log();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        match result_store.reap().await {
            Ok(reaped) => {
                // Drop the terminal pointer for each swept task so it never
                // outlives its result blob (a dangling pointer would make
                // get_task_info report Completed for a task whose result is
                // already gone). kv_delete is idempotent, so tasks that never
                // had a pointer (dead-lettered failures, Pending
                // cancellations) are harmless no-ops.
                for task_id in &reaped {
                    if let Err(e) = backend.queue().kv_delete(&pointer_key(task_id)).await {
                        tracing::warn!(task_id = %task_id, error = %e, "pointer reap failed");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "result store reap failed"),
        }
        if let Err(e) = audit_log.reap(audit_retention).await {
            tracing::warn!(error = %e, "audit log reap failed");
        }
    }
    Ok(())
}

/// In-flight task summary kept in memory while a job is being processed.
#[derive(Debug, Clone)]
pub(crate) struct InFlightInfo {
    pub tool: String,
    pub started_at_ms: u64,
}

/// In-flight task index. Populated by workers on `claim`, cleared on
/// result-write. Used by `list_tasks` and `get_task_info` to see in-flight
/// tasks without a (currently nonexistent) Queue::list_jobs API.
pub(crate) type InFlight = HashMap<String, InFlightInfo>;
