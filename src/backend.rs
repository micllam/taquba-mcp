//! The durable task backend.
//!
//! [`TaqubaTaskBackend`] owns the [`taquba::Queue`] and the object-store
//! handles that back result and audit storage. User code constructs it via
//! [`TaqubaTaskBackend::builder`] and then plugs it into a `ServerHandler`
//! impl using the [`crate::taquba_task_handler`] macro.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rmcp::ErrorData as McpError;
use rmcp::model::{
    CallToolRequestParams, CancelTaskParams, CancelTaskResult, CreateTaskResult, GetTaskInfoParams,
    GetTaskPayloadResult, GetTaskResult, GetTaskResultParams, ListTasksResult,
    PaginatedRequestParams, Task, TaskStatus,
};
use rmcp::service::{RequestContext, RoleServer};
use taquba::object_store::ObjectStore;
use taquba::object_store::path::Path;
use taquba::{CancelOutcome, Clock, EnqueueOptions, JobStatus, OpenOptions, Queue, SystemClock};
use tokio::sync::RwLock;

use crate::audit::{AuditEntry, AuditEvent, AuditLog};
use crate::error::{Error, Result};
use crate::pointer;
use crate::result_store::{FinishedStatus, ResultRecord, ResultStore, expires_at_ms};
use crate::worker::{InFlight, InFlightInfo, RetryPolicy};

/// Configuration settings for a [`TaqubaTaskBackend`].
///
/// Constructed by [`TaqubaTaskBackendBuilder`]; users should not need to
/// build this directly.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TaqubaTaskBackendConfig {
    /// How long completed result blobs live in object storage before the
    /// reaper sweeps them.
    pub result_retention: Duration,
    /// How long audit log blobs live in object storage before the reaper
    /// sweeps them.
    pub audit_retention: Duration,
    /// Default retry policy for tool jobs.
    pub default_retry_policy: RetryPolicy,
    /// Number of concurrent worker tasks per tool queue.
    pub workers_per_tool: usize,
    /// How often the in-process reaper sweeps expired result + audit blobs.
    pub reaper_interval: Duration,
    /// Whether the in-process reaper runs at all. Disable to manage retention
    /// from a separate process.
    pub enable_reaper: bool,
    /// How long `get_task_result` waits for a still-running task before
    /// returning a `not yet available` error. The coarse wait is
    /// notification-based via [`taquba::Queue::wait_for_completion`]; this
    /// bounds the *total* time, including the brief result-blob reconcile.
    pub get_result_max_wait: Duration,
    /// Poll cadence for `get_task_result`'s result-blob reconcile, the
    /// short window after [`taquba::Queue::wait_for_completion`] returns
    /// during which the result blob may still be landing (a `cancel`
    /// writes the blob around the queue removal; the reaper can
    /// dead-letter a job whose worker is still running). The common path
    /// finds the blob on the first check and never sleeps.
    pub get_result_poll_interval: Duration,
}

impl Default for TaqubaTaskBackendConfig {
    fn default() -> Self {
        Self {
            result_retention: Duration::from_secs(24 * 60 * 60),
            audit_retention: Duration::from_secs(30 * 24 * 60 * 60),
            default_retry_policy: RetryPolicy::default(),
            workers_per_tool: 1,
            reaper_interval: Duration::from_secs(60),
            enable_reaper: true,
            get_result_max_wait: Duration::from_secs(30),
            get_result_poll_interval: Duration::from_millis(250),
        }
    }
}

/// Durable backend for `rmcp`'s task subsystem.
///
/// Created via [`TaqubaTaskBackend::builder`].
///
/// All `enqueue_task` / `list_tasks` / `get_task_info` / `get_task_result` /
/// `cancel_task` methods on the user's `ServerHandler` should delegate here
/// via [`crate::taquba_task_handler`].
pub struct TaqubaTaskBackend {
    queue: Arc<Queue>,
    result_store: ResultStore,
    audit_log: AuditLog,
    in_flight: RwLock<InFlight>,
    config: TaqubaTaskBackendConfig,
    clock: Arc<dyn Clock>,
}

impl TaqubaTaskBackend {
    /// Start configuring a backend.
    pub fn builder() -> TaqubaTaskBackendBuilder {
        TaqubaTaskBackendBuilder::default()
    }

    pub(crate) fn config(&self) -> &TaqubaTaskBackendConfig {
        &self.config
    }

    pub(crate) fn queue(&self) -> &Queue {
        &self.queue
    }

    pub(crate) fn result_store(&self) -> ResultStore {
        self.result_store.clone()
    }

    pub(crate) fn audit_log(&self) -> AuditLog {
        self.audit_log.clone()
    }

    /// Current time in ms since the UNIX epoch, read through the backend's
    /// configured [`Clock`]. Used everywhere a state-transition timestamp is
    /// recorded so tests can substitute a [`taquba::MockClock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub(crate) async fn mark_in_flight(&self, task_id: &str, tool: &str) {
        let started_at_ms = self.now_ms();
        self.in_flight.write().await.insert(
            task_id.to_string(),
            InFlightInfo {
                tool: tool.to_string(),
                started_at_ms,
            },
        );
    }

    pub(crate) async fn clear_in_flight(&self, task_id: &str) {
        self.in_flight.write().await.remove(task_id);
    }

    // Methods below are invoked by the `taquba_task_handler!` macro

    /// Enqueue a task-based tool invocation.
    pub async fn enqueue_task(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CreateTaskResult, McpError> {
        let tool = request.name.to_string();
        let payload = serde_json::to_vec(&request).map_err(|e| {
            McpError::internal_error(format!("failed to serialize tool request: {e}"), None)
        })?;
        let mut headers = HashMap::new();
        headers.insert("tool".to_string(), tool.clone());

        let opts = EnqueueOptions {
            headers,
            ..EnqueueOptions::default()
        };
        let task_id = self
            .queue
            .enqueue_with(&tool, payload, opts)
            .await
            .map_err(|e| McpError::internal_error(format!("taquba enqueue failed: {e}"), None))?;

        // Track it as in-flight so `list_tasks` / `get_task_info` see it
        // before a worker has claimed.
        let now = self.now_ms();
        self.in_flight.write().await.insert(
            task_id.clone(),
            InFlightInfo {
                tool: tool.clone(),
                started_at_ms: now,
            },
        );

        let _ = self
            .audit_log
            .record(AuditEntry {
                event: AuditEvent::Submitted,
                task_id: task_id.clone(),
                tool: tool.clone(),
                at_ms: now,
                error: None,
            })
            .await;

        let now = Utc::now().to_rfc3339();
        let mut task =
            Task::new(task_id, TaskStatus::Working, now.clone(), now).with_status_message("queued");
        if let Some(ttl) = self.task_ttl_ms() {
            task = task.with_ttl(ttl);
        }
        Ok(CreateTaskResult::new(task))
    }

    /// List tasks the backend knows about.
    ///
    /// Returns in-flight tasks plus everything in the result store.
    /// Pending-but-not-yet-claimed tasks that the in-flight map doesn't yet
    /// know about (e.g. after a restart) are not surfaced.
    pub async fn list_tasks(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListTasksResult, McpError> {
        let mut tasks: Vec<Task> = Vec::new();

        {
            let map = self.in_flight.read().await;
            for (task_id, info) in map.iter() {
                let ts = Utc::now().to_rfc3339();
                let mut task = Task::new(
                    task_id.clone(),
                    TaskStatus::Working,
                    chrono::DateTime::<Utc>::from_timestamp_millis(info.started_at_ms as i64)
                        .map(|d| d.to_rfc3339())
                        .unwrap_or_else(|| ts.clone()),
                    ts,
                )
                .with_status_message("running");
                if let Some(ttl) = self.task_ttl_ms() {
                    task = task.with_ttl(ttl);
                }
                tasks.push(task);
            }
        }

        let finished = self.result_store.list().await.map_err(|e| {
            McpError::internal_error(format!("result store list failed: {e}"), None)
        })?;
        for record in finished {
            let ts = chrono::DateTime::<Utc>::from_timestamp_millis(record.completed_at_ms as i64)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| Utc::now().to_rfc3339());
            let mut task = Task::new(record.task_id, record.status.into(), ts.clone(), ts);
            if let Some(ttl) = self.task_ttl_ms() {
                task = task.with_ttl(ttl);
            }
            if let Some(err) = record.error {
                task = task.with_status_message(err);
            }
            tasks.push(task);
        }

        Ok(ListTasksResult::new(tasks))
    }

    /// Look up a task's current status.
    pub async fn get_task_info(
        &self,
        request: GetTaskInfoParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskResult, McpError> {
        let task_id = request.task_id;
        if let Some(task) = self.snapshot_task(&task_id).await? {
            Ok(GetTaskResult { meta: None, task })
        } else {
            Err(McpError::resource_not_found(
                format!("task not found: {task_id}"),
                None,
            ))
        }
    }

    /// Fetch a completed task's result.
    ///
    /// The coarse wait is notification-based: [`taquba::Queue::wait_for_completion`]
    /// blocks (no polling) until the queue job reaches a terminal state.
    /// taquba-mcp's result blob is then read as the authoritative outcome.
    ///
    /// The blob is normally present the moment `wait_for_completion` returns
    /// (workers write it *before* their terminal queue transition), but it
    /// can lag briefly in two cases: `cancel_task` writes the Cancelled blob
    /// around the queue removal, and the reaper can dead-letter a job whose
    /// worker is still running the tool. A short result-blob reconcile (the
    /// only place this method polls) covers that gap, bounded by the same
    /// `get_result_max_wait` budget.
    pub async fn get_task_result(
        &self,
        request: GetTaskResultParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskPayloadResult, McpError> {
        let task_id = request.task_id;
        let deadline = tokio::time::Instant::now() + self.config.get_result_max_wait;

        // Coarse wait; notify-based, no per-job polling.
        let _ = self
            .queue
            .wait_for_completion(&task_id, self.config.get_result_max_wait)
            .await
            .map_err(|e| {
                McpError::internal_error(format!("wait_for_completion failed: {e}"), None)
            })?;

        // Reconcile against the result store, which is taquba-mcp's
        // authoritative record of task outcomes.
        loop {
            if let Some(record) = self.result_store.get(&task_id).await.map_err(|e| {
                McpError::internal_error(format!("result store read failed: {e}"), None)
            })? {
                return payload_from_record(&task_id, record);
            }

            // No blob yet. If a worker is still on the job (its lease may
            // have been reaped out from under it while the tool runs), keep
            // waiting for the blob; otherwise the task is genuinely gone.
            let worker_running = self.in_flight.read().await.contains_key(&task_id)
                || self
                    .queue
                    .get_job(&task_id)
                    .await
                    .map_err(|e| {
                        McpError::internal_error(format!("queue lookup failed: {e}"), None)
                    })?
                    .is_some();
            if !worker_running {
                return Err(McpError::resource_not_found(
                    format!("task not found: {task_id}"),
                    None,
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(McpError::invalid_request(
                    format!("task result not yet available: {task_id}"),
                    None,
                ));
            }
            tokio::time::sleep(self.config.get_result_poll_interval).await;
        }
    }

    /// Cancel a task.
    ///
    /// - **Pending / Scheduled**: the queue job is removed and a terminal
    ///   `Cancelled` result blob is recorded. The blob is written *before*
    ///   the queue removal so a concurrent `get_task_result` observer sees
    ///   it the instant the removal fires the completion signal.
    /// - **Claimed (a worker is running the tool)**: cooperative
    ///   cancellation. [`taquba::Queue::cancel`] fires the job's
    ///   cancellation token, which the worker wired into the tool's
    ///   `RequestContext::ct`. A tool that watches `ctx.ct` short-circuits;
    ///   one that ignores it runs to completion. Either way the worker
    ///   writes the authoritative result blob, so `cancel_task` does not
    ///   write one here.
    /// - **Terminal / unknown**: returns `invalid_request` (already
    ///   finished) or `resource_not_found`.
    pub async fn cancel_task(
        &self,
        request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CancelTaskResult, McpError> {
        let task_id = request.task_id;

        // Peek to decide who owns the result blob: for a Pending/Scheduled
        // job no worker will ever run, so `cancel_task` records the
        // Cancelled outcome itself; for a Claimed job the worker owns it.
        let snapshot = self
            .queue
            .get_job(&task_id)
            .await
            .map_err(|e| McpError::internal_error(format!("queue lookup failed: {e}"), None))?;

        match snapshot.as_ref().map(|job| job.status) {
            Some(JobStatus::Pending) | Some(JobStatus::Scheduled) => {
                let tool = self
                    .in_flight
                    .write()
                    .await
                    .remove(&task_id)
                    .map(|info| info.tool)
                    .or_else(|| {
                        snapshot
                            .as_ref()
                            .and_then(|job| job.headers.get("tool").cloned())
                    })
                    .unwrap_or_default();
                // Write the Cancelled blob *before* removing the queue job.
                // If a worker races in and claims between the peek and the
                // cancel call, `cancel` returns `Requested` and that worker's
                // eventual result blob overwrites this one (last-write-wins,
                // worker outcome authoritative), so this is safe either way.
                self.record_cancelled(&task_id, &tool).await;
                let _ = self.queue.cancel(&task_id).await.map_err(|e| {
                    McpError::internal_error(format!("queue cancel failed: {e}"), None)
                })?;
                Ok(self.cancelled_result(task_id, None))
            }
            Some(JobStatus::Claimed) => {
                match self.queue.cancel(&task_id).await.map_err(|e| {
                    McpError::internal_error(format!("queue cancel failed: {e}"), None)
                })? {
                    CancelOutcome::Requested | CancelOutcome::Removed => {
                        // The cancellation token fired into the worker's
                        // `RequestContext::ct`. The worker owns the result
                        // blob; we only record the request in the audit log.
                        let tool = snapshot
                            .as_ref()
                            .and_then(|job| job.headers.get("tool").cloned())
                            .unwrap_or_default();
                        let _ = self
                            .audit_log
                            .record(AuditEntry {
                                event: AuditEvent::Cancelled,
                                task_id: task_id.clone(),
                                tool,
                                at_ms: self.now_ms(),
                                error: None,
                            })
                            .await;
                        Ok(self.cancelled_result(
                            task_id,
                            Some("cancellation requested; the tool may still be finishing"),
                        ))
                    }
                    CancelOutcome::NotFound => {
                        // The worker settled the job between our peek and the
                        // cancel call; fall through to the terminal check.
                        self.terminal_or_not_found(&task_id).await
                    }
                }
            }
            Some(JobStatus::Done) | Some(JobStatus::Dead) | None => {
                self.terminal_or_not_found(&task_id).await
            }
        }
    }

    /// Write a terminal `Cancelled` result blob and audit entry. Used by
    /// `cancel_task` for tasks cancelled while still Pending/Scheduled.
    async fn record_cancelled(&self, task_id: &str, tool: &str) {
        let now = self.now_ms();
        let record = ResultRecord {
            task_id: task_id.to_string(),
            tool: tool.to_string(),
            status: FinishedStatus::Cancelled,
            result: None,
            error: None,
            completed_at_ms: now,
            expires_at_ms: expires_at_ms(now, self.config.result_retention),
        };
        if let Err(e) = self.result_store.put(&record).await {
            tracing::warn!(task_id = %task_id, error = %e, "cancel result write failed");
        }
        let _ = self
            .audit_log
            .record(AuditEntry {
                event: AuditEvent::Cancelled,
                task_id: task_id.to_string(),
                tool: tool.to_string(),
                at_ms: now,
                error: None,
            })
            .await;
    }

    /// Build a `Cancelled` [`CancelTaskResult`], optionally with a
    /// human-readable status message.
    fn cancelled_result(&self, task_id: String, message: Option<&str>) -> CancelTaskResult {
        let now = Utc::now().to_rfc3339();
        let mut task = Task::new(task_id, TaskStatus::Cancelled, now.clone(), now);
        if let Some(message) = message {
            task = task.with_status_message(message);
        }
        if let Some(ttl) = self.task_ttl_ms() {
            task = task.with_ttl(ttl);
        }
        CancelTaskResult { meta: None, task }
    }

    /// For a task that the queue could not cancel: report `invalid_request`
    /// if it already reached a terminal state, otherwise `resource_not_found`.
    async fn terminal_or_not_found(
        &self,
        task_id: &str,
    ) -> std::result::Result<CancelTaskResult, McpError> {
        if let Some(record) =
            self.result_store.get(task_id).await.map_err(|e| {
                McpError::internal_error(format!("result store read failed: {e}"), None)
            })?
        {
            Err(McpError::invalid_request(
                format!(
                    "task is already in terminal state: {:?}",
                    TaskStatus::from(record.status),
                ),
                None,
            ))
        } else {
            Err(McpError::resource_not_found(
                format!("task not found: {task_id}"),
                None,
            ))
        }
    }

    async fn snapshot_task(&self, task_id: &str) -> std::result::Result<Option<Task>, McpError> {
        // 1. Terminal pointer, committed atomically with the worker's ack, is
        // the authoritative record for ack-settled outcomes (Completed /
        // cooperative Cancelled). It is checked *first* so it wins over a stale
        // in-flight entry: `enqueue_task` records in-flight after the job is
        // already claimable, so a fast worker can settle and clear before that
        // record lands, leaving an entry that never clears. It also beats the
        // result blob, which a worker writes *before* its ack, so a
        // still-claimed (and potentially re-running) job is never reported
        // terminal.
        if let Some(p) = pointer::read(&self.queue, task_id)
            .await
            .map_err(|e| McpError::internal_error(format!("pointer read failed: {e}"), None))?
        {
            let ts = ms_to_rfc3339(p.completed_at_ms);
            let mut task = Task::new(task_id.to_string(), p.status.into(), ts.clone(), ts);
            if let Some(ttl) = self.task_ttl_ms() {
                task = task.with_ttl(ttl);
            }
            return Ok(Some(task));
        }
        // 2. In-flight (a worker is actively running the tool).
        if let Some(info) = self.in_flight.read().await.get(task_id).cloned() {
            let started = ms_to_rfc3339(info.started_at_ms);
            let now = Utc::now().to_rfc3339();
            let mut task = Task::new(task_id.to_string(), TaskStatus::Working, started, now)
                .with_status_message("running");
            if let Some(ttl) = self.task_ttl_ms() {
                task = task.with_ttl(ttl);
            }
            return Ok(Some(task));
        }
        // 3. Live queue job: Claimed is still running (its result blob, if any,
        // is provisional until the ack settles the pointer above); Pending /
        // Scheduled is queued. Done and Dead fall through to the result blob
        // for the terminal record (and, for failures, the error message).
        if let Some(job) = self
            .queue
            .get_job(task_id)
            .await
            .map_err(|e| McpError::internal_error(format!("queue lookup failed: {e}"), None))?
        {
            let message = match job.status {
                JobStatus::Claimed => Some("running"),
                JobStatus::Pending | JobStatus::Scheduled => Some("queued"),
                JobStatus::Done | JobStatus::Dead => None,
            };
            if let Some(message) = message {
                let now = Utc::now().to_rfc3339();
                let mut task = Task::new(
                    task_id.to_string(),
                    TaskStatus::Working,
                    ms_to_rfc3339(job.enqueued_at),
                    now,
                )
                .with_status_message(message);
                if let Some(ttl) = self.task_ttl_ms() {
                    task = task.with_ttl(ttl);
                }
                return Ok(Some(task));
            }
        }
        // 4. Result blob: terminal outcomes that carry no pointer
        // (dead-lettered failures and tasks cancelled while still Pending),
        // plus a defensive fallback for any terminal whose queue job is gone.
        if let Some(record) =
            self.result_store.get(task_id).await.map_err(|e| {
                McpError::internal_error(format!("result store read failed: {e}"), None)
            })?
        {
            let ts = ms_to_rfc3339(record.completed_at_ms);
            let mut task = Task::new(task_id.to_string(), record.status.into(), ts.clone(), ts);
            if let Some(ttl) = self.task_ttl_ms() {
                task = task.with_ttl(ttl);
            }
            if let Some(err) = record.error {
                task = task.with_status_message(err);
            }
            return Ok(Some(task));
        }
        Ok(None)
    }

    fn task_ttl_ms(&self) -> Option<u64> {
        let ms = self.config.result_retention.as_millis();
        u64::try_from(ms).ok()
    }
}

/// Render a millisecond UNIX timestamp as an RFC 3339 string, falling back to
/// "now" if the value is somehow out of range.
fn ms_to_rfc3339(ms: u64) -> String {
    chrono::DateTime::<Utc>::from_timestamp_millis(ms as i64)
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

impl From<FinishedStatus> for TaskStatus {
    fn from(status: FinishedStatus) -> Self {
        match status {
            FinishedStatus::Completed => TaskStatus::Completed,
            FinishedStatus::Failed => TaskStatus::Failed,
            FinishedStatus::Cancelled => TaskStatus::Cancelled,
        }
    }
}

/// Translate a stored [`ResultRecord`] into the `tasks/result` payload:
/// the tool's `CallToolResult` for a success, or an MCP error for a
/// failed / cancelled task.
fn payload_from_record(
    task_id: &str,
    record: ResultRecord,
) -> std::result::Result<GetTaskPayloadResult, McpError> {
    match record.status {
        FinishedStatus::Completed => {
            let value = record.result.unwrap_or_default();
            let json = serde_json::to_value(value).map_err(|e| {
                McpError::internal_error(format!("failed to serialize tool result: {e}"), None)
            })?;
            Ok(GetTaskPayloadResult::new(json))
        }
        FinishedStatus::Failed => Err(McpError::internal_error(
            format!(
                "task failed: {}",
                record.error.unwrap_or_else(|| "(no error message)".into()),
            ),
            None,
        )),
        FinishedStatus::Cancelled => Err(McpError::invalid_request(
            format!("task was cancelled: {task_id}"),
            None,
        )),
    }
}

/// Builder for [`TaqubaTaskBackend`].
#[derive(Default)]
pub struct TaqubaTaskBackendBuilder {
    object_store: Option<Arc<dyn ObjectStore>>,
    prefix: Option<String>,
    config: TaqubaTaskBackendConfig,
    clock: Option<Arc<dyn Clock>>,
}

impl TaqubaTaskBackendBuilder {
    /// Set the object store the backend will use for both the underlying
    /// Taquba queue and the result / audit blob storage.
    pub fn object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the path prefix under which all `taquba-mcp` state lives. The
    /// backend writes:
    ///
    /// - `<prefix>/queue/...`: the Taquba queue's SlateDB store.
    /// - `<prefix>/results/<task_id>.json`: completed task results.
    /// - `<prefix>/audit/<ulid>.json`: audit entries.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Override the full configuration. Most users will prefer the
    /// individual setters below.
    pub fn config(mut self, config: TaqubaTaskBackendConfig) -> Self {
        self.config = config;
        self
    }

    /// Set how long completed result blobs are retained.
    pub fn result_retention(mut self, retention: Duration) -> Self {
        self.config.result_retention = retention;
        self
    }

    /// Set how long audit log blobs are retained.
    pub fn audit_retention(mut self, retention: Duration) -> Self {
        self.config.audit_retention = retention;
        self
    }

    /// Set the default retry policy applied to tool jobs.
    pub fn default_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.config.default_retry_policy = policy;
        self
    }

    /// Set the number of concurrent workers per tool queue.
    pub fn workers_per_tool(mut self, n: usize) -> Self {
        self.config.workers_per_tool = n;
        self
    }

    /// Disable the in-process reaper. Use when retention is managed
    /// externally (e.g. by an object-store lifecycle policy or a separate
    /// reaper process).
    pub fn disable_reaper(mut self) -> Self {
        self.config.enable_reaper = false;
        self
    }

    /// Override the time source. The same [`Clock`] is threaded into
    /// Taquba's [`OpenOptions::clock`](taquba::OpenOptions::clock) so the
    /// queue and taquba-mcp's audit/result/in-flight timestamps share one
    /// view of "now."
    ///
    /// Production callers leave this unset (the default is
    /// [`taquba::SystemClock`]). Tests can pass a [`taquba::MockClock`] to
    /// advance time deterministically.
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Open the Taquba queue and finalize the backend.
    pub async fn build(self) -> Result<Arc<TaqubaTaskBackend>> {
        let object_store = self
            .object_store
            .ok_or_else(|| Error::Configuration("object_store is required".into()))?;
        let prefix_str = self
            .prefix
            .ok_or_else(|| Error::Configuration("prefix is required".into()))?;

        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));

        let queue_path = format!("{}/queue", prefix_str.trim_end_matches('/'));
        let queue = Queue::open_with_options(
            object_store.clone(),
            &queue_path,
            OpenOptions {
                default_queue_config: self.config.default_retry_policy.to_queue_config(),
                clock: clock.clone(),
                ..OpenOptions::default()
            },
        )
        .await?;
        let prefix = Path::from(prefix_str);
        let result_store = ResultStore::new(object_store.clone(), prefix.clone(), clock.clone());
        let audit_log = AuditLog::new(object_store, prefix, clock.clone());

        Ok(Arc::new(TaqubaTaskBackend {
            queue: Arc::new(queue),
            result_store,
            audit_log,
            in_flight: RwLock::new(InFlight::default()),
            config: self.config,
            clock,
        }))
    }
}
