//! Terminal task pointer, written to Taquba's user KV namespace in the *same
//! transaction* as the job ack via [`taquba::Queue::ack_with`].
//!
//! This is where taquba-mcp uses Taquba's transactional settlement. A
//! worker finishing a task writes the (potentially large) result payload blob
//! to object storage and then settles the job with `ack_with`, committing this
//! small terminal pointer atomically with the acknowledgement. The pointer
//! (not the result blob) authoritatively records whether a task reached a
//! terminal ack state. Because the blob is written *before* the queue
//! transition but the pointer is committed *with* it, reading the pointer first
//! stops [`get_task_info`](crate::TaqubaTaskBackend::get_task_info) from ever
//! reporting a Completed status for a job that is still claimed and could yet
//! re-run.
//!
//! Only the pointer is written in the transaction; the result payload stays
//! in the object store because Taquba KV values are capped at
//! [`taquba::MAX_KV_VALUE_SIZE`]. Enumeration (`list_tasks`, the reaper) stays
//! on object storage too; the pointer is a by-id consistency check, not an
//! index.
//!
//! Pointers exist only for ack-path terminals: a worker `Completed` run or a
//! cooperative `Cancelled` one. The dead-letter (`Failed`) and
//! cancel-while-`Pending` paths do not ack the job, so they carry no pointer
//! and are read back from the result blob.

use serde::{Deserialize, Serialize};
use taquba::Queue;

use crate::error::Result;
use crate::result_store::FinishedStatus;

/// Key under which a task's terminal pointer lives. Taquba internally scopes
/// caller keys under its reserved `usr:` prefix, so this cannot collide with
/// queue-internal state.
pub(crate) fn pointer_key(task_id: &str) -> Vec<u8> {
    format!("mcp:task:{task_id}").into_bytes()
}

/// Terminal pointer for an ack-settled task. Carries just enough to answer
/// `get_task_info` without reading the result blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskPointer {
    /// Terminal status. Only ever `Completed` or `Cancelled` (the ack paths).
    pub status: FinishedStatus,
    pub completed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl TaskPointer {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}

/// Read a task's terminal pointer from Taquba KV, if present.
pub(crate) async fn read(queue: &Queue, task_id: &str) -> Result<Option<TaskPointer>> {
    match queue.kv_get(&pointer_key(task_id)).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}
