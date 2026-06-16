//! Persistent storage for completed task results, backed by the object store.
//!
//! On-store layout:
//!
//! ```text
//! <prefix>/results/<task_id>.json
//! ```
//!
//! One blob per finished task. The reaper lists the prefix and deletes
//! entries whose `expires_at_ms` is in the past.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use rmcp::model::CallToolResult;
use serde::{Deserialize, Serialize};
use taquba::Clock;
use taquba::object_store::{ObjectStore, PutPayload, path::Path};

use crate::error::Result;

/// What we wrote to the object store for a finished task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinishedStatus {
    Completed,
    Failed,
    Cancelled,
}

/// On-store record of a finished task. Reaper looks at `expires_at_ms` only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResultRecord {
    pub task_id: String,
    pub tool: String,
    pub status: FinishedStatus,
    /// JSON-serialized `CallToolResult` for successful runs. `None` on
    /// `Failed` / `Cancelled`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<CallToolResult>,
    /// Error message for `Failed` runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub completed_at_ms: u64,
    pub expires_at_ms: u64,
}

/// Object-store-backed result store.
#[derive(Clone)]
pub(crate) struct ResultStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    clock: Arc<dyn Clock>,
}

impl ResultStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: Path, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            prefix,
            clock,
        }
    }

    fn key(&self, task_id: &str) -> Path {
        self.prefix
            .child("results")
            .child(format!("{}.json", task_id))
    }

    /// Write a finished-task record. Idempotent (last write wins).
    pub(crate) async fn put(&self, record: &ResultRecord) -> Result<()> {
        let bytes = serde_json::to_vec(record)?;
        self.store
            .put(
                &self.key(&record.task_id),
                PutPayload::from(Bytes::from(bytes)),
            )
            .await?;
        Ok(())
    }

    /// Fetch a finished-task record by task id, if present.
    pub(crate) async fn get(&self, task_id: &str) -> Result<Option<ResultRecord>> {
        match self.store.get(&self.key(task_id)).await {
            Ok(obj) => {
                let bytes = obj.bytes().await?;
                let record: ResultRecord = serde_json::from_slice(&bytes)?;
                Ok(Some(record))
            }
            Err(taquba::object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a record. Used when retention expires.
    #[allow(dead_code)]
    pub(crate) async fn delete(&self, task_id: &str) -> Result<()> {
        self.store.delete(&self.key(task_id)).await?;
        Ok(())
    }

    /// List all finished-task records under the prefix.
    pub(crate) async fn list(&self) -> Result<Vec<ResultRecord>> {
        let prefix = self.prefix.child("results");
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let obj = self.store.get(&meta.location).await?;
            let bytes = obj.bytes().await?;
            match serde_json::from_slice::<ResultRecord>(&bytes) {
                Ok(record) => out.push(record),
                Err(e) => tracing::warn!(
                    location = %meta.location,
                    error = %e,
                    "skipping unreadable result blob",
                ),
            }
        }
        Ok(out)
    }

    /// Delete every record whose `expires_at_ms` is in the past. Called by
    /// the periodic reaper task. Returns the task ids that were swept so the
    /// caller can drop their terminal KV pointers in step.
    pub(crate) async fn reap(&self) -> Result<Vec<String>> {
        let now_ms = self.clock.now_ms();
        let mut deleted = Vec::new();
        for record in self.list().await? {
            if record.expires_at_ms <= now_ms {
                if let Err(e) = self.delete(&record.task_id).await {
                    tracing::warn!(task_id = %record.task_id, error = %e, "reap failed");
                } else {
                    deleted.push(record.task_id);
                }
            }
        }
        Ok(deleted)
    }
}

pub(crate) fn expires_at_ms(now_ms: u64, retention: Duration) -> u64 {
    now_ms.saturating_add(retention.as_millis() as u64)
}
