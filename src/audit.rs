//! Persistent audit log of task lifecycle events, backed by the object store.
//!
//! On-store layout:
//!
//! ```text
//! <prefix>/audit/<ulid>.json
//! ```
//!
//! Each event gets its own ULID. ULIDs are lexicographically sortable by
//! creation time, so `list` over the prefix returns events oldest-first
//! and the reaper can decide retention by parsing the timestamp out of the
//! key; no GET needed.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use taquba::object_store::{ObjectStore, PutPayload, path::Path};
use ulid::Ulid;

use crate::error::Result;
use crate::result_store::now_ms;

/// Lifecycle event recorded into the audit log.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuditEvent {
    Submitted,
    Started,
    Completed,
    Failed,
    Cancelled,
}

/// One audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuditEntry {
    pub event: AuditEvent,
    pub task_id: String,
    pub tool: String,
    pub at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Object-store-backed audit log.
#[derive(Clone)]
pub(crate) struct AuditLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl AuditLog {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }

    fn key(&self, ulid: &Ulid) -> Path {
        self.prefix.child("audit").child(format!("{}.json", ulid))
    }

    /// Append one audit event. The ULID embedded in the filename carries the
    /// creation time; the reaper uses it to decide retention without
    /// reading any blob contents.
    pub(crate) async fn record(&self, entry: AuditEntry) -> Result<()> {
        let ulid = Ulid::new();
        let bytes = serde_json::to_vec(&entry)?;
        self.store
            .put(&self.key(&ulid), PutPayload::from(Bytes::from(bytes)))
            .await?;
        Ok(())
    }

    /// Delete every audit entry whose ULID timestamp is older than `retention`.
    pub(crate) async fn reap(&self, retention: Duration) -> Result<usize> {
        let cutoff_ms = now_ms().saturating_sub(retention.as_millis() as u64);
        let prefix = self.prefix.child("audit");
        let mut stream = self.store.list(Some(&prefix));
        let mut deleted = 0usize;
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            // Strip the trailing ".json" and parse the filename as a ULID.
            let filename = match meta.location.filename() {
                Some(name) => name,
                None => continue,
            };
            let ulid_str = match filename.strip_suffix(".json") {
                Some(s) => s,
                None => continue,
            };
            let ulid = match Ulid::from_string(ulid_str) {
                Ok(u) => u,
                Err(_) => continue,
            };
            if ulid.timestamp_ms() < cutoff_ms {
                if let Err(e) = self.store.delete(&meta.location).await {
                    tracing::warn!(location = %meta.location, error = %e, "audit reap failed");
                } else {
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }
}
