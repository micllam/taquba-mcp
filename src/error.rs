//! Error types.

use thiserror::Error;

/// Convenience alias for `Result<T, taquba_mcp::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that surface out of `taquba-mcp` setup and operation.
///
/// Per-request task failures are returned to MCP clients as
/// [`rmcp::ErrorData`] and do not flow through this type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An operation on the underlying Taquba queue failed.
    #[error("taquba: {0}")]
    Taquba(#[from] taquba::Error),

    /// An object-store operation failed.
    #[error("object store: {0}")]
    ObjectStore(#[from] taquba::object_store::Error),

    /// A required builder field was missing.
    #[error("configuration: {0}")]
    Configuration(String),

    /// A payload or result blob could not be serialized or deserialized.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}
