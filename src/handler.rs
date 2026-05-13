//! The `taquba_task_handler!` declarative macro.

/// Generate `enqueue_task` / `list_tasks` / `get_task_info` /
/// `get_task_result` / `cancel_task` method bodies inside an
/// `impl ServerHandler for ...` block, delegating to a
/// [`crate::TaqubaTaskBackend`] reachable as `self.<field>`.
///
/// `rmcp` 1.7 ships a `#[task_handler]` proc macro, but its expansion is
/// hardcoded to `OperationProcessor`'s sync, in-memory shape (it calls
/// `.lock().await` once on a `Mutex<OperationProcessor>` and then chains
/// synchronous method calls). Our taquba-backed operations are inherently
/// async (queue I/O, object-store I/O), so we ship our own delegator instead.
///
/// # Usage
///
/// ```ignore
/// use std::sync::Arc;
/// use rmcp::{ServerHandler, tool_handler};
/// use taquba_mcp::{TaqubaTaskBackend, taquba_task_handler};
///
/// #[derive(Clone)]
/// struct MyServer {
///     tasks: Arc<TaqubaTaskBackend>,
///     // ... other state ...
/// }
///
/// #[tool_handler]
/// impl ServerHandler for MyServer {
///     taquba_task_handler!(tasks);
/// }
/// ```
///
/// The argument is the name of the field on `self` that holds the backend.
/// `Arc<TaqubaTaskBackend>` is the typical type. If you keep the backend
/// somewhere other than a direct field of `Self`, write the five `async fn`
/// methods by hand, each taking one line that calls into the backend.
#[macro_export]
macro_rules! taquba_task_handler {
    ($field:ident) => {
        async fn enqueue_task(
            &self,
            request: $crate::rmcp::model::CallToolRequestParams,
            context: $crate::rmcp::service::RequestContext<$crate::rmcp::RoleServer>,
        ) -> ::core::result::Result<$crate::rmcp::model::CreateTaskResult, $crate::rmcp::ErrorData>
        {
            self.$field.enqueue_task(request, context).await
        }

        async fn list_tasks(
            &self,
            request: ::core::option::Option<$crate::rmcp::model::PaginatedRequestParams>,
            context: $crate::rmcp::service::RequestContext<$crate::rmcp::RoleServer>,
        ) -> ::core::result::Result<$crate::rmcp::model::ListTasksResult, $crate::rmcp::ErrorData> {
            self.$field.list_tasks(request, context).await
        }

        async fn get_task_info(
            &self,
            request: $crate::rmcp::model::GetTaskInfoParams,
            context: $crate::rmcp::service::RequestContext<$crate::rmcp::RoleServer>,
        ) -> ::core::result::Result<$crate::rmcp::model::GetTaskResult, $crate::rmcp::ErrorData> {
            self.$field.get_task_info(request, context).await
        }

        async fn get_task_result(
            &self,
            request: $crate::rmcp::model::GetTaskResultParams,
            context: $crate::rmcp::service::RequestContext<$crate::rmcp::RoleServer>,
        ) -> ::core::result::Result<
            $crate::rmcp::model::GetTaskPayloadResult,
            $crate::rmcp::ErrorData,
        > {
            self.$field.get_task_result(request, context).await
        }

        async fn cancel_task(
            &self,
            request: $crate::rmcp::model::CancelTaskParams,
            context: $crate::rmcp::service::RequestContext<$crate::rmcp::RoleServer>,
        ) -> ::core::result::Result<$crate::rmcp::model::CancelTaskResult, $crate::rmcp::ErrorData>
        {
            self.$field.cancel_task(request, context).await
        }
    };
}
