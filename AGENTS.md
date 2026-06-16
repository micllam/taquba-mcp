# AGENTS.md

This file provides guidance to agents when working with code in this repository.

Layout: a single flat crate — `Cargo.toml` + `src/` + `examples/` at the repo root. No Cargo workspace; reintroduce one only if sibling crates are ever added.

## What this crate is

A durable backend for [rmcp](https://crates.io/crates/rmcp)'s task subsystem, backed by [Taquba](https://crates.io/crates/taquba). `rmcp` 1.7 ships an in-memory `OperationProcessor`; `taquba-mcp` provides a `TaqubaTaskBackend` that persists task state across restarts via Taquba + the same object store Taquba uses.

The pitch is deliberately narrow: *durable backend for rmcp's task subsystem*, not "production-grade MCP server infrastructure." Resist scope creep.

## Build / test

End-to-end coverage is in `src/integration_tests.rs` (in-crate rather than `tests/` because it wires up an internal worker pool, `crate::worker::run_workers`).

The `aws` / `gcp` / `azure` features are mutually exclusive — they forward to `taquba`'s same-named features. Pick one per deployment.

Canonical check (run locally before pushing):

```bash
cargo fmt --all
cargo clippy --all-features --all-targets -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

## Architectural invariants

These constrain almost every design decision; violating them breaks correctness.

- **Single-process, single-writer.** Inherited from Taquba (SlateDB single-writer per store). All producers and workers for a given `TaqubaTaskBackend` must live in the same process and share one `Arc<TaqubaTaskBackend>`. Do not propose multi-node worker fleets.
- **Build on rmcp, don't reimplement.** Protocol, transports, tool registration, schema validation, OAuth: all are `rmcp`'s job. `taquba-mcp` adds *only* the durable task backend and the macro that wires it in.
- **MCP task semantics drive the design.** Tools declare `execution(task_support = "required" | "optional")` via `rmcp::tool`; clients create a task by augmenting a `tools/call` (there is no `tasks/create` method) and then drive it via MCP's native `tasks/get` / `tasks/result` / `tasks/list` / `tasks/cancel` requests (the 2025-11-25 / SEP-1686 surface rmcp 1.7 implements; the method is `tasks/get`, not `tasks/info`). Do not invent a parallel `__taquba_get_result` polling tool, sync/async builder split, or progress mechanism.
- **`#[task_handler]` is not reusable for us.** `rmcp`'s proc macro is hardcoded to `OperationProcessor`'s sync, in-memory shape (no awaits between `lock().await` and return). Use the `taquba_task_handler!` declarative macro shipped here instead.
- **Object_store direct, no separate KV layer.** Result and audit blobs land at `<prefix>/results/<task_id>.json` and `<prefix>/audit/<ulid>.json`. We reuse the `Arc<dyn ObjectStore>` the user passes in (the same one Taquba opens its queue on). No SlateDB second-store; no `opendata-keyvalue` dep. Revisit only if write rates demand it.
- **Pre-1.0:** minor bumps may break source compat *and* on-store layout; patch bumps preserve both.

## Misc

- Content parity between `lib.rs` top-level `//!` docstring and `README.md` is expected: anything substantive — new sections, design notes, semantics callouts — lands in both. Format may differ: `lib.rs` uses intra-doc `[Foo]` links and `# `-hidden rustdoc lines inside doctests; `README.md` uses URL links and copy-pasteable code blocks.
- Worker errors: returning `taquba::PermanentFailure` from a tool handler dead-letters the job immediately; any other error nacks and retries per the configured `RetryPolicy`.
- Status mapping: `taquba::JobStatus` (Pending/Claimed/Done/Failed/Scheduled/DeadLettered) maps onto `rmcp::model::TaskStatus` (Submitted/Working/Completed/Failed/Cancelled). The translation table lives next to the `get_task_info` implementation; keep it in one place.
- One tool ↔ one Taquba queue. Queue name = tool name. Per-tool `QueueConfig` overrides (lease, retry) are a 0.2 feature.
