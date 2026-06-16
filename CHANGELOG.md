# Changelog

All notable changes to the `taquba-mcp` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TaqubaTaskBackendBuilder::clock` and re-exports of taquba's `Clock`,
  `MockClock`, and `SystemClock`. Callers can inject a time source that drives
  every state-transition timestamp, so tests can advance time deterministically
  with a `MockClock`.

### Changed

- Bumped `taquba` to 0.8. Pre-1.0: the on-store queue layout may differ from the
  0.4-based 0.1.0 release; drain in-flight tasks before upgrading.
- Successful and cancelled tasks now settle through `taquba::Queue::ack_with`,
  committing a terminal status pointer to taquba's KV namespace in the same
  transaction as the job ack. Adds one small KV entry per ack-settled task.

### Fixed

- `get_task_info` no longer reports a task `Completed` / `Cancelled` while its
  job is still claimed and could re-run: terminal reads consult the atomically
  committed pointer rather than the provisional result blob, and a stale
  in-flight entry can no longer mask a settled task.
- `list_tasks` no longer shows a phantom "running" duplicate for a completed
  task. `enqueue_task` now assigns the task id (via taquba's `id_override`) and
  records the in-flight entry before the job is claimable, closing a race where
  a fast worker could settle and clear the entry before it was written.

## [0.1.0] - 2026-05-14

Initial release.
