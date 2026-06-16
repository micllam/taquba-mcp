# Changelog

All notable changes to the `taquba-mcp` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Successful and cancelled tasks now settle through `taquba::Queue::ack_with`,
  committing a terminal status pointer to taquba's KV namespace in the same
  transaction as the job ack. Adds one small KV entry per ack-settled task.

### Fixed

- `get_task_info` no longer reports a task `Completed` / `Cancelled` while its
  job is still claimed and could re-run: terminal reads consult the atomically
  committed pointer rather than the provisional result blob, and a stale
  in-flight entry can no longer mask a settled task.

## [0.1.0] - 2026-05-14

Initial release.
