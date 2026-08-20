# Khepri patch set

Base: upstream `databendlabs/openraft` `v0.9.25` (`8815cdba2826f74e848acef361ad03f93bb1c3f8`).

This repository distributes the patched crate as `waterbase-openraft` version `1.0.0`. It is an
internal Git dependency and is intentionally not published to crates.io.

## Bounded committed apply

Upstream `RaftCore::apply_to_state_machine()` reads a complete committed gap before submitting it
to the state-machine worker. That bypasses the storage reader's bounded
`limited_get_log_entries()` contract and can retain an unbounded gap in memory.

This patch is limited to four upstream files:

- `openraft/src/core/raft_core.rs` reads and applies one bounded chunk at a time.
- `openraft/src/core/sm/command.rs` adds an internal callback for one bounded chunk.
- `openraft/src/core/sm/worker.rs` returns that callback after durable state-machine apply.
- `openraft/Cargo.toml` keeps only the macro dependency outside the downstream fork.

`RaftCore` validates every returned chunk is non-empty, contiguous and in range, then waits for
its state-machine callback before reading the next chunk. A malformed reader fails closed.

The patch does not change public OpenRaft APIs, on-disk formats, or wire formats. Khepri overrides
`limited_get_log_entries()` with an entry and byte bound; its integration test is
`service::tests::raft::applies_a_committed_gap_in_byte_bounded_chunks`.

On upgrade, rebase this patch and retain that integration test. Do not remove the patch merely
because replication itself uses `limited_get_log_entries()`; the committed-apply path is separate.
