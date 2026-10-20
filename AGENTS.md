# Low-latency HFT in Rust

This project is an HFT system written in Rust. Priorities: correctness, predictable latency, then throughput.

## Development

- Consise and succinct code, keep changes focused on the task; avoid unnecessary dependencies and abstractions.
- Avoid allocations, locks, blocking I/O, and synchronous logging on the hot path.
- Use preallocated buffers and bounded queues; define overflow behavior explicitly.
- Represent prices and quantities as integer ticks/lots or fixed-point values; check overflow and rounding.
- Do not use `unwrap()` / `expect()` on external input; handle errors explicitly.
- Prefer safe Rust. Document safety invariants and justify each use of `unsafe`.
- Make data ownership explicit in concurrent code and justify atomic memory orderings.

## Validation

- Keep only tests that verify meaningful project behavior, domain invariants, failure handling, or concrete regressions.
- Do not add tests that merely check getters/setters, assignments, dependency constructors, framework-generated help, or duplicate existing coverage. Avoid tests that mirror implementation details or manufacture unreachable states solely for coverage.
- Test edge cases, invalid input, and trading logic invariants; prefer observable outcomes through public APIs.
- Once a Cargo project exists, run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`.
- Validate optimizations with release builds: compare p50/p99/p99.9 latency and throughput under the same workload and environment.
- Briefly report changes, checks performed, and limitations; do not claim speedups without measurements.
