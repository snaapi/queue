# Changelog

## [1.0.4] - 2026-04-19

### Other

- add license

## [1.0.0] - 2026-04-18

Initial release.

### Added

- `Queue` built on Deno KV with no external dependencies.
- Fluent dispatch builder with `delay`, `onQueue`, `attempts`, `backoff`, and
  `unique`.
- Global and per job middleware, including built in `rateLimit` and
  `withoutOverlapping`.
- Sequential job chains via `queue.chain()`.
- Failed job store with list, get, retry, retryAll, forget, and purge.
- `JobContext` exposing job name, attempt, max attempts, queue, id, and KV
  handle.
