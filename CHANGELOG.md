# Changelog

## [Unreleased]

### Fixed

- `PostgresDriver.listen()` no longer crashes the host process when the worker
  loop hits a connection error. The poll loop catches driver errors, surfaces
  them through a new `onError` option (defaulting to `console.error`), and backs
  off (capped at 30 seconds) before retrying instead of escaping as an unhandled
  rejection.

### Added

- `PostgresDriverOptions.onError`. Invoked with any error raised by the polling
  loop or the `LISTEN`/`NOTIFY` client.

## [2.0.0] - 2026-04-27

### Fixed

- fix: decouple jobs row PK from envelope id

### Breaking

- Removed the Deno KV driver and the `createQueue()` factory. The queue now runs
  exclusively on Postgres. Construct a `PostgresDriver` and pass it to
  `new Queue(driver)`. KV Connect on Deno Deploy did not support `enqueue`,
  which made the dual driver story misleading on the platform we care about
  most.
- `JobContext` no longer exposes `kv`. Use `ctx.locks` and `ctx.counters` from
  middleware.
- `FailedJobDriver.store(record)` is now `store(record, envelope)`. Drivers must
  persist the original `QueueEnvelope` so retries reconstruct dispatch metadata
  (max attempts, backoff schedule, unique key/ttl, chain).

### Added

- `PostgresDriver` with `LISTEN`/`NOTIFY` wakeups, polling fallback, and
  `SELECT FOR UPDATE SKIP LOCKED` reservation.
- `reserveTtlMs` option. Reserved jobs are deleted only after the handler
  returns, so a worker crash leaves the row reserved until peers can reclaim it.
- `tablePrefix` option (and `QUEUE_TABLE_PREFIX` env var) for sharing a database
  with other applications.
- Auto migration on first use, with a standalone `deno task db:migrate` task and
  `compose.yaml` for local development.

### Changed

- Unique dispatch is now enforced via a TTL row in the locks table rather than a
  row level constraint. The dedupe window now matches the requested `uniqueTtl`
  instead of ending when the job is reserved.
- The `jobs` ready index dropped its partial `WHERE reserved_at IS NULL`
  predicate so the reservation query (which also reclaims expired reservations)
  can use it.
- `PgFailedStore.retry()` rebuilds the envelope from the stored `QueueEnvelope`,
  preserving `maxAttempts`, `backoffSchedule`, `uniqueKey`, `uniqueTtl`, and
  `chain`.

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
