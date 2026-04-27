# @snaapi/queue

A Postgres backed job queue for Deno. Designed for Deno Deploy and any
environment where you need durable, multi worker job processing. Uses
`LISTEN`/`NOTIFY` for low latency wakeups and `SELECT FOR UPDATE SKIP LOCKED`
for safe concurrent reservation.

## Install

```ts
import { PostgresDriver, Queue } from "@snaapi/queue";
```

## Quick Start

```ts
import { PostgresDriver, Queue } from "@snaapi/queue";
import type { JobHandler } from "@snaapi/queue";

const driver = new PostgresDriver({
  connectionString: Deno.env.get("DATABASE_URL")!,
});
const queue = new Queue(driver);

const sendEmail: JobHandler<{ to: string; subject: string }> = {
  handle(payload, ctx) {
    console.log(
      `[${ctx.id}] Sending email to ${payload.to}: ${payload.subject}`,
    );
  },
};

queue.register("send-email", sendEmail);
queue.listen();

await queue.dispatch("send-email", {
  to: "user@example.com",
  subject: "Welcome!",
}).send();
```

Note that `queue.listen()` attaches to the backing store and must run in the
same long lived process. A script that only dispatches jobs and then exits will
enqueue work, but nothing will consume it until a process with `listen()` is
running against the same database.

## Driver

```ts
import { PostgresDriver, Queue } from "@snaapi/queue";

const driver = new PostgresDriver({
  connectionString: Deno.env.get("DATABASE_URL")!,
  pollIntervalMs: 1000, // fallback when LISTEN/NOTIFY misses (default 1000)
  concurrency: 1, // in-flight jobs per listener (default 1)
  reserveTtlMs: 5 * 60_000, // how long a worker can hold a job before peers can reclaim it
  tablePrefix: "snaapi_", // see "Table prefix" below
});
const queue = new Queue(driver);
```

The driver wakes up via `LISTEN`/`NOTIFY` when a new job is enqueued and falls
back to a polling timer for delayed jobs. Multiple worker processes can
`listen()` against the same database. `SELECT FOR UPDATE SKIP LOCKED` ensures
each ready job is reserved by exactly one worker. Reserved jobs are deleted only
after the handler returns, so a worker crash leaves the row reserved until
`reserveTtlMs` elapses, at which point a peer can reclaim it.

### Auto migration

The Postgres driver runs `migrate()` lazily on first use. The schema is
idempotent (`CREATE TABLE IF NOT EXISTS`), so it is safe to leave on across
deploys. Disable it (and run migrations as a separate step) by passing
`autoMigrate: false`:

```ts
const driver = new PostgresDriver({
  connectionString: Deno.env.get("DATABASE_URL")!,
  autoMigrate: false,
});
await driver.migrate(); // run explicitly if you prefer
```

The standalone task is also available:

```bash
DATABASE_URL=postgres://... deno task db:migrate
```

### Table prefix

By default the driver creates four tables: `snaapi_jobs`, `snaapi_failed_jobs`,
`snaapi_locks`, `snaapi_counters`. The prefix (default `snaapi_`) is
configurable via the `tablePrefix` option or the `QUEUE_TABLE_PREFIX` env var.
The prefix must match `[a-zA-Z_][a-zA-Z0-9_]*`.

```ts
new PostgresDriver({
  connectionString: url,
  tablePrefix: "myapp_",
  // tables become myapp_jobs, myapp_failed_jobs, myapp_locks, myapp_counters
});
```

### Local development

A `compose.yaml` ships with the project:

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://queue:queue@localhost:5444/queue
deno task test
```

### Migrating from 1.x

Versions before 2.0 took a `Deno.Kv` directly and exposed `ctx.kv` to handlers.
The 2.x line drops Deno KV support entirely (KV Connect on Deno Deploy does not
support `enqueue`). Pass a `PostgresDriver` to the `Queue` constructor and use
`ctx.locks` / `ctx.counters` from middleware instead of `ctx.kv`. Pending jobs
do not migrate between stores; drain the KV queue before cutting over.

## Dispatch Options

The `dispatch()` method returns a builder. Chain options before calling
`.send()`.

```ts
await queue
  .dispatch("send-email", { to: "user@example.com", subject: "Hello" })
  .delay(5000) // wait 5 seconds before delivery (default: 0)
  .onQueue("emails") // named queue (default: "default")
  .attempts(5) // max retry attempts (default: 3)
  .backoff([1000, 5000, 30000]) // backoff delays in ms (default: [1000, 5000, 30000])
  .unique("welcome-user@example.com", 60_000) // prevent duplicates (default TTL: 300000 ms / 5 min)
  .send();
```

### Defaults

| Option     | Default                |
| ---------- | ---------------------- |
| `onQueue`  | `"default"`            |
| `attempts` | `3`                    |
| `backoff`  | `[1000, 5000, 30000]`  |
| `unique`   | TTL `300_000` ms (5 m) |
| `delay`    | `0`                    |

## Middleware

Middleware wraps job execution. Global middleware runs for every job. Per job
middleware runs only for matching jobs.

```ts
// Global middleware (runs for all jobs)
queue.use(async (ctx, payload, next) => {
  console.log(`Starting ${ctx.jobName}`);
  const start = Date.now();
  await next();
  console.log(`Finished ${ctx.jobName} in ${Date.now() - start}ms`);
});

// Per job middleware
queue.middleware("send-email", async (ctx, payload, next) => {
  console.log(`Email attempt ${ctx.attempt} of ${ctx.maxAttempts}`);
  await next();
});
```

### Built in Middleware

**Rate limiting** restricts how many times a job can run within a time window.

```ts
import { rateLimit } from "@snaapi/queue";

queue.middleware(
  "send-email",
  rateLimit({ key: "emails", maxPerWindow: 100, windowMs: 60_000 }),
);
```

**Without overlapping** prevents concurrent execution of the same job.

```ts
import { withoutOverlapping } from "@snaapi/queue";

queue.middleware(
  "generate-report",
  withoutOverlapping({ key: "report", expiresIn: 120_000 }),
);
```

## Job Chains

Run jobs sequentially. Each step executes only after the previous one succeeds.

```ts
await queue.chain([
  {
    jobName: "download-file",
    payload: { url: "https://example.com/data.csv" },
  },
  { jobName: "parse-csv", payload: { file: "/tmp/data.csv" } },
  { jobName: "send-report", payload: { to: "team@example.com" } },
]);
```

## Failed Jobs

Jobs that exhaust all retry attempts are persisted to the backing store. You can
inspect, retry, or remove them.

```ts
// List failed jobs
const failed = await queue.failed.list("default");

// Get a specific failed job
const job = await queue.failed.get("job-id");

// Retry a single failed job
await queue.failed.retry("job-id");

// Retry all failed jobs in a queue
const count = await queue.failed.retryAll("default");

// Remove a failed job without retrying
await queue.failed.forget("job-id");

// Purge all failed jobs
await queue.failed.purge("default");
```

## Job Context

Every handler receives a `JobContext` with metadata about the current execution.

```ts
interface JobContext {
  jobName: string; // registered job name
  attempt: number; // current attempt (1 based)
  maxAttempts: number; // configured max attempts
  queue: string; // queue name
  id: string; // unique dispatch ID
  locks: LockDriver; // mutex primitive used by withoutOverlapping
  counters: CounterDriver; // counter primitive used by rateLimit
}
```

## License

MIT
