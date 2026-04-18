# @snaapi/queue

A job queue for Deno built on top of Deno KV. No external dependencies, no
separate queue server. Jobs are dispatched, persisted, retried, and chained
using the KV queue primitives that ship with Deno.

## Install

```ts
import { Queue } from "@snaapi/queue";
```

## Quick Start

```ts
import { Queue } from "@snaapi/queue";
import type { JobHandler } from "@snaapi/queue";

const kv = await Deno.openKv();

const sendEmail: JobHandler<{ to: string; subject: string }> = {
  handle(payload, ctx) {
    console.log(
      `[${ctx.id}] Sending email to ${payload.to}: ${payload.subject}`,
    );
  },
};

const queue = new Queue(kv);
queue.register("send-email", sendEmail);
queue.listen();

await queue.dispatch("send-email", {
  to: "user@example.com",
  subject: "Welcome!",
}).send();
```

## Dispatch Options

The `dispatch()` method returns a builder. Chain options before calling
`.send()`.

```ts
await queue
  .dispatch("send-email", { to: "user@example.com", subject: "Hello" })
  .delay(5000) // wait 5 seconds before delivery
  .onQueue("emails") // named queue (default: "default")
  .attempts(5) // max retry attempts (default: 3)
  .backoff([1000, 5000, 30000]) // backoff delays between retries
  .unique("welcome-user@example.com", 60_000) // prevent duplicates for 60s
  .send();
```

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

Jobs that exhaust all retry attempts are stored in KV. You can inspect, retry,
or remove them.

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
  kv: Deno.Kv; // KV store reference
}
```

## License

MIT
