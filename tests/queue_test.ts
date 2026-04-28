import { assertEquals } from "@std/assert";
import { PostgresDriver, Queue } from "../mod.ts";
import type { JobHandler } from "../mod.ts";

const DATABASE_URL = Deno.env.get("DATABASE_URL");

if (!DATABASE_URL) {
  console.warn(
    "DATABASE_URL not set; skipping queue tests. " +
      "Run `docker compose up -d postgres && DATABASE_URL=postgres://queue:queue@localhost:5444/queue deno task test`.",
  );
}

async function withPg(
  fn: (queue: Queue, driver: PostgresDriver) => Promise<void>,
  opts: { concurrency?: number } = {},
) {
  if (!DATABASE_URL) return;
  const driver = new PostgresDriver({
    connectionString: DATABASE_URL,
    pollIntervalMs: 100,
    concurrency: opts.concurrency ?? 1,
  });
  await driver.migrate();
  await truncate();
  const queue = new Queue(driver);
  try {
    await fn(queue, driver);
  } finally {
    await queue.close();
  }
}

const pgTest = (name: string, fn: () => Promise<void>) =>
  Deno.test({
    name,
    fn,
    // npm:pg holds keep-alive timers and connection pools; close() drains
    // the pool, but Deno's leak detector still flags transient timers.
    sanitizeOps: false,
    sanitizeResources: false,
  });

async function truncate() {
  const { default: pg } = await import("pg");
  const client = new pg.Client({ connectionString: DATABASE_URL });
  await client.connect();
  try {
    await client.query(
      `TRUNCATE snaapi_jobs, snaapi_failed_jobs,
                snaapi_locks, snaapi_counters`,
    );
  } finally {
    await client.end();
  }
}

pgTest("dispatch and process a simple job", async () => {
  await withPg(async (queue) => {
    const results: string[] = [];
    const done = Promise.withResolvers<void>();
    const greetJob: JobHandler<{ name: string }> = {
      handle(payload) {
        results.push(`Hello ${payload.name}`);
        done.resolve();
      },
    };
    queue.register("greet", greetJob);
    queue.listen();

    await queue.dispatch("greet", { name: "World" }).send();
    await done.promise;
    assertEquals(results, ["Hello World"]);
  });
});

pgTest("dispatch with delay", async () => {
  await withPg(async (queue) => {
    const done = Promise.withResolvers<void>();
    let receivedAt = 0;
    const job: JobHandler<null> = {
      handle() {
        receivedAt = Date.now();
        done.resolve();
      },
    };
    queue.register("delayed", job);
    queue.listen();

    const sentAt = Date.now();
    await queue.dispatch("delayed", null).delay(300).send();
    await done.promise;
    const elapsed = receivedAt - sentAt;
    assertEquals(
      elapsed >= 250,
      true,
      `Expected delay >= 250ms, got ${elapsed}ms`,
    );
  });
});

pgTest("job context is populated correctly", async () => {
  await withPg(async (queue) => {
    const done = Promise.withResolvers<void>();
    let captured: Record<string, unknown> = {};
    const job: JobHandler<string> = {
      handle(_payload, ctx) {
        captured = {
          jobName: ctx.jobName,
          attempt: ctx.attempt,
          maxAttempts: ctx.maxAttempts,
          queue: ctx.queue,
          hasId: typeof ctx.id === "string" && ctx.id.length > 0,
          hasLocks: typeof ctx.locks?.acquire === "function",
          hasCounters: typeof ctx.counters?.increment === "function",
        };
        done.resolve();
      },
    };
    queue.register("ctx", job);
    queue.listen();

    await queue.dispatch("ctx", "data").onQueue("high").attempts(5).send();
    await done.promise;

    assertEquals(captured.jobName, "ctx");
    assertEquals(captured.attempt, 1);
    assertEquals(captured.maxAttempts, 5);
    assertEquals(captured.queue, "high");
    assertEquals(captured.hasId, true);
    assertEquals(captured.hasLocks, true);
    assertEquals(captured.hasCounters, true);
  });
});

pgTest("failed job is stored after max attempts", async () => {
  await withPg(async (queue) => {
    const done = Promise.withResolvers<void>();
    let attemptCount = 0;
    const failingJob: JobHandler<string> = {
      handle() {
        attemptCount++;
        if (attemptCount >= 2) {
          setTimeout(() => done.resolve(), 200);
        }
        throw new Error("always fails");
      },
    };
    queue.register("fail-job", failingJob);
    queue.listen();

    await queue.dispatch("fail-job", "test")
      .attempts(2)
      .backoff([50])
      .send();
    await done.promise;

    const failed = await queue.failed.list("default");
    assertEquals(failed.length, 1);
    assertEquals(failed[0].jobName, "fail-job");
    assertEquals(failed[0].error, "always fails");
    assertEquals(failed[0].attempts, 2);
  });
});

pgTest("global middleware runs for all jobs", async () => {
  await withPg(async (queue) => {
    const done = Promise.withResolvers<void>();
    const order: string[] = [];
    const job: JobHandler<null> = {
      handle() {
        order.push("handler");
        done.resolve();
      },
    };
    queue.use(async (_ctx, _payload, next) => {
      order.push("middleware-before");
      await next();
      order.push("middleware-after");
    });
    queue.register("mw", job);
    queue.listen();

    await queue.dispatch("mw", null).send();
    await done.promise;
    await new Promise((r) => setTimeout(r, 100));

    assertEquals(order, ["middleware-before", "handler", "middleware-after"]);
  });
});

pgTest("job chain executes in order", async () => {
  await withPg(async (queue) => {
    const results: string[] = [];
    const done = Promise.withResolvers<void>();
    const stepHandler = (
      resolve?: () => void,
    ): JobHandler<{ step: number }> => ({
      handle(payload) {
        results.push(`step-${payload.step}`);
        resolve?.();
      },
    });
    queue.register("step1", stepHandler());
    queue.register("step2", stepHandler());
    queue.register("step3", stepHandler(done.resolve));
    queue.listen();

    await queue.chain([
      { jobName: "step1", payload: { step: 1 } },
      { jobName: "step2", payload: { step: 2 } },
      { jobName: "step3", payload: { step: 3 } },
    ]);
    await done.promise;

    assertEquals(results, ["step-1", "step-2", "step-3"]);
  });
});

pgTest("unique job prevents duplicates", async () => {
  await withPg(async (queue) => {
    let callCount = 0;
    const done = Promise.withResolvers<void>();
    const job: JobHandler<null> = {
      handle() {
        callCount++;
        done.resolve();
      },
    };
    queue.register("unique-job", job);

    await queue.dispatch("unique-job", null).unique("same-key", 60_000).send();
    await queue.dispatch("unique-job", null).unique("same-key", 60_000).send();
    queue.listen();
    await done.promise;
    await new Promise((r) => setTimeout(r, 300));

    assertEquals(callCount, 1);
  });
});

pgTest("failed job can be retried", async () => {
  await withPg(async (queue) => {
    let attemptCount = 0;
    const failDone = Promise.withResolvers<void>();
    const retryDone = Promise.withResolvers<void>();
    const job: JobHandler<string> = {
      handle() {
        attemptCount++;
        if (attemptCount === 1) {
          setTimeout(() => failDone.resolve(), 200);
          throw new Error("first try fails");
        }
        retryDone.resolve();
      },
    };
    queue.register("retry-job", job);
    queue.listen();

    await queue.dispatch("retry-job", "data").attempts(1).send();
    await failDone.promise;
    const failed = await queue.failed.list("default");
    assertEquals(failed.length, 1);

    const retried = await queue.failed.retry(failed[0].id, "default");
    await retryDone.promise;

    assertEquals(retried, true);
    assertEquals(attemptCount, 2);
    const remaining = await queue.failed.list("default");
    assertEquals(remaining.length, 0);
  });
});

pgTest("rate limit middleware throws when exceeded", async () => {
  await withPg(async (queue) => {
    const { rateLimit } = await import("../mod.ts");
    const done = Promise.withResolvers<void>();
    let attempts = 0;
    const job: JobHandler<null> = {
      handle() {
        attempts++;
      },
    };
    queue.register("rl", job);
    queue.middleware(
      "rl",
      rateLimit({ key: "rl-test", maxPerWindow: 1, windowMs: 60_000 }),
    );
    queue.listen();

    // First dispatch succeeds (count=1, limit=1).
    await queue.dispatch("rl", null).attempts(1).send();
    // Second dispatch trips the limit (count=2 > limit=1) and goes to failed.
    await queue.dispatch("rl", null).attempts(1).send();

    // Wait for both to settle.
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
      const failed = await queue.failed.list("default");
      if (attempts === 1 && failed.length === 1) {
        done.resolve();
        break;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    await done.promise;
    assertEquals(attempts, 1);
  });
});

pgTest("schema is auto-migrated on first use", async () => {
  if (!DATABASE_URL) return;
  // Drop all tables; the driver should recreate them on first call.
  const { default: pg } = await import("pg");
  const setup = new pg.Client({ connectionString: DATABASE_URL });
  await setup.connect();
  try {
    await setup.query(
      `DROP TABLE IF EXISTS snaapi_jobs, snaapi_failed_jobs,
                            snaapi_locks, snaapi_counters CASCADE`,
    );
  } finally {
    await setup.end();
  }

  const driver = new PostgresDriver({
    connectionString: DATABASE_URL,
    pollIntervalMs: 100,
    // autoMigrate defaults to true.
  });
  const queue = new Queue(driver);
  try {
    const done = Promise.withResolvers<void>();
    queue.register("auto", { handle: () => done.resolve() });
    queue.listen();
    await queue.dispatch("auto", null).send();
    await done.promise;
  } finally {
    await queue.close();
  }
});

pgTest("custom tablePrefix creates and uses prefixed tables", async () => {
  if (!DATABASE_URL) return;
  const prefix = "qtest_";
  const { default: pg } = await import("pg");

  const setup = new pg.Client({ connectionString: DATABASE_URL });
  await setup.connect();
  try {
    await setup.query(
      `DROP TABLE IF EXISTS ${prefix}jobs, ${prefix}failed_jobs,
                            ${prefix}locks, ${prefix}counters CASCADE`,
    );
  } finally {
    await setup.end();
  }

  const driver = new PostgresDriver({
    connectionString: DATABASE_URL,
    pollIntervalMs: 100,
    tablePrefix: prefix,
  });
  const queue = new Queue(driver);
  try {
    const done = Promise.withResolvers<void>();
    queue.register("prefixed", { handle: () => done.resolve() });
    queue.listen();
    await queue.dispatch("prefixed", null).send();
    await done.promise;

    // Verify the prefixed tables exist (and the default ones were not
    // touched by this driver instance).
    const verify = new pg.Client({ connectionString: DATABASE_URL });
    await verify.connect();
    try {
      const res = await verify.query(
        `SELECT table_name FROM information_schema.tables
           WHERE table_name LIKE $1 ORDER BY table_name`,
        [`${prefix}%`],
      );
      const names = (res.rows as Array<{ table_name: string }>)
        .map((r) => r.table_name).sort();
      assertEquals(names, [
        `${prefix}counters`,
        `${prefix}failed_jobs`,
        `${prefix}jobs`,
        `${prefix}locks`,
      ]);
    } finally {
      await verify.end();
    }
  } finally {
    await queue.close();
    // Clean up so the test is repeatable.
    const cleanup = new pg.Client({ connectionString: DATABASE_URL });
    await cleanup.connect();
    try {
      await cleanup.query(
        `DROP TABLE IF EXISTS ${prefix}jobs, ${prefix}failed_jobs,
                              ${prefix}locks, ${prefix}counters CASCADE`,
      );
    } finally {
      await cleanup.end();
    }
  }
});

pgTest("withoutOverlapping skips concurrent runs", async () => {
  await withPg(async (queue) => {
    const { withoutOverlapping } = await import("../mod.ts");
    let active = 0;
    let peak = 0;
    let started = 0;
    let completed = 0;
    const inflight = Promise.withResolvers<void>();
    const job: JobHandler<null> = {
      async handle() {
        started++;
        active++;
        peak = Math.max(peak, active);
        await inflight.promise;
        active--;
        completed++;
      },
    };
    queue.register("solo", job);
    queue.middleware(
      "solo",
      withoutOverlapping({ key: "solo-test", expiresIn: 60_000 }),
    );
    queue.listen();

    // Two dispatches enqueued together; both pulled by the concurrency=2
    // consumer at roughly the same time. Lock arbitration happens inside
    // the middleware: only one wins, the other returns silently.
    await queue.dispatch("solo", null).send();
    await queue.dispatch("solo", null).send();
    await new Promise((r) => setTimeout(r, 400));
    inflight.resolve();
    await new Promise((r) => setTimeout(r, 200));

    assertEquals(peak, 1);
    assertEquals(started, 1);
    assertEquals(completed, 1);
  }, { concurrency: 2 });
});

Deno.test({
  name: "worker survives unreachable DB and surfaces errors via onError",
  // The pool keeps reconnect timers alive past the test body.
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const errors: unknown[] = [];
    const driver = new PostgresDriver({
      // Port 1 is reserved and refuses connections, so every poll fails.
      connectionString: "postgres://nobody:nobody@127.0.0.1:1/none",
      pollIntervalMs: 50,
      autoMigrate: false,
      onError: (err) => errors.push(err),
    });
    const queue = new Queue(driver);
    queue.register("noop", { handle: () => Promise.resolve() });

    queue.listen();
    await new Promise((r) => setTimeout(r, 400));
    await queue.close();

    assertEquals(
      errors.length > 0,
      true,
      `expected at least one onError invocation, got ${errors.length}`,
    );
  },
});
