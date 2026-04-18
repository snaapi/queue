import { assertEquals } from "@std/assert";
import { Queue } from "../mod.ts";
import type { JobHandler } from "../mod.ts";

async function withKv(fn: (kv: Deno.Kv) => Promise<void>) {
  const kv = await Deno.openKv(":memory:");
  try {
    await fn(kv);
  } finally {
    kv.close();
  }
}

Deno.test("dispatch and process a simple job", async () => {
  await withKv(async (kv) => {
    // Arrange
    const results: string[] = [];
    const done = Promise.withResolvers<void>();
    const greetJob: JobHandler<{ name: string }> = {
      handle(payload) {
        results.push(`Hello ${payload.name}`);
        done.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("greet", greetJob);
    queue.listen();

    // Act
    await queue.dispatch("greet", { name: "World" }).send();
    await done.promise;

    // Assert
    assertEquals(results, ["Hello World"]);
  });
});

Deno.test("dispatch with delay", async () => {
  await withKv(async (kv) => {
    // Arrange
    const done = Promise.withResolvers<void>();
    let receivedAt = 0;
    const job: JobHandler<null> = {
      handle() {
        receivedAt = Date.now();
        done.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("delayed", job);
    queue.listen();

    // Act
    const sentAt = Date.now();
    await queue.dispatch("delayed", null).delay(100).send();
    await done.promise;

    // Assert
    const elapsed = receivedAt - sentAt;
    assertEquals(
      elapsed >= 50,
      true,
      `Expected delay >= 50ms, got ${elapsed}ms`,
    );
  });
});

Deno.test("job context is populated correctly", async () => {
  await withKv(async (kv) => {
    // Arrange
    const done = Promise.withResolvers<void>();
    let capturedCtx: Record<string, unknown> = {};
    const job: JobHandler<string> = {
      handle(_payload, ctx) {
        capturedCtx = {
          jobName: ctx.jobName,
          attempt: ctx.attempt,
          maxAttempts: ctx.maxAttempts,
          queue: ctx.queue,
          hasId: typeof ctx.id === "string" && ctx.id.length > 0,
          hasKv: ctx.kv !== undefined,
        };
        done.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("ctx-test", job);
    queue.listen();

    // Act
    await queue.dispatch("ctx-test", "data").onQueue("high").attempts(5).send();
    await done.promise;

    // Assert
    assertEquals(capturedCtx.jobName, "ctx-test");
    assertEquals(capturedCtx.attempt, 1);
    assertEquals(capturedCtx.maxAttempts, 5);
    assertEquals(capturedCtx.queue, "high");
    assertEquals(capturedCtx.hasId, true);
    assertEquals(capturedCtx.hasKv, true);
  });
});

Deno.test("failed job is stored after max attempts", async () => {
  await withKv(async (kv) => {
    // Arrange
    const done = Promise.withResolvers<void>();
    let attemptCount = 0;
    const failingJob: JobHandler<string> = {
      handle() {
        attemptCount++;
        if (attemptCount >= 2) {
          setTimeout(() => done.resolve(), 100);
        }
        throw new Error("always fails");
      },
    };
    const queue = new Queue(kv);
    queue.register("fail-job", failingJob);
    queue.listen();

    // Act
    await queue.dispatch("fail-job", "test")
      .attempts(2)
      .backoff([50])
      .send();
    await done.promise;

    // Assert
    const failed = await queue.failed.list("default");
    assertEquals(failed.length, 1);
    assertEquals(failed[0].jobName, "fail-job");
    assertEquals(failed[0].error, "always fails");
    assertEquals(failed[0].attempts, 2);
  });
});

Deno.test("global middleware runs for all jobs", async () => {
  await withKv(async (kv) => {
    // Arrange
    const done = Promise.withResolvers<void>();
    const order: string[] = [];
    const job: JobHandler<null> = {
      handle() {
        order.push("handler");
        done.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.use(async (_ctx, _payload, next) => {
      order.push("middleware-before");
      await next();
      order.push("middleware-after");
    });
    queue.register("mw-test", job);
    queue.listen();

    // Act
    await queue.dispatch("mw-test", null).send();
    await done.promise;
    await new Promise((r) => setTimeout(r, 50));

    // Assert
    assertEquals(order, ["middleware-before", "handler", "middleware-after"]);
  });
});

Deno.test("job-specific middleware only runs for that job", async () => {
  await withKv(async (kv) => {
    // Arrange
    const done1 = Promise.withResolvers<void>();
    const done2 = Promise.withResolvers<void>();
    const logs: string[] = [];
    const jobA: JobHandler<null> = {
      handle() {
        logs.push("jobA");
        done1.resolve();
      },
    };
    const jobB: JobHandler<null> = {
      handle() {
        logs.push("jobB");
        done2.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("a", jobA);
    queue.register("b", jobB);
    queue.middleware("a", async (_ctx, _payload, next) => {
      logs.push("mw-a");
      await next();
    });
    queue.listen();

    // Act
    await queue.dispatch("a", null).send();
    await done1.promise;
    await queue.dispatch("b", null).send();
    await done2.promise;

    // Assert
    assertEquals(logs.includes("mw-a"), true);
    assertEquals(logs.includes("jobA"), true);
    assertEquals(logs.includes("jobB"), true);
    assertEquals(logs.indexOf("mw-a") < logs.indexOf("jobA"), true);
  });
});

Deno.test("job chain executes in order", async () => {
  await withKv(async (kv) => {
    // Arrange
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
    const queue = new Queue(kv);
    queue.register("step1", stepHandler());
    queue.register("step2", stepHandler());
    queue.register("step3", stepHandler(done.resolve));
    queue.listen();

    // Act
    await queue.chain([
      { jobName: "step1", payload: { step: 1 } },
      { jobName: "step2", payload: { step: 2 } },
      { jobName: "step3", payload: { step: 3 } },
    ]);
    await done.promise;

    // Assert
    assertEquals(results, ["step-1", "step-2", "step-3"]);
  });
});

Deno.test("unique job prevents duplicates", async () => {
  await withKv(async (kv) => {
    // Arrange
    let callCount = 0;
    const done = Promise.withResolvers<void>();
    const job: JobHandler<null> = {
      handle() {
        callCount++;
        done.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("unique-job", job);

    // Act — dispatch twice with same unique key before listening
    await queue.dispatch("unique-job", null).unique("same-key", 60_000).send();
    await queue.dispatch("unique-job", null).unique("same-key", 60_000).send();
    queue.listen();
    await done.promise;
    await new Promise((r) => setTimeout(r, 200));

    // Assert
    assertEquals(callCount, 1);
  });
});

Deno.test("failed job can be retried", async () => {
  await withKv(async (kv) => {
    // Arrange
    let attemptCount = 0;
    const failDone = Promise.withResolvers<void>();
    const retryDone = Promise.withResolvers<void>();
    const job: JobHandler<string> = {
      handle() {
        attemptCount++;
        if (attemptCount === 1) {
          setTimeout(() => failDone.resolve(), 100);
          throw new Error("first try fails");
        }
        retryDone.resolve();
      },
    };
    const queue = new Queue(kv);
    queue.register("retry-job", job);
    queue.listen();

    // Act — dispatch and wait for initial failure
    await queue.dispatch("retry-job", "data").attempts(1).send();
    await failDone.promise;
    const failed = await queue.failed.list("default");

    // Assert — verify failure was recorded
    assertEquals(failed.length, 1);

    // Act — retry the failed job
    const retried = await queue.failed.retry(failed[0].id, "default");
    await retryDone.promise;

    // Assert — verify retry succeeded and store is cleared
    assertEquals(retried, true);
    assertEquals(attemptCount, 2);
    const remaining = await queue.failed.list("default");
    assertEquals(remaining.length, 0);
  });
});
