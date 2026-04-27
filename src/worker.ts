import type {
  FailedJob,
  JobContext,
  JobHandler,
  JobMiddleware,
} from "./types.ts";
import type { QueueEnvelope } from "./envelope.ts";
import type { QueueDriver } from "./drivers/types.ts";
import { runMiddlewareChain } from "./middleware.ts";
import { enqueueChainStep } from "./chain.ts";

export interface WorkerConfig {
  driver: QueueDriver;
  handlers: Map<string, JobHandler>;
  middlewareMap: Map<string, JobMiddleware[]>;
  globalMiddleware: JobMiddleware[];
}

/** Build the per-envelope handler the driver invokes for each delivered job. */
export function createListener(
  config: WorkerConfig,
): (envelope: QueueEnvelope) => Promise<void> {
  const { driver, handlers, middlewareMap, globalMiddleware } = config;

  return async (envelope: QueueEnvelope) => {
    const handler = handlers.get(envelope.jobName);

    if (!handler) {
      await storeFailed(
        driver,
        envelope,
        `No handler registered for "${envelope.jobName}"`,
      );
      return;
    }

    const ctx: JobContext = {
      jobName: envelope.jobName,
      attempt: envelope.attempt,
      maxAttempts: envelope.maxAttempts,
      queue: envelope.queue,
      id: envelope.id,
      locks: driver.locks,
      counters: driver.counters,
    };

    const jobMw = middlewareMap.get(envelope.jobName) ?? [];
    const allMiddleware = [...globalMiddleware, ...jobMw];

    try {
      await runMiddlewareChain(
        allMiddleware,
        ctx,
        envelope.payload,
        async () => {
          await handler.handle(envelope.payload, ctx);
        },
      );

      // Success: clear unique lock if present.
      if (envelope.uniqueKey) {
        await driver.clearUniqueLock(envelope.uniqueKey);
      }

      // Advance chain.
      if (envelope.chain && envelope.chain.length > 0) {
        await enqueueChainStep(driver, envelope.chain);
      }
    } catch (error) {
      if (envelope.attempt < envelope.maxAttempts) {
        const backoffIndex = envelope.attempt - 1;
        const delay = envelope.backoffSchedule[backoffIndex] ??
          envelope.backoffSchedule[envelope.backoffSchedule.length - 1] ??
          30_000;

        const retryEnvelope: QueueEnvelope = {
          ...envelope,
          attempt: envelope.attempt + 1,
        };
        await driver.enqueue(retryEnvelope, { delay });
      } else {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        await storeFailed(driver, envelope, errorMessage);

        if (envelope.uniqueKey) {
          await driver.clearUniqueLock(envelope.uniqueKey);
        }
      }
    }
  };
}

async function storeFailed(
  driver: QueueDriver,
  envelope: QueueEnvelope,
  error: string,
): Promise<void> {
  const record: FailedJob = {
    id: envelope.id,
    jobName: envelope.jobName,
    queue: envelope.queue,
    payload: envelope.payload,
    error,
    failedAt: Date.now(),
    attempts: envelope.attempt,
  };
  await driver.failed.store(record, envelope);
}
