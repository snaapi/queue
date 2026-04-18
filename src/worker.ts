import type { JobContext, JobHandler, JobMiddleware } from "./types.ts";
import { isQueueEnvelope, type QueueEnvelope } from "./envelope.ts";
import { Keys } from "./keys.ts";
import { runMiddlewareChain } from "./middleware.ts";
import { storeFailed } from "./failed.ts";
import { enqueueChainStep } from "./chain.ts";

export interface WorkerConfig {
  kv: Deno.Kv;
  handlers: Map<string, JobHandler>;
  middlewareMap: Map<string, JobMiddleware[]>;
  globalMiddleware: JobMiddleware[];
}

/** Create the listener function to pass to kv.listenQueue(). */
export function createListener(
  config: WorkerConfig,
): (msg: unknown) => Promise<void> {
  const { kv, handlers, middlewareMap, globalMiddleware } = config;

  return async (msg: unknown) => {
    // Ignore messages not from this library
    if (!isQueueEnvelope(msg)) return;

    const envelope: QueueEnvelope = msg;
    const handler = handlers.get(envelope.jobName);

    if (!handler) {
      await storeFailed(
        kv,
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
      kv,
    };

    // Build middleware chain: global first, then per-job
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

      // Success: clear unique lock if present
      if (envelope.uniqueKey) {
        await kv.delete(Keys.unique(envelope.uniqueKey));
      }

      // Advance chain if there are remaining steps
      if (envelope.chain && envelope.chain.length > 0) {
        await enqueueChainStep(kv, envelope.chain);
      }
    } catch (error) {
      if (envelope.attempt < envelope.maxAttempts) {
        // Re-enqueue with incremented attempt and backoff delay
        const backoffIndex = envelope.attempt - 1;
        const delay = envelope.backoffSchedule[backoffIndex] ??
          envelope.backoffSchedule[envelope.backoffSchedule.length - 1] ??
          30_000;

        const retryEnvelope: QueueEnvelope = {
          ...envelope,
          attempt: envelope.attempt + 1,
        };
        await kv.enqueue(retryEnvelope, { delay });
      } else {
        // Max attempts exhausted: store as failed
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        await storeFailed(kv, envelope, errorMessage);

        // Clear unique lock so the job can be retried manually
        if (envelope.uniqueKey) {
          await kv.delete(Keys.unique(envelope.uniqueKey));
        }
      }
    }
  };
}
