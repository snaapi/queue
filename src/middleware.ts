import type { JobContext, JobMiddleware } from "./types.ts";
import { Keys } from "./keys.ts";

/** Execute a middleware chain, then the final handler. */
export async function runMiddlewareChain(
  middleware: JobMiddleware[],
  ctx: JobContext,
  payload: unknown,
  handler: () => Promise<void>,
): Promise<void> {
  let index = 0;
  const next = async (): Promise<void> => {
    if (index < middleware.length) {
      const mw = middleware[index++];
      await mw(ctx, payload, next);
    } else {
      await handler();
    }
  };
  await next();
}

/** Rate-limit job execution within a time window using KV counters. */
export function rateLimit(opts: {
  key: string;
  maxPerWindow: number;
  windowMs: number;
}): JobMiddleware {
  return async (ctx, _payload, next) => {
    const windowKey = [
      "snaapi_queue",
      "ratelimit",
      opts.key,
      Math.floor(Date.now() / opts.windowMs),
    ];
    const counter = await ctx.kv.get<Deno.KvU64>(windowKey);
    const current = counter.value ? Number(counter.value) : 0;

    if (current >= opts.maxPerWindow) {
      throw new Error(
        `Rate limit exceeded for "${opts.key}": ${current}/${opts.maxPerWindow}`,
      );
    }

    const atomic = ctx.kv.atomic()
      .sum(windowKey, 1n)
      .commit();
    await atomic;
    await next();
  };
}

/** Prevent overlapping execution of a job. Acquires a KV lock, skips if already held. */
export function withoutOverlapping(opts: {
  key: string;
  expiresIn?: number;
}): JobMiddleware {
  const ttl = opts.expiresIn ?? 300_000; // 5 min default

  return async (ctx, _payload, next) => {
    const lockKey = Keys.unique(`overlap:${opts.key}`);

    // Try to acquire lock atomically — only succeeds if key doesn't exist
    const result = await ctx.kv.atomic()
      .check({ key: lockKey, versionstamp: null })
      .set(lockKey, true, { expireIn: ttl })
      .commit();

    if (!result.ok) {
      // Lock held — skip this execution silently
      return;
    }

    try {
      await next();
    } finally {
      await ctx.kv.delete(lockKey);
    }
  };
}
