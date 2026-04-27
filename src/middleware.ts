import type { JobContext, JobMiddleware } from "./types.ts";

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

/** Rate-limit job execution within a time window. */
export function rateLimit(opts: {
  key: string;
  maxPerWindow: number;
  windowMs: number;
}): JobMiddleware {
  return async (ctx, _payload, next) => {
    const count = await ctx.counters.increment(opts.key, opts.windowMs);
    if (count > opts.maxPerWindow) {
      throw new Error(
        `Rate limit exceeded for "${opts.key}": ${count}/${opts.maxPerWindow}`,
      );
    }
    await next();
  };
}

/** Prevent overlapping execution of a job. Skips silently if the lock is held. */
export function withoutOverlapping(opts: {
  key: string;
  expiresIn?: number;
}): JobMiddleware {
  const ttl = opts.expiresIn ?? 300_000;
  return async (ctx, _payload, next) => {
    const acquired = await ctx.locks.acquire(opts.key, ttl);
    if (!acquired) return;
    try {
      await next();
    } finally {
      await ctx.locks.release(opts.key);
    }
  };
}
