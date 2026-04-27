import type { CounterDriver, LockDriver } from "./drivers/types.ts";

/** The core job interface. Consumers implement this to define job handlers. */
export interface JobHandler<T = unknown> {
  handle(payload: T, ctx: JobContext): Promise<void> | void;
}

/** Context passed to every job handler during execution. */
export interface JobContext {
  /** The job name as registered. */
  jobName: string;
  /** Which attempt this is (1-based). */
  attempt: number;
  /** Maximum attempts configured. */
  maxAttempts: number;
  /** The queue name this job was dispatched to. */
  queue: string;
  /** Unique ID for this job dispatch. */
  id: string;
  /** Lock primitive used by `withoutOverlapping` and available to user middleware. */
  locks: LockDriver;
  /** Counter primitive used by `rateLimit` and available to user middleware. */
  counters: CounterDriver;
}

/** Options when dispatching a job. */
export interface DispatchOptions {
  /** Delay in milliseconds before delivery. */
  delay?: number;
  /** Named queue (default: "default"). */
  queue?: string;
  /** Max attempts before moving to failed jobs (default: 3). */
  maxAttempts?: number;
  /** Backoff delays in ms between retries (default: [1000, 5000, 30000]). */
  backoffSchedule?: number[];
  /** Unique key. If set, prevents duplicate jobs with the same key. */
  uniqueKey?: string;
  /** TTL for uniqueness lock in ms (default: 300000 = 5 min). */
  uniqueTtl?: number;
}

/** Middleware wraps job execution. Call next() to proceed. */
export type JobMiddleware = (
  ctx: JobContext,
  payload: unknown,
  next: () => Promise<void>,
) => Promise<void>;

/** A step in a job chain. */
export interface JobChainStep {
  jobName: string;
  payload: unknown;
  options?: Omit<DispatchOptions, "delay">;
}

/** A failed job record stored in the backing store. */
export interface FailedJob {
  id: string;
  jobName: string;
  queue: string;
  payload: unknown;
  error: string;
  failedAt: number;
  attempts: number;
}
