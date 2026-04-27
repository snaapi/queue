import type { FailedJob } from "../types.ts";
import type { QueueEnvelope } from "../envelope.ts";

/** Options passed to a driver's enqueue call. */
export interface EnqueueOptions {
  /** Delay in milliseconds before delivery. */
  delay?: number;
}

/** Returned by `QueueDriver.listen`. Stop the consumer with `stop()`. */
export interface Listener {
  stop(): Promise<void>;
}

/** Backing-store specific operations for failed jobs. */
export interface FailedJobDriver {
  store(record: FailedJob): Promise<void>;
  list(queue: string | undefined, limit: number): Promise<FailedJob[]>;
  get(id: string, queue: string): Promise<FailedJob | null>;
  /** Re-enqueue a failed job and remove it from the failed store. */
  retry(id: string, queue: string): Promise<boolean>;
  /** Re-enqueue every failed job in the queue. Returns count retried. */
  retryAll(queue: string): Promise<number>;
  forget(id: string, queue: string): Promise<void>;
  purge(queue?: string): Promise<number>;
}

/** Mutex-style lock primitive used by the `withoutOverlapping` middleware. */
export interface LockDriver {
  /** Acquire a TTL-bounded lock. Returns true iff the caller now owns it. */
  acquire(key: string, ttlMs: number): Promise<boolean>;
  release(key: string): Promise<void>;
}

/** Windowed counter primitive used by the `rateLimit` middleware. */
export interface CounterDriver {
  /** Atomically increment the counter and return the new value. */
  increment(key: string, windowMs: number): Promise<number>;
}

/** The transport-agnostic interface every backing store implements. */
export interface QueueDriver {
  enqueue(envelope: QueueEnvelope, opts: EnqueueOptions): Promise<void>;
  /**
   * Atomically: enqueue iff `lockKey` is unheld, then claim the lock for
   * `lockTtlMs`. Returns false when the lock was already held (duplicate).
   */
  enqueueUnique(
    envelope: QueueEnvelope,
    opts: EnqueueOptions,
    lockKey: string,
    lockTtlMs: number,
  ): Promise<boolean>;
  /** Start consuming jobs. The handler runs once per delivered envelope. */
  listen(handler: (envelope: QueueEnvelope) => Promise<void>): Listener;
  /** Release a unique-dispatch lock after the job runs (success or terminal failure). */
  clearUniqueLock(uniqueKey: string): Promise<void>;

  readonly failed: FailedJobDriver;
  readonly locks: LockDriver;
  readonly counters: CounterDriver;

  close(): Promise<void>;
}
