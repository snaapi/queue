import type { FailedJob } from "../types.ts";
import { isQueueEnvelope, type QueueEnvelope } from "../envelope.ts";
import { Keys } from "../keys.ts";
import type {
  CounterDriver,
  EnqueueOptions,
  FailedJobDriver,
  Listener,
  LockDriver,
  QueueDriver,
} from "./types.ts";

class DenoKvFailedStore implements FailedJobDriver {
  #kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  async store(record: FailedJob): Promise<void> {
    await this.#kv.set(Keys.failed(record.queue, record.id), record);
  }

  async list(queue: string | undefined, limit: number): Promise<FailedJob[]> {
    const prefix = queue ? Keys.failedPrefix(queue) : Keys.failedAll();
    const results: FailedJob[] = [];
    for await (const entry of this.#kv.list<FailedJob>({ prefix }, { limit })) {
      results.push(entry.value);
    }
    return results;
  }

  async get(id: string, queue: string): Promise<FailedJob | null> {
    const entry = await this.#kv.get<FailedJob>(Keys.failed(queue, id));
    return entry.value;
  }

  async retry(id: string, queue: string): Promise<boolean> {
    const entry = await this.#kv.get<FailedJob>(Keys.failed(queue, id));
    if (!entry.value) return false;

    const record = entry.value;
    const envelope: QueueEnvelope = {
      __snaapi_queue: true,
      id: crypto.randomUUID(),
      jobName: record.jobName,
      payload: record.payload,
      queue: record.queue,
      attempt: 1,
      maxAttempts: record.attempts,
      backoffSchedule: [1000, 5000, 30000],
    };

    const result = await this.#kv.atomic()
      .check(entry)
      .enqueue(envelope)
      .delete(Keys.failed(queue, id))
      .commit();

    return result.ok;
  }

  async retryAll(queue: string): Promise<number> {
    let count = 0;
    for await (
      const entry of this.#kv.list<FailedJob>({
        prefix: Keys.failedPrefix(queue),
      })
    ) {
      const record = entry.value;
      const envelope: QueueEnvelope = {
        __snaapi_queue: true,
        id: crypto.randomUUID(),
        jobName: record.jobName,
        payload: record.payload,
        queue: record.queue,
        attempt: 1,
        maxAttempts: record.attempts,
        backoffSchedule: [1000, 5000, 30000],
      };
      const result = await this.#kv.atomic()
        .check(entry)
        .enqueue(envelope)
        .delete(entry.key)
        .commit();
      if (result.ok) count++;
    }
    return count;
  }

  async forget(id: string, queue: string): Promise<void> {
    await this.#kv.delete(Keys.failed(queue, id));
  }

  async purge(queue?: string): Promise<number> {
    const prefix = queue ? Keys.failedPrefix(queue) : Keys.failedAll();
    let count = 0;
    for await (const entry of this.#kv.list({ prefix })) {
      await this.#kv.delete(entry.key);
      count++;
    }
    return count;
  }
}

class DenoKvLockDriver implements LockDriver {
  #kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  async acquire(key: string, ttlMs: number): Promise<boolean> {
    const lockKey = Keys.unique(`overlap:${key}`);
    const result = await this.#kv.atomic()
      .check({ key: lockKey, versionstamp: null })
      .set(lockKey, true, { expireIn: ttlMs })
      .commit();
    return result.ok;
  }

  async release(key: string): Promise<void> {
    await this.#kv.delete(Keys.unique(`overlap:${key}`));
  }
}

class DenoKvCounterDriver implements CounterDriver {
  #kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  async increment(key: string, windowMs: number): Promise<number> {
    const windowKey = [
      "snaapi_queue",
      "ratelimit",
      key,
      Math.floor(Date.now() / windowMs),
    ];
    await this.#kv.atomic().sum(windowKey, 1n).commit();
    const after = await this.#kv.get<Deno.KvU64>(windowKey);
    return after.value ? Number(after.value) : 0;
  }
}

/** Deno KV implementation of `QueueDriver`. */
export class DenoKvDriver implements QueueDriver {
  #kv: Deno.Kv;
  readonly failed: FailedJobDriver;
  readonly locks: LockDriver;
  readonly counters: CounterDriver;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
    this.failed = new DenoKvFailedStore(kv);
    this.locks = new DenoKvLockDriver(kv);
    this.counters = new DenoKvCounterDriver(kv);
  }

  async enqueue(envelope: QueueEnvelope, opts: EnqueueOptions): Promise<void> {
    const enqueueOpts: { delay?: number; keysIfUndelivered?: Deno.KvKey[] } = {
      keysIfUndelivered: [Keys.undelivered(envelope.id)],
    };
    if (opts.delay !== undefined) enqueueOpts.delay = opts.delay;
    await this.#kv.enqueue(envelope, enqueueOpts);
  }

  async enqueueUnique(
    envelope: QueueEnvelope,
    opts: EnqueueOptions,
    lockKey: string,
    lockTtlMs: number,
  ): Promise<boolean> {
    const key = Keys.unique(lockKey);
    const enqueueOpts: { delay?: number; keysIfUndelivered?: Deno.KvKey[] } = {
      keysIfUndelivered: [Keys.undelivered(envelope.id)],
    };
    if (opts.delay !== undefined) enqueueOpts.delay = opts.delay;

    const result = await this.#kv.atomic()
      .check({ key, versionstamp: null })
      .enqueue(envelope, enqueueOpts)
      .set(key, true, { expireIn: lockTtlMs })
      .commit();
    return result.ok;
  }

  listen(handler: (envelope: QueueEnvelope) => Promise<void>): Listener {
    this.#kv.listenQueue(async (msg: unknown) => {
      if (!isQueueEnvelope(msg)) return;
      await handler(msg);
    });
    return { stop: () => Promise.resolve() };
  }

  async clearUniqueLock(uniqueKey: string): Promise<void> {
    await this.#kv.delete(Keys.unique(uniqueKey));
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
