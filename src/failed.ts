import type { FailedJob } from "./types.ts";
import type { QueueEnvelope } from "./envelope.ts";
import { Keys } from "./keys.ts";

/** Store a failed job record in KV. Used internally by the worker. */
export async function storeFailed(
  kv: Deno.Kv,
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
  await kv.set(Keys.failed(envelope.queue, envelope.id), record);
}

/** Provides access to failed job records for inspection and retry. */
export class FailedJobStore {
  #kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  /** List failed jobs, optionally filtered by queue name. */
  async list(queue?: string, limit = 100): Promise<FailedJob[]> {
    const prefix = queue ? Keys.failedPrefix(queue) : Keys.failedAll();
    const results: FailedJob[] = [];

    for await (const entry of this.#kv.list<FailedJob>({ prefix }, { limit })) {
      results.push(entry.value);
    }
    return results;
  }

  /** Get a single failed job by ID. Searches the given queue or "default". */
  async get(id: string, queue = "default"): Promise<FailedJob | null> {
    const entry = await this.#kv.get<FailedJob>(Keys.failed(queue, id));
    return entry.value;
  }

  /** Retry a failed job: re-enqueue it with attempt reset to 1, then remove from failed store. */
  async retry(id: string, queue = "default"): Promise<boolean> {
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
      maxAttempts: record.attempts, // preserve original max
      backoffSchedule: [1000, 5000, 30000],
    };

    const result = await this.#kv.atomic()
      .check(entry)
      .enqueue(envelope)
      .delete(Keys.failed(queue, id))
      .commit();

    return result.ok;
  }

  /** Retry all failed jobs in a queue. Returns count of retried jobs. */
  async retryAll(queue = "default"): Promise<number> {
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

  /** Delete a failed job record without retrying. */
  async forget(id: string, queue = "default"): Promise<void> {
    await this.#kv.delete(Keys.failed(queue, id));
  }

  /** Purge all failed jobs in a queue. Returns count of purged records. */
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
