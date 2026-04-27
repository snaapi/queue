import type { FailedJob } from "./types.ts";
import type { QueueDriver } from "./drivers/types.ts";

/** Provides access to failed job records for inspection and retry. */
export class FailedJobStore {
  #driver: QueueDriver;

  constructor(driver: QueueDriver) {
    this.#driver = driver;
  }

  /** List failed jobs, optionally filtered by queue name. */
  list(queue?: string, limit = 100): Promise<FailedJob[]> {
    return this.#driver.failed.list(queue, limit);
  }

  /** Get a single failed job by ID. Searches the given queue or "default". */
  get(id: string, queue = "default"): Promise<FailedJob | null> {
    return this.#driver.failed.get(id, queue);
  }

  /** Retry a failed job: re-enqueue it with attempt reset to 1, then remove from failed store. */
  retry(id: string, queue = "default"): Promise<boolean> {
    return this.#driver.failed.retry(id, queue);
  }

  /** Retry all failed jobs in a queue. Returns count of retried jobs. */
  retryAll(queue = "default"): Promise<number> {
    return this.#driver.failed.retryAll(queue);
  }

  /** Delete a failed job record without retrying. */
  forget(id: string, queue = "default"): Promise<void> {
    return this.#driver.failed.forget(id, queue);
  }

  /** Purge all failed jobs in a queue. Returns count of purged records. */
  purge(queue?: string): Promise<number> {
    return this.#driver.failed.purge(queue);
  }
}
