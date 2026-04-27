import type { QueueEnvelope } from "./envelope.ts";
import type { EnqueueOptions, QueueDriver } from "./drivers/types.ts";

const DEFAULT_QUEUE = "default";
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BACKOFF = [1000, 5000, 30000];
const DEFAULT_UNIQUE_TTL = 300_000; // 5 minutes

/** Chainable builder for dispatching a job. Call .send() to enqueue. */
export class PendingDispatch {
  #driver: QueueDriver;
  #jobName: string;
  #payload: unknown;
  #delay?: number;
  #queue = DEFAULT_QUEUE;
  #maxAttempts = DEFAULT_MAX_ATTEMPTS;
  #backoffSchedule = DEFAULT_BACKOFF;
  #uniqueKey?: string;
  #uniqueTtl = DEFAULT_UNIQUE_TTL;
  #chain?: QueueEnvelope["chain"];

  constructor(
    driver: QueueDriver,
    jobName: string,
    payload: unknown,
    chain?: QueueEnvelope["chain"],
  ) {
    this.#driver = driver;
    this.#jobName = jobName;
    this.#payload = payload;
    this.#chain = chain;
  }

  /** Set delay in milliseconds before the job is delivered. */
  delay(ms: number): this {
    this.#delay = ms;
    return this;
  }

  /** Set the named queue for this job. */
  onQueue(name: string): this {
    this.#queue = name;
    return this;
  }

  /** Set maximum retry attempts. */
  attempts(n: number): this {
    this.#maxAttempts = n;
    return this;
  }

  /** Set backoff schedule (array of delays in ms between retries). */
  backoff(schedule: number[]): this {
    this.#backoffSchedule = schedule;
    return this;
  }

  /** Make this a unique job. Prevents duplicates with the same key. */
  unique(key: string, ttl?: number): this {
    this.#uniqueKey = key;
    if (ttl !== undefined) this.#uniqueTtl = ttl;
    return this;
  }

  /** Enqueue the job. Must be called to actually dispatch. */
  async send(): Promise<void> {
    const id = crypto.randomUUID();

    const envelope: QueueEnvelope = {
      __snaapi_queue: true,
      id,
      jobName: this.#jobName,
      payload: this.#payload,
      queue: this.#queue,
      attempt: 1,
      maxAttempts: this.#maxAttempts,
      backoffSchedule: this.#backoffSchedule,
      uniqueKey: this.#uniqueKey,
      uniqueTtl: this.#uniqueTtl,
      chain: this.#chain,
    };

    const opts: EnqueueOptions = {};
    if (this.#delay !== undefined) opts.delay = this.#delay;

    if (this.#uniqueKey) {
      // Driver decides how to atomically gate the enqueue on a lock.
      await this.#driver.enqueueUnique(
        envelope,
        opts,
        this.#uniqueKey,
        this.#uniqueTtl,
      );
    } else {
      await this.#driver.enqueue(envelope, opts);
    }
  }
}
