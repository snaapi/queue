import type { JobChainStep, JobHandler, JobMiddleware } from "./types.ts";
import { PendingDispatch } from "./dispatcher.ts";
import { FailedJobStore } from "./failed.ts";
import { dispatchChain } from "./chain.ts";
import { createListener } from "./worker.ts";

/** The main queue interface. Register jobs, dispatch work, and start listening. */
export class Queue {
  #kv: Deno.Kv;
  #handlers = new Map<string, JobHandler>();
  #middlewareMap = new Map<string, JobMiddleware[]>();
  #globalMiddleware: JobMiddleware[] = [];
  #failedStore: FailedJobStore;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
    this.#failedStore = new FailedJobStore(kv);
  }

  /** Register a named job handler. */
  register<T>(name: string, handler: JobHandler<T>): this {
    this.#handlers.set(name, handler as JobHandler);
    return this;
  }

  /** Add middleware for a specific job. */
  middleware(jobName: string, ...mw: JobMiddleware[]): this {
    const existing = this.#middlewareMap.get(jobName) ?? [];
    this.#middlewareMap.set(jobName, [...existing, ...mw]);
    return this;
  }

  /** Add global middleware that runs for all jobs. */
  use(...mw: JobMiddleware[]): this {
    this.#globalMiddleware.push(...mw);
    return this;
  }

  /** Dispatch a job. Returns a chainable builder — call .send() to enqueue. */
  dispatch<T>(jobName: string, payload: T): PendingDispatch {
    return new PendingDispatch(this.#kv, jobName, payload);
  }

  /** Dispatch a chain of jobs to run sequentially. */
  async chain(steps: JobChainStep[]): Promise<void> {
    await dispatchChain(this.#kv, steps);
  }

  /** Start listening for queue messages. Call once per worker process. */
  listen(): void {
    const listener = createListener({
      kv: this.#kv,
      handlers: this.#handlers,
      middlewareMap: this.#middlewareMap,
      globalMiddleware: this.#globalMiddleware,
    });
    this.#kv.listenQueue(listener);
  }

  /** Access the failed job store for inspection and retry. */
  get failed(): FailedJobStore {
    return this.#failedStore;
  }
}
