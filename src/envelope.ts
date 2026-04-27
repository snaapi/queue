import type { JobChainStep } from "./types.ts";

/** The message envelope persisted into the queue store. */
export interface QueueEnvelope {
  /** Discriminator so the listener knows this message belongs to us */
  __snaapi_queue: true;
  /** Unique ID for this message */
  id: string;
  /** Registered job name */
  jobName: string;
  /** Arbitrary payload (must be structured-clone-safe) */
  payload: unknown;
  /** Named queue */
  queue: string;
  /** Current attempt (1-based) */
  attempt: number;
  /** Max attempts before failure */
  maxAttempts: number;
  /** Backoff schedule in ms */
  backoffSchedule: number[];
  /** Unique key for deduplication, if any */
  uniqueKey?: string;
  /** TTL for unique lock in ms */
  uniqueTtl?: number;
  /** Remaining chain steps after this job completes */
  chain?: JobChainStep[];
}

/** Type guard to identify our queue envelopes. */
export function isQueueEnvelope(value: unknown): value is QueueEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    "__snaapi_queue" in value &&
    (value as Record<string, unknown>).__snaapi_queue === true
  );
}
