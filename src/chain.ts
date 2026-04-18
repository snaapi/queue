import type { JobChainStep } from "./types.ts";
import type { QueueEnvelope } from "./envelope.ts";
import { Keys } from "./keys.ts";

/** Dispatch a chain of jobs. The first job is enqueued immediately; remaining steps are carried in the envelope. */
export async function dispatchChain(
  kv: Deno.Kv,
  steps: JobChainStep[],
): Promise<void> {
  if (steps.length === 0) return;
  const [first, ...rest] = steps;

  const envelope: QueueEnvelope = {
    __snaapi_queue: true,
    id: crypto.randomUUID(),
    jobName: first.jobName,
    payload: first.payload,
    queue: first.options?.queue ?? "default",
    attempt: 1,
    maxAttempts: first.options?.maxAttempts ?? 3,
    backoffSchedule: first.options?.backoffSchedule ?? [1000, 5000, 30000],
    uniqueKey: first.options?.uniqueKey,
    uniqueTtl: first.options?.uniqueTtl,
    chain: rest.length > 0 ? rest : undefined,
  };

  await kv.enqueue(envelope, {
    keysIfUndelivered: [Keys.undelivered(envelope.id)],
  });
}

/** Enqueue the next step in a chain. Called by the worker after a successful job. */
export async function enqueueChainStep(
  kv: Deno.Kv,
  remainingSteps: JobChainStep[],
): Promise<void> {
  await dispatchChain(kv, remainingSteps);
}
