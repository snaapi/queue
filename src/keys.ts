const PREFIX = "snaapi_queue" as const;

export const Keys = {
  /** Failed job by ID: ["snaapi_queue", "failed", queue, id] */
  failed(queue: string, id: string): Deno.KvKey {
    return [PREFIX, "failed", queue, id];
  },

  /** Prefix for all failed jobs in a queue */
  failedPrefix(queue: string): Deno.KvKey {
    return [PREFIX, "failed", queue];
  },

  /** Prefix for all failed jobs across all queues */
  failedAll(): Deno.KvKey {
    return [PREFIX, "failed"];
  },

  /** Unique job lock: ["snaapi_queue", "unique", uniqueKey] */
  unique(uniqueKey: string): Deno.KvKey {
    return [PREFIX, "unique", uniqueKey];
  },

  /** Undelivered fallback key: ["snaapi_queue", "undelivered", id] */
  undelivered(id: string): Deno.KvKey {
    return [PREFIX, "undelivered", id];
  },
} as const;
