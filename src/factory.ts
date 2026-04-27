import { Queue } from "./queue.ts";
import { DenoKvDriver } from "./drivers/deno-kv.ts";
import {
  PostgresDriver,
  type PostgresDriverOptions,
} from "./drivers/postgres.ts";

export type QueueDriverName = "deno-kv" | "postgres";

export interface CreateQueueOptions {
  /** Override the env-driven driver selection. */
  driver?: QueueDriverName;
  /** Postgres connection string. Falls back to DATABASE_URL. */
  databaseUrl?: string;
  /** Pre-opened Deno.Kv handle. Falls back to `Deno.openKv()`. */
  kv?: Deno.Kv;
  /** Extra Postgres driver options (pool, polling interval, prefix, etc). */
  postgres?: Omit<PostgresDriverOptions, "connectionString">;
}

/**
 * Build a `Queue` using the driver indicated by `QUEUE_DRIVER`.
 *
 * Defaults to `postgres` (requires `DATABASE_URL`). Set `QUEUE_DRIVER=deno-kv`
 * to opt into the local Deno KV driver. Postgres schema is auto-migrated on
 * first use unless you disable it via `postgres.autoMigrate: false`.
 */
export async function createQueue(
  opts: CreateQueueOptions = {},
): Promise<Queue> {
  const driverName = opts.driver ??
    (Deno.env.get("QUEUE_DRIVER") as QueueDriverName | undefined) ??
    "postgres";

  if (driverName === "postgres") {
    const connectionString = opts.databaseUrl ?? Deno.env.get("DATABASE_URL");
    if (!connectionString) {
      throw new Error(
        "createQueue: DATABASE_URL is required for the postgres driver. " +
          "Set QUEUE_DRIVER=deno-kv to use the Deno KV driver instead.",
      );
    }
    const pg = new PostgresDriver({
      ...(opts.postgres ?? {}),
      connectionString,
    });
    return new Queue(pg);
  }

  if (driverName === "deno-kv") {
    const kv = opts.kv ?? await Deno.openKv();
    return new Queue(new DenoKvDriver(kv));
  }

  throw new Error(`createQueue: unknown driver "${driverName}"`);
}
