import pg from "pg";
import type { FailedJob } from "../types.ts";
import type { QueueEnvelope } from "../envelope.ts";
import type {
  CounterDriver,
  EnqueueOptions,
  FailedJobDriver,
  Listener,
  LockDriver,
  QueueDriver,
} from "./types.ts";

const { Pool, Client } = pg;
type Pool = pg.Pool;
type PoolClient = pg.PoolClient;
type Client = pg.Client;

const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_RESERVE_TTL = 5 * 60_000;
const DEFAULT_TABLE_PREFIX = "snaapi_";

export interface PostgresDriverOptions {
  /** Postgres connection string (e.g. postgres://user:pass@host:5432/db). */
  connectionString: string;
  /** Polling fallback interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Max concurrent in-flight jobs per listener. Default: 1. */
  concurrency?: number;
  /** How long a worker can hold a job before another can steal it. Default: 5m. */
  reserveTtlMs?: number;
  /** Pool size. Default: 10. */
  poolSize?: number;
  /**
   * Prefix for the queue tables. Defaults to `snaapi_` (so tables are
   * `snaapi_jobs`, `snaapi_failed_jobs`, `snaapi_locks`, `snaapi_counters`).
   * The `LISTEN`/`NOTIFY` channel is derived from the prefix as well.
   * Falls back to the `QUEUE_TABLE_PREFIX` env var when omitted.
   */
  tablePrefix?: string;
  /**
   * Auto-run `migrate()` on first use. Default: true. Disable if you manage
   * the schema yourself or run migrations as a separate deploy step.
   */
  autoMigrate?: boolean;
}

interface TableNames {
  jobs: string;
  failedJobs: string;
  locks: string;
  counters: string;
  notifyChannel: string;
}

function resolvePrefix(opts: PostgresDriverOptions): string {
  return opts.tablePrefix ??
    Deno.env.get("QUEUE_TABLE_PREFIX") ??
    DEFAULT_TABLE_PREFIX;
}

function uniqueLockKey(key: string): string {
  // Namespace the unique-dispatch lock to avoid colliding with other lock
  // keys consumers may use through ctx.locks (`withoutOverlapping`, etc).
  return `unique:${key}`;
}

function tableNamesFor(prefix: string): TableNames {
  // Identifiers come from config, not user input; bare interpolation is safe.
  // We also validate the prefix to keep that promise honest.
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(prefix)) {
    throw new Error(
      `PostgresDriver: tablePrefix "${prefix}" must match [a-zA-Z_][a-zA-Z0-9_]*`,
    );
  }
  return {
    jobs: `${prefix}jobs`,
    failedJobs: `${prefix}failed_jobs`,
    locks: `${prefix}locks`,
    counters: `${prefix}counters`,
    notifyChannel: `${prefix}jobs_channel`,
  };
}

function schemaSql(t: TableNames): string {
  return `
CREATE TABLE IF NOT EXISTS ${t.jobs} (
  id           UUID PRIMARY KEY,
  queue        TEXT NOT NULL,
  envelope     JSONB NOT NULL,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reserved_at  TIMESTAMPTZ,
  unique_key   TEXT UNIQUE
);
DROP INDEX IF EXISTS ${t.jobs}_ready_idx;
CREATE INDEX IF NOT EXISTS ${t.jobs}_available_idx
  ON ${t.jobs} (available_at);

CREATE TABLE IF NOT EXISTS ${t.failedJobs} (
  id         UUID PRIMARY KEY,
  job_name   TEXT NOT NULL,
  queue      TEXT NOT NULL,
  payload    JSONB NOT NULL,
  error      TEXT NOT NULL,
  failed_at  TIMESTAMPTZ NOT NULL,
  attempts   INT NOT NULL,
  envelope   JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS ${t.failedJobs}_queue_idx
  ON ${t.failedJobs} (queue, failed_at DESC);

CREATE TABLE IF NOT EXISTS ${t.locks} (
  key        TEXT PRIMARY KEY,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ${t.counters} (
  key        TEXT PRIMARY KEY,
  count      BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
`;
}

class PgFailedStore implements FailedJobDriver {
  #pool: Pool;
  #t: TableNames;
  #ensureReady: () => Promise<void>;

  constructor(pool: Pool, t: TableNames, ensureReady: () => Promise<void>) {
    this.#pool = pool;
    this.#t = t;
    this.#ensureReady = ensureReady;
  }

  async store(record: FailedJob, envelope: QueueEnvelope): Promise<void> {
    await this.#ensureReady();
    await this.#pool.query(
      `INSERT INTO ${this.#t.failedJobs}
         (id, job_name, queue, payload, error, failed_at, attempts, envelope)
       VALUES ($1, $2, $3, $4, $5, to_timestamp($6 / 1000.0), $7, $8)
       ON CONFLICT (id) DO UPDATE SET
         error = EXCLUDED.error,
         failed_at = EXCLUDED.failed_at,
         attempts = EXCLUDED.attempts,
         envelope = EXCLUDED.envelope`,
      [
        record.id,
        record.jobName,
        record.queue,
        JSON.stringify(record.payload),
        record.error,
        record.failedAt,
        record.attempts,
        JSON.stringify(envelope),
      ],
    );
  }

  async list(queue: string | undefined, limit: number): Promise<FailedJob[]> {
    await this.#ensureReady();
    const params: unknown[] = [];
    let where = "";
    if (queue) {
      params.push(queue);
      where = "WHERE queue = $1";
    }
    params.push(limit);
    const limitParam = `$${params.length}`;
    const res = await this.#pool.query(
      `SELECT id, job_name, queue, payload, error,
              extract(epoch from failed_at) * 1000 AS failed_at_ms,
              attempts
         FROM ${this.#t.failedJobs}
         ${where}
         ORDER BY failed_at DESC
         LIMIT ${limitParam}`,
      params,
    );
    return res.rows.map(rowToFailed);
  }

  async get(id: string, queue: string): Promise<FailedJob | null> {
    await this.#ensureReady();
    const res = await this.#pool.query(
      `SELECT id, job_name, queue, payload, error,
              extract(epoch from failed_at) * 1000 AS failed_at_ms,
              attempts
         FROM ${this.#t.failedJobs}
         WHERE id = $1 AND queue = $2`,
      [id, queue],
    );
    if (res.rowCount === 0) return null;
    return rowToFailed(res.rows[0]);
  }

  async retry(id: string, queue: string): Promise<boolean> {
    await this.#ensureReady();
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const res = await client.query<{ envelope: QueueEnvelope }>(
        `DELETE FROM ${this.#t.failedJobs}
           WHERE id = $1 AND queue = $2
         RETURNING envelope`,
        [id, queue],
      );
      if (res.rowCount === 0) {
        await client.query("ROLLBACK");
        return false;
      }
      const original = res.rows[0].envelope;
      const fresh: QueueEnvelope = {
        __snaapi_queue: true,
        id: crypto.randomUUID(),
        jobName: original.jobName,
        payload: original.payload,
        queue: original.queue,
        attempt: 1,
        maxAttempts: original.maxAttempts,
        backoffSchedule: original.backoffSchedule,
        uniqueKey: original.uniqueKey,
        uniqueTtl: original.uniqueTtl,
        chain: original.chain,
      };
      await insertJob(client, this.#t, fresh, 0);
      await client.query("COMMIT");
      return true;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  async retryAll(queue: string): Promise<number> {
    const list = await this.list(queue, 1000);
    let count = 0;
    for (const failed of list) {
      if (await this.retry(failed.id, queue)) count++;
    }
    return count;
  }

  async forget(id: string, queue: string): Promise<void> {
    await this.#ensureReady();
    await this.#pool.query(
      `DELETE FROM ${this.#t.failedJobs} WHERE id = $1 AND queue = $2`,
      [id, queue],
    );
  }

  async purge(queue?: string): Promise<number> {
    await this.#ensureReady();
    const res = queue
      ? await this.#pool.query(
        `DELETE FROM ${this.#t.failedJobs} WHERE queue = $1`,
        [queue],
      )
      : await this.#pool.query(`DELETE FROM ${this.#t.failedJobs}`);
    return res.rowCount ?? 0;
  }
}

function rowToFailed(row: Record<string, unknown>): FailedJob {
  return {
    id: String(row.id),
    jobName: String(row.job_name),
    queue: String(row.queue),
    payload: row.payload,
    error: String(row.error),
    failedAt: Number(row.failed_at_ms),
    attempts: Number(row.attempts),
  };
}

class PgLockDriver implements LockDriver {
  #pool: Pool;
  #t: TableNames;
  #ensureReady: () => Promise<void>;

  constructor(pool: Pool, t: TableNames, ensureReady: () => Promise<void>) {
    this.#pool = pool;
    this.#t = t;
    this.#ensureReady = ensureReady;
  }

  async acquire(key: string, ttlMs: number): Promise<boolean> {
    await this.#ensureReady();
    const res = await this.#pool.query<{ acquired: boolean }>(
      `INSERT INTO ${this.#t.locks} (key, expires_at)
         VALUES ($1, NOW() + ($2 || ' milliseconds')::INTERVAL)
       ON CONFLICT (key) DO UPDATE
         SET expires_at = EXCLUDED.expires_at
         WHERE ${this.#t.locks}.expires_at <= NOW()
       RETURNING true AS acquired`,
      [key, String(ttlMs)],
    );
    return (res.rowCount ?? 0) > 0;
  }

  async release(key: string): Promise<void> {
    await this.#ensureReady();
    await this.#pool.query(
      `DELETE FROM ${this.#t.locks} WHERE key = $1`,
      [key],
    );
  }
}

class PgCounterDriver implements CounterDriver {
  #pool: Pool;
  #t: TableNames;
  #ensureReady: () => Promise<void>;

  constructor(pool: Pool, t: TableNames, ensureReady: () => Promise<void>) {
    this.#pool = pool;
    this.#t = t;
    this.#ensureReady = ensureReady;
  }

  async increment(key: string, windowMs: number): Promise<number> {
    await this.#ensureReady();
    const windowKey = `${key}:${Math.floor(Date.now() / windowMs)}`;
    const res = await this.#pool.query<{ count: string }>(
      `INSERT INTO ${this.#t.counters} (key, count, expires_at)
         VALUES ($1, 1, NOW() + ($2 || ' milliseconds')::INTERVAL)
       ON CONFLICT (key) DO UPDATE
         SET count = ${this.#t.counters}.count + 1,
             expires_at = EXCLUDED.expires_at
       RETURNING count`,
      [windowKey, String(windowMs)],
    );
    return Number(res.rows[0].count);
  }
}

async function insertJob(
  client: PoolClient | Client | Pool,
  t: TableNames,
  envelope: QueueEnvelope,
  delayMs: number,
): Promise<boolean> {
  // The row PK is independent of envelope.id so a mid-flight retry
  // (which keeps the same envelope.id) does not collide with the
  // still-reserved original row. unique_key is NULL because dedup is
  // enforced via the locks table; see PostgresDriver.enqueueUnique.
  const res = await client.query(
    `INSERT INTO ${t.jobs} (id, queue, envelope, available_at)
       VALUES ($1, $2, $3, NOW() + ($4 || ' milliseconds')::INTERVAL)`,
    [
      crypto.randomUUID(),
      envelope.queue,
      JSON.stringify(envelope),
      String(delayMs),
    ],
  );
  return (res.rowCount ?? 0) > 0;
}

/** Postgres implementation of `QueueDriver`. */
export class PostgresDriver implements QueueDriver {
  #pool: Pool;
  #opts: Required<
    Omit<PostgresDriverOptions, "connectionString" | "tablePrefix">
  >;
  #connectionString: string;
  #t: TableNames;
  #migrationPromise?: Promise<void>;
  #notifyClient?: Client;
  #pollTimer?: number;
  #cleanupTimer?: number;
  #stopped = false;
  #wakeup: () => void = () => {};
  #inFlight = 0;
  readonly failed: FailedJobDriver;
  readonly locks: LockDriver;
  readonly counters: CounterDriver;

  constructor(opts: PostgresDriverOptions) {
    this.#connectionString = opts.connectionString;
    this.#opts = {
      pollIntervalMs: opts.pollIntervalMs ?? DEFAULT_POLL_INTERVAL,
      concurrency: opts.concurrency ?? 1,
      reserveTtlMs: opts.reserveTtlMs ?? DEFAULT_RESERVE_TTL,
      poolSize: opts.poolSize ?? 10,
      autoMigrate: opts.autoMigrate ?? true,
    };
    this.#t = tableNamesFor(resolvePrefix(opts));
    this.#pool = new Pool({
      connectionString: this.#connectionString,
      max: this.#opts.poolSize,
    });
    const ensureReady = () => this.#ensureReady();
    this.failed = new PgFailedStore(this.#pool, this.#t, ensureReady);
    this.locks = new PgLockDriver(this.#pool, this.#t, ensureReady);
    this.counters = new PgCounterDriver(this.#pool, this.#t, ensureReady);
  }

  /** Create or update the queue tables. Idempotent. */
  async migrate(): Promise<void> {
    await this.#pool.query(schemaSql(this.#t));
  }

  #ensureReady(): Promise<void> {
    if (!this.#opts.autoMigrate) return Promise.resolve();
    if (!this.#migrationPromise) {
      this.#migrationPromise = this.migrate().catch((err) => {
        // Reset so a later call can retry instead of being permanently broken.
        this.#migrationPromise = undefined;
        throw err;
      });
    }
    return this.#migrationPromise;
  }

  async enqueue(envelope: QueueEnvelope, opts: EnqueueOptions): Promise<void> {
    await this.#ensureReady();
    await insertJob(this.#pool, this.#t, envelope, opts.delay ?? 0);
    await this.#pool.query(`SELECT pg_notify($1, $2)`, [
      this.#t.notifyChannel,
      envelope.queue,
    ]);
  }

  async enqueueUnique(
    envelope: QueueEnvelope,
    opts: EnqueueOptions,
    lockKey: string,
    lockTtlMs: number,
  ): Promise<boolean> {
    await this.#ensureReady();
    // Hold a TTL-bounded row in the locks table so the dedupe window
    // matches the requested lockTtlMs (the unique_key column on jobs would
    // only dedupe until the row is deleted on completion, which is shorter
    // than the documented contract).
    const acquired = await this.#acquireUniqueLock(lockKey, lockTtlMs);
    if (!acquired) return false;

    try {
      const inserted = await insertJob(
        this.#pool,
        this.#t,
        envelope,
        opts.delay ?? 0,
      );
      if (!inserted) {
        await this.clearUniqueLock(lockKey);
        return false;
      }
      await this.#pool.query(`SELECT pg_notify($1, $2)`, [
        this.#t.notifyChannel,
        envelope.queue,
      ]);
      return true;
    } catch (err) {
      await this.clearUniqueLock(lockKey).catch(() => {});
      throw err;
    }
  }

  async clearUniqueLock(uniqueKey: string): Promise<void> {
    await this.#ensureReady();
    await this.#pool.query(
      `DELETE FROM ${this.#t.locks} WHERE key = $1`,
      [uniqueLockKey(uniqueKey)],
    );
  }

  async #acquireUniqueLock(
    uniqueKey: string,
    ttlMs: number,
  ): Promise<boolean> {
    const res = await this.#pool.query(
      `INSERT INTO ${this.#t.locks} (key, expires_at)
         VALUES ($1, NOW() + ($2 || ' milliseconds')::INTERVAL)
       ON CONFLICT (key) DO UPDATE
         SET expires_at = EXCLUDED.expires_at
         WHERE ${this.#t.locks}.expires_at <= NOW()
       RETURNING key`,
      [uniqueLockKey(uniqueKey), String(ttlMs)],
    );
    return (res.rowCount ?? 0) > 0;
  }

  listen(handler: (envelope: QueueEnvelope) => Promise<void>): Listener {
    this.#stopped = false;

    const drainOnce = async () => {
      if (this.#stopped) return;
      while (
        !this.#stopped && this.#inFlight < this.#opts.concurrency
      ) {
        const reserved = await this.#reserveOne();
        if (!reserved) break;
        this.#inFlight++;
        (async () => {
          try {
            await handler(reserved.envelope);
          } catch (_err) {
            // Worker.ts already handles errors. Anything reaching here is a
            // bug in the worker pipeline.
          } finally {
            // Ack the reservation. The row is only removed once the handler
            // has returned, so a worker crash leaves the row reserved and a
            // peer can reclaim it after reserveTtlMs.
            await this.#ackReservation(reserved.id).catch(() => {});
            this.#inFlight--;
            this.#wakeup();
          }
        })();
      }
    };

    const loop = async () => {
      // Make sure tables exist before we try to SELECT from them.
      await this.#ensureReady().catch(() => {});
      while (!this.#stopped) {
        await drainOnce();
        const wakeupPromise = new Promise<void>((resolve) => {
          this.#wakeup = resolve;
          this.#pollTimer = setTimeout(
            resolve,
            this.#opts.pollIntervalMs,
          ) as unknown as number;
        });
        await wakeupPromise;
        if (this.#pollTimer !== undefined) {
          clearTimeout(this.#pollTimer);
          this.#pollTimer = undefined;
        }
      }
    };

    this.#startNotifyClient().catch(() => {
      // Notify failure falls back to polling.
    });
    loop();

    this.#cleanupTimer = setInterval(() => {
      this.#pool.query(
        `DELETE FROM ${this.#t.locks} WHERE expires_at <= NOW();
         DELETE FROM ${this.#t.counters} WHERE expires_at <= NOW();`,
      ).catch(() => {});
    }, 60_000) as unknown as number;

    return {
      stop: async () => {
        this.#stopped = true;
        this.#wakeup();
        if (this.#pollTimer !== undefined) clearTimeout(this.#pollTimer);
        if (this.#cleanupTimer !== undefined) clearInterval(this.#cleanupTimer);
        if (this.#notifyClient) {
          try {
            await this.#notifyClient.end();
          } catch (_err) { /* ignore */ }
          this.#notifyClient = undefined;
        }
        const deadline = Date.now() + 5_000;
        while (this.#inFlight > 0 && Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 25));
        }
      },
    };
  }

  async close(): Promise<void> {
    this.#stopped = true;
    if (this.#cleanupTimer !== undefined) clearInterval(this.#cleanupTimer);
    if (this.#notifyClient) {
      try {
        await this.#notifyClient.end();
      } catch (_err) { /* ignore */ }
      this.#notifyClient = undefined;
    }
    await this.#pool.end();
  }

  async #reserveOne(): Promise<{ id: string; envelope: QueueEnvelope } | null> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const res = await client.query<{ id: string; envelope: QueueEnvelope }>(
        `WITH next AS (
           SELECT id FROM ${this.#t.jobs}
             WHERE available_at <= NOW()
               AND (
                 reserved_at IS NULL
                 OR reserved_at <= NOW() - ($1::bigint * INTERVAL '1 millisecond')
               )
             ORDER BY available_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE ${this.#t.jobs} AS jobs
            SET reserved_at = NOW()
           FROM next
          WHERE jobs.id = next.id
         RETURNING jobs.id, jobs.envelope`,
        [this.#opts.reserveTtlMs],
      );
      await client.query("COMMIT");
      if (res.rowCount === 0) return null;
      return { id: res.rows[0].id, envelope: res.rows[0].envelope };
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  async #ackReservation(id: string): Promise<void> {
    await this.#pool.query(
      `DELETE FROM ${this.#t.jobs} WHERE id = $1`,
      [id],
    );
  }

  async #startNotifyClient(): Promise<void> {
    const client = new Client({ connectionString: this.#connectionString });
    await client.connect();
    client.on("notification", () => this.#wakeup());
    client.on("error", () => {
      this.#notifyClient = undefined;
    });
    await client.query(`LISTEN ${this.#t.notifyChannel}`);
    this.#notifyClient = client;
  }
}
