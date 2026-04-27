// Run the Postgres schema migration for @snaapi/queue.
// Usage: DATABASE_URL=postgres://... deno run -A scripts/migrate.ts

import { PostgresDriver } from "../mod.ts";

const url = Deno.env.get("DATABASE_URL");
if (!url) {
  console.error("DATABASE_URL is required");
  Deno.exit(1);
}

const driver = new PostgresDriver({ connectionString: url });
try {
  await driver.migrate();
  console.log("snaapi_queue: schema applied");
} finally {
  await driver.close();
}
