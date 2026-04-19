#!/usr/bin/env -S deno run -A

// Bumps the version in deno.json and prepends a new entry to CHANGELOG.md
// based on commits since the last tag.
//
// Usage:
//   deno task release patch
//   deno task release minor
//   deno task release major
//   deno task release 1.2.3

type BumpLevel = "patch" | "minor" | "major";

function parseSemver(v: string): [number, number, number] {
  const m = v.match(/^(\d+)\.(\d+)\.(\d+)$/);
  if (!m) throw new Error(`Invalid semver: ${v}`);
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

function bumpVersion(current: string, level: BumpLevel): string {
  const [maj, min, pat] = parseSemver(current);
  switch (level) {
    case "major":
      return `${maj + 1}.0.0`;
    case "minor":
      return `${maj}.${min + 1}.0`;
    case "patch":
      return `${maj}.${min}.${pat + 1}`;
  }
}

async function run(
  cmd: string[],
  opts: { capture?: boolean } = {},
): Promise<string> {
  const c = new Deno.Command(cmd[0], {
    args: cmd.slice(1),
    stdout: opts.capture ? "piped" : "inherit",
    stderr: opts.capture ? "piped" : "inherit",
  });
  const { code, stdout, stderr } = await c.output();
  if (code !== 0) {
    const err = opts.capture ? new TextDecoder().decode(stderr) : "";
    throw new Error(`Command failed: ${cmd.join(" ")}\n${err}`);
  }
  return opts.capture ? new TextDecoder().decode(stdout) : "";
}

async function getLastTag(): Promise<string | null> {
  try {
    const out = await run(["git", "describe", "--tags", "--abbrev=0"], {
      capture: true,
    });
    return out.trim() || null;
  } catch {
    return null;
  }
}

async function getCommitsSince(ref: string | null): Promise<string[]> {
  const args = ref
    ? ["git", "log", `${ref}..HEAD`, "--pretty=format:%s"]
    : ["git", "log", "--pretty=format:%s"];
  const out = await run(args, { capture: true });
  return out
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
}

function categorize(commits: string[]): Record<string, string[]> {
  const groups: Record<string, string[]> = {
    Added: [],
    Changed: [],
    Fixed: [],
    Other: [],
  };
  for (const c of commits) {
    if (/^feat(\(.+\))?!?:/i.test(c)) groups.Added.push(c);
    else if (/^fix(\(.+\))?!?:/i.test(c)) groups.Fixed.push(c);
    else if (
      /^(refactor|perf|chore|docs|style|test|build|ci)(\(.+\))?!?:/i.test(c)
    ) {
      groups.Changed.push(c);
    } else groups.Other.push(c);
  }
  return groups;
}

function renderChangelog(
  version: string,
  date: string,
  groups: Record<string, string[]>,
): string {
  const lines = [`## [${version}] - ${date}`, ""];
  let hasAny = false;
  for (const [heading, items] of Object.entries(groups)) {
    if (items.length === 0) continue;
    hasAny = true;
    lines.push(`### ${heading}`, "");
    for (const item of items) lines.push(`- ${item}`);
    lines.push("");
  }
  if (!hasAny) {
    lines.push("_No changes recorded._", "");
  }
  return lines.join("\n");
}

async function updateChangelog(entry: string): Promise<void> {
  const path = "CHANGELOG.md";
  const header = "# Changelog\n\n";
  let existing = "";
  try {
    existing = await Deno.readTextFile(path);
  } catch {
    existing = header;
  }

  const match = existing.match(/^(# Changelog[\s\S]*?\n\n)([\s\S]*)$/);
  if (match) {
    await Deno.writeTextFile(path, match[1] + entry + match[2]);
  } else {
    await Deno.writeTextFile(path, header + entry + existing);
  }
}

async function main() {
  const arg = Deno.args[0];
  if (!arg) {
    console.error("Usage: deno task release <patch|minor|major|x.y.z>");
    Deno.exit(1);
  }

  const status = await run(["git", "status", "--porcelain"], { capture: true });
  if (status.trim()) {
    console.error("Working tree is not clean. Commit or stash changes first.");
    Deno.exit(1);
  }

  const denoJson = JSON.parse(await Deno.readTextFile("deno.json"));
  const current: string = denoJson.version;

  let next: string;
  if (arg === "patch" || arg === "minor" || arg === "major") {
    next = bumpVersion(current, arg);
  } else {
    parseSemver(arg);
    next = arg;
  }

  console.log(`Bumping ${current} -> ${next}`);

  denoJson.version = next;
  await Deno.writeTextFile(
    "deno.json",
    JSON.stringify(denoJson, null, 2) + "\n",
  );

  const lastTag = await getLastTag();
  const commits = await getCommitsSince(lastTag);
  const groups = categorize(commits);
  const today = new Date().toISOString().slice(0, 10);
  const entry = renderChangelog(next, today, groups);
  await updateChangelog(entry);

  console.log("Updated deno.json and CHANGELOG.md.");
  console.log("");
  console.log("Review the changes, then run:");
  console.log(`  git add deno.json CHANGELOG.md`);
  console.log(`  git commit -s -m "chore: release v${next}"`);
  console.log(`  git tag v${next}`);
  console.log(`  git push && git push --tags`);
}

if (import.meta.main) {
  await main();
}
