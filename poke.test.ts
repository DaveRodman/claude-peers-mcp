import { test, expect, afterAll } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  decidePoke,
  tryAcquirePokeLoop,
  releasePokeLoop,
  isPokeLoopActive,
  logPoke,
} from "./server.ts";

// Redirect the poke log to a temp dir. logPoke reads this at call time, so
// setting it here is enough — the real ~/.claude-peers-runtime is untouched.
const RUNTIME = mkdtempSync(join(tmpdir(), "cp-poke-"));
process.env.CLAUDE_PEERS_RUNTIME_DIR = RUNTIME;
const LOG = join(RUNTIME, "poke.log");

afterAll(() => {
  rmSync(RUNTIME, { recursive: true, force: true });
});

const TWO_HOURS = 2 * 60 * 60_000;

test("keeps retrying while there is undelivered mail and time remains", () => {
  expect(decidePoke({ elapsedMs: 0, maxMs: TWO_HOURS, hasUndeliveredMail: true })).toBe("retry");
  expect(decidePoke({ elapsedMs: 29_999, maxMs: TWO_HOURS, hasUndeliveredMail: true })).toBe("retry");
  expect(decidePoke({ elapsedMs: TWO_HOURS - 1, maxMs: TWO_HOURS, hasUndeliveredMail: true })).toBe("retry");
});

test("stops as soon as there is no undelivered mail left, even well before the cap", () => {
  expect(decidePoke({ elapsedMs: 0, maxMs: TWO_HOURS, hasUndeliveredMail: false })).toBe("skipped-no-mail");
  expect(decidePoke({ elapsedMs: 30_000, maxMs: TWO_HOURS, hasUndeliveredMail: false })).toBe("skipped-no-mail");
  expect(decidePoke({ elapsedMs: TWO_HOURS, maxMs: TWO_HOURS, hasUndeliveredMail: false })).toBe("skipped-no-mail");
});

test("gives up at the 2 h cap even when mail is still queued", () => {
  expect(decidePoke({ elapsedMs: TWO_HOURS, maxMs: TWO_HOURS, hasUndeliveredMail: true })).toBe("gave-up");
  expect(decidePoke({ elapsedMs: 9_000_000, maxMs: TWO_HOURS, hasUndeliveredMail: true })).toBe("gave-up");
});

test("only one poke retry loop can run at a time", () => {
  expect(isPokeLoopActive()).toBe(false);
  expect(tryAcquirePokeLoop()).toBe(true);
  expect(isPokeLoopActive()).toBe(true);
  // A new message arriving while a loop is pending must not start a second.
  expect(tryAcquirePokeLoop()).toBe(false);
  releasePokeLoop();
  expect(isPokeLoopActive()).toBe(false);
  expect(tryAcquirePokeLoop()).toBe(true);
  releasePokeLoop();
});

test("logs poke outcomes with a UTC timestamp to the bounded log file", () => {
  logPoke("sent", "outcome=poked elapsed=12ms");
  const lines = readFileSync(LOG, "utf8").trim().split("\n");
  expect(lines.length).toBe(1);
  expect(lines[0]).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z sent outcome=poked elapsed=12ms$/);

  // Bound: a log past the cap is truncated before the next append.
  writeFileSync(LOG, "x".repeat(1_200_000));
  logPoke("gave-up", "elapsed=7200000ms");
  const after = readFileSync(LOG, "utf8");
  expect(after.length).toBeLessThan(1_000_000);
  expect(after.trim().endsWith("gave-up elapsed=7200000ms")).toBe(true);
});
