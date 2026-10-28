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
  parsePokeRetryMs,
  probeUndelivered,
  runPokeLoop,
  runPokeWithGuard,
  type PokeLoopDeps,
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

// A deps bundle with sane defaults; override only what a test cares about.
function makeDeps(overrides: Partial<PokeLoopDeps> = {}): { deps: PokeLoopDeps; logs: string[] } {
  const logs: string[] = [];
  const d: PokeLoopDeps = {
    attempt: async () => "poked",
    hasMail: async () => false,
    sleep: async () => {},
    now: () => 0,
    log: (label) => logs.push(label),
    retryMs: 1000,
    maxMs: TWO_HOURS,
    ...overrides,
  };
  return { deps: d, logs };
}

// A server that accepts connections and never answers — a stalled broker.
function stallServer() {
  return Bun.serve({ port: 0, fetch: () => new Promise<Response>(() => {}) });
}

// --- pure decision + env parsing ---

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

test("guards CLAUDE_PEERS_POKE_RETRY_MS parsing", () => {
  expect(parsePokeRetryMs(undefined)).toBe(30_000);
  expect(parsePokeRetryMs("")).toBe(30_000);
  expect(parsePokeRetryMs("abc")).toBe(30_000);
  expect(parsePokeRetryMs("0")).toBe(30_000);
  expect(parsePokeRetryMs("-5")).toBe(30_000);
  expect(parsePokeRetryMs("999")).toBe(30_000);
  expect(parsePokeRetryMs("1000")).toBe(1000);
  expect(parsePokeRetryMs("45000")).toBe(45_000);
});

// --- the whole loop, injected ---

test("focused, focused, poked: retries twice then exits and frees the guard", async () => {
  const attempts = ["focused", "focused", "poked"];
  let i = 0;
  const { deps, logs } = makeDeps({ attempt: async () => attempts[i++]!, hasMail: async () => true });
  await runPokeWithGuard("sess", deps);
  expect(logs).toEqual(["deferred-focused", "deferred-focused", "sent"]);
  expect(isPokeLoopActive()).toBe(false);
});

test("an osascript error retries like focused; a missing pane ends the loop", async () => {
  const attempts = ["error", "error", "poked"];
  let i = 0;
  const { deps, logs } = makeDeps({ attempt: async () => attempts[i++]!, hasMail: async () => true });
  await runPokeLoop("sess", deps);
  expect(logs).toEqual(["deferred-error", "deferred-error", "sent"]);

  const { deps: gone, logs: goneLogs } = makeDeps({ attempt: async () => "not-found" });
  await runPokeLoop("sess", gone);
  expect(goneLogs).toEqual(["skipped-no-pane"]);
});

test("stops retrying as soon as the mail probe says there is nothing left", async () => {
  let probe = 0;
  const { deps, logs } = makeDeps({
    attempt: async () => "focused",
    hasMail: async () => {
      probe += 1;
      return probe === 1;
    },
  });
  await runPokeLoop("sess", deps);
  expect(logs).toEqual(["deferred-focused", "skipped-no-mail"]);
});

test("gives up at the cap when mail keeps arriving, on a fast clock", async () => {
  let t = 0;
  const { deps, logs } = makeDeps({
    attempt: async () => "focused",
    hasMail: async () => true,
    sleep: async (ms) => {
      t += ms;
    },
    now: () => t,
    retryMs: 1000,
    maxMs: 3000,
  });
  await runPokeLoop("sess", deps);
  expect(logs).toEqual(["deferred-focused", "deferred-focused", "deferred-focused", "gave-up"]);
});

test("a mail probe that never resolves still ends the loop at the cap and frees the guard", async () => {
  const stall = stallServer();
  try {
    let t = 0;
    const { deps, logs } = makeDeps({
      attempt: async () => "focused",
      hasMail: () => probeUndelivered("x", `http://127.0.0.1:${stall.port}`, 150),
      sleep: async (ms) => {
        t += ms;
      },
      now: () => t,
      retryMs: 1000,
      maxMs: 3000,
    });
    await runPokeWithGuard("sess", deps);
    expect(logs.at(-1)).toBe("gave-up");
    expect(isPokeLoopActive()).toBe(false);
  } finally {
    stall.stop(true);
  }
});

test("an exception mid-loop still releases the guard", async () => {
  const { deps } = makeDeps({
    attempt: async () => {
      throw new Error('boom');
    },
  });
  await expect(runPokeWithGuard("sess", deps)).rejects.toThrow('boom');
  expect(isPokeLoopActive()).toBe(false);
});

test("the mail probe gives up on a stalled broker instead of hanging", async () => {
  const stall = stallServer();
  try {
    const started = Date.now();
    const result = await probeUndelivered("x", `http://127.0.0.1:${stall.port}`, 200);
    expect(result).toBe(true); // can't tell → keep retrying
    expect(Date.now() - started).toBeLessThan(2000);
  } finally {
    stall.stop(true);
  }
});

// --- guard + logging ---

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

test("logs skipped-loop-active at most once per loop, not on every poll", async () => {
  let releaseGate: () => void = () => {};
  const gate = new Promise<void>((r) => {
    releaseGate = r;
  });
  const { deps: held, logs: heldLogs } = makeDeps({
    attempt: async () => {
      await gate;
      return "poked";
    },
  });
  const first = runPokeWithGuard("held", held); // acquires the guard synchronously
  expect(isPokeLoopActive()).toBe(true);

  const { deps: skip1, logs: skipLogs1 } = makeDeps();
  const { deps: skip2, logs: skipLogs2 } = makeDeps();
  await runPokeWithGuard("other", skip1);
  await runPokeWithGuard("other", skip2);
  expect(skipLogs1).toEqual(["skipped-loop-active"]);
  expect(skipLogs2).toEqual([]);

  releaseGate();
  await first;
  expect(heldLogs).toEqual(["sent"]);
  expect(isPokeLoopActive()).toBe(false);
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
