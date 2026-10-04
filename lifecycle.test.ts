import { test, expect } from "bun:test";
import { rmSync } from "node:fs";
import { mayStartBroker } from "./server.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const tmp = () => `/tmp/cp-life-${crypto.randomUUID()}`;

async function healthy(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(1000) });
    return res.ok;
  } catch {
    return false;
  }
}

function cleanupDb(db: string) {
  for (const f of [db, db + "-wal", db + "-shm"]) {
    rmSync(f, { force: true });
  }
}

test("swarm workers may not start a broker; coordinators and top-level peers may", () => {
  expect(mayStartBroker({ CLAUDE_AUTO_SWARM_ROLE: "glm-worker" })).toBe(false);
  expect(mayStartBroker({})).toBe(true);
  expect(mayStartBroker({ CLAUDE_AUTO_SWARM_STATE_DIR: "/x", CLAUDE_AUTO_SWARM_COORD_MODE: "claude" })).toBe(true);
});

test("a worker that finds no broker does not start one", async () => {
  const port = 7921;
  const db = tmp() + ".db";
  const child = Bun.spawn([process.execPath, "-e",
    `const { ensureBroker } = await import("./server.ts"); console.log(await ensureBroker());`], {
    cwd: import.meta.dir,
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: db,
      CLAUDE_PEERS_BROKER_LOG: tmp() + ".log", CLAUDE_AUTO_SWARM_ROLE: "worker" },
    stdout: "pipe", stderr: "ignore",
  });
  expect((await new Response(child.stdout).text()).trim()).toBe("false");
  await sleep(500);
  expect(await healthy(port)).toBe(false);
  cleanupDb(db);
});

test("a broker started by a session survives that session's whole process group being killed", async () => {
  const port = 7922;
  const db = tmp() + ".db";
  const starter = Bun.spawn([process.execPath, "-e",
    `const { ensureBroker } = await import("./server.ts"); console.log(await ensureBroker()); await new Promise(() => {});`], {
    cwd: import.meta.dir,
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: db,
      CLAUDE_PEERS_BROKER_LOG: tmp() + ".log", CLAUDE_AUTO_SWARM_ROLE: "" },
    stdout: "pipe", stderr: "ignore",
    detached: true, // its own group, standing in for a Claude session's group
  } as Parameters<typeof Bun.spawn>[1]);
  const reader = starter.stdout.getReader();
  const { value } = await reader.read();
  expect(new TextDecoder().decode(value).trim()).toBe("true");
  expect(await healthy(port)).toBe(true);

  process.kill(-starter.pid, "SIGHUP");
  process.kill(-starter.pid, "SIGKILL");
  await starter.exited;
  await sleep(500);
  try {
    expect(await healthy(port)).toBe(true);
  } finally {
    Bun.spawnSync(["sh", "-c", `lsof -ti tcp:${port} -sTCP:LISTEN | xargs kill 2>/dev/null`]);
    cleanupDb(db);
  }
}, 15_000);

test("the broker shuts itself down once no peers are left, and not while one is connected", async () => {
  const port = 7923;
  const db = tmp() + ".db";
  const broker = Bun.spawn([process.execPath, "broker.ts"], {
    cwd: import.meta.dir,
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: db,
      CLAUDE_PEERS_IDLE_SHUTDOWN_MS: "1500" },
    stdout: "ignore", stderr: "ignore",
  });
  try {
    await sleep(500);
    const reg = await fetch(`http://127.0.0.1:${port}/register`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pid: process.pid, cwd: process.cwd(), git_root: null, tty: null, summary: "" }),
    });
    const { id } = (await reg.json()) as { id: string };

    await sleep(3000); // twice the idle window, with a peer connected
    expect(await healthy(port)).toBe(true);

    await fetch(`http://127.0.0.1:${port}/unregister`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    const code = await Promise.race([broker.exited, sleep(5000).then(() => "still running")]);
    expect(code).toBe(0);
  } finally {
    broker.kill();
    cleanupDb(db);
  }
}, 15_000);
