import { test, expect, beforeAll, afterAll } from "bun:test";
import { Database } from "bun:sqlite";

const PORT = 7901;
const DB = `/tmp/cp-test-${crypto.randomUUID()}.db`;

let proc: ReturnType<typeof Bun.spawn> | null = null;
const url = (path: string) => `http://127.0.0.1:${PORT}${path}`;

beforeAll(async () => {
  proc = Bun.spawn(["bun", "broker.ts"], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB },
    cwd: import.meta.dir,
    stdout: "inherit",
    stderr: "inherit",
  });
  // wait for listen
  await new Promise((r) => setTimeout(r, 800));
});

afterAll(() => {
  if (proc) {
    proc.kill("SIGTERM");
  }
  try { Bun.file(DB).delete(); } catch {}
  try { Bun.file(DB + "-wal").delete(); } catch {}
});

let peerId: string;

function post(path: string, body: unknown) {
  return fetch(url(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

test("register a live peer", async () => {
  const res = await post("/register", {
    pid: process.pid,
    cwd: process.cwd(),
    git_root: null,
    tty: null,
    summary: "test",
  });
  expect(res.status).toBe(200);
  const data = await res.json();
  expect(typeof data.id).toBe("string");
  peerId = data.id;
});

test("heartbeat known peer", async () => {
  const res = await post("/heartbeat", { id: peerId });
  expect(res.status).toBe(200);
  const data = await res.json();
  expect(data.ok).toBe(true);
  expect(data.known).toBe(true);
});

test("heartbeat bogus peer", async () => {
  const res = await post("/heartbeat", { id: "bogus" });
  expect(res.status).toBe(200);
  const data = await res.json();
  expect(data.ok).toBe(true);
  expect(data.known).toBe(false);
});

test("health returns honest diagnostics", async () => {
  const res = await fetch(url("/health"));
  expect(res.status).toBe(200);
  const data = await res.json();
  expect(data.status).toBe("ok");
  expect(data.db_writable).toBe(true);
  expect(typeof data.peers).toBe("number");
  expect(typeof data.uptime_s).toBe("number");
  expect(data.uptime_s).toBeGreaterThanOrEqual(0);
  expect(typeof data.wal_bytes).toBe("number");
});

test("a duplicate broker cannot delete the running broker's peer", async () => {
  const duplicate = Bun.spawn([process.execPath, "broker.ts"], {
    env: { ...process.env, PATH: "/nonexistent", CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB },
    cwd: import.meta.dir,
    stdout: "ignore",
    stderr: "ignore",
  });
  expect(await duplicate.exited).not.toBe(0);
  const res = await post("/heartbeat", { id: peerId });
  expect((await res.json()).known).toBe(true);
});

test("failed process inspection preserves a live peer at startup and in listings", async () => {
  const isolatedPort = PORT + 1;
  const isolatedDb = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const db = new Database(isolatedDb);
  db.run(`CREATE TABLE peers (id TEXT PRIMARY KEY, pid INTEGER NOT NULL, cwd TEXT NOT NULL,
    git_root TEXT, tty TEXT, summary TEXT NOT NULL DEFAULT '', registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL, process_start TEXT)`);
  db.run("INSERT INTO peers VALUES (?, ?, ?, NULL, NULL, '', ?, ?, NULL)",
    ["live-peer", process.pid, process.cwd(), new Date().toISOString(), new Date().toISOString()]);
  db.close();
  const isolated = Bun.spawn([process.execPath, "broker.ts"], {
    env: { ...process.env, PATH: "/nonexistent", CLAUDE_PEERS_PORT: String(isolatedPort),
      CLAUDE_PEERS_DB: isolatedDb },
    cwd: import.meta.dir,
    stdout: "ignore",
    stderr: "inherit",
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 300));
    const heartbeat = await fetch(`http://127.0.0.1:${isolatedPort}/heartbeat`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: "live-peer" }),
    });
    expect((await heartbeat.json()).known).toBe(true);
    const listing = await fetch(`http://127.0.0.1:${isolatedPort}/list-peers`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scope: "machine" }),
    });
    expect((await listing.json()).some((peer: { id: string }) => peer.id === "live-peer")).toBe(true);
  } finally {
    isolated.kill("SIGTERM");
    await isolated.exited;
    try { Bun.file(isolatedDb).delete(); } catch {}
    try { Bun.file(isolatedDb + "-wal").delete(); } catch {}
    try { Bun.file(isolatedDb + "-shm").delete(); } catch {}
  }
});
