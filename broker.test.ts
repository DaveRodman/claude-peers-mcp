import { test, expect, beforeAll, afterAll } from "bun:test";

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
