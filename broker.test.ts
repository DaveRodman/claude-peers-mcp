import { test, expect, beforeAll, afterAll } from "bun:test";
import { Database } from "bun:sqlite";
import { statSync, rmSync, mkdirSync, chmodSync } from 'node:fs';

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

test("a time-zone change does not evict live peers, but a recycled PID still does", async () => {
  const lstartIn = (tz: string) => new TextDecoder().decode(
    Bun.spawnSync(["ps", "-p", String(process.pid), "-o", "lstart="], { env: { ...process.env, TZ: tz } }).stdout,
  ).trim();
  const recordedInDenver = lstartIn("America/Denver");
  expect(recordedInDenver).not.toBe(lstartIn("Asia/Singapore"));

  const isolatedPort = PORT + 2;
  const isolatedDb = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const db = new Database(isolatedDb);
  db.run(`CREATE TABLE peers (id TEXT PRIMARY KEY, pid INTEGER NOT NULL, cwd TEXT NOT NULL,
    git_root TEXT, tty TEXT, summary TEXT NOT NULL DEFAULT '', registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL, process_start TEXT)`);
  const now = new Date().toISOString();
  db.run("INSERT INTO peers VALUES (?, ?, ?, NULL, NULL, '', ?, ?, ?)",
    ["traveller", process.pid, process.cwd(), now, now, recordedInDenver]);
  db.run("INSERT INTO peers VALUES (?, ?, ?, NULL, NULL, '', ?, ?, ?)",
    ["recycled", process.pid, process.cwd(), now, now, "Mon Jan  1 00:00:00 2001"]);
  db.close();
  const isolated = Bun.spawn([process.execPath, "broker.ts"], {
    env: { ...process.env, TZ: "Asia/Singapore", CLAUDE_PEERS_PORT: String(isolatedPort),
      CLAUDE_PEERS_DB: isolatedDb },
    cwd: import.meta.dir,
    stdout: "ignore",
    stderr: "inherit",
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 500));
    const listing = await fetch(`http://127.0.0.1:${isolatedPort}/list-peers`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scope: "machine" }),
    });
    const ids = (await listing.json()).map((peer: { id: string }) => peer.id);
    expect(ids).toContain("traveller");
    expect(ids).not.toContain("recycled");
  } finally {
    isolated.kill("SIGTERM");
    await isolated.exited;
    try { Bun.file(isolatedDb).delete(); } catch {}
    try { Bun.file(isolatedDb + "-wal").delete(); } catch {}
    try { Bun.file(isolatedDb + "-shm").delete(); } catch {}
  }
});


// --- Retention / archiving ---

const DAY_MS = 24 * 60 * 60 * 1000;

function seedRetentionDb(
  path: string,
  rows: { from_id: string; to_id: string; text: string; sent_at: string; delivered: number }[],
) {
  const seed = new Database(path);
  seed.run(`CREATE TABLE peers (id TEXT PRIMARY KEY, pid INTEGER NOT NULL, cwd TEXT NOT NULL,
    git_root TEXT, tty TEXT, summary TEXT NOT NULL DEFAULT '', registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL, process_start TEXT)`);
  seed.run(
    "INSERT INTO peers VALUES (?, ?, ?, NULL, NULL, '', ?, ?, NULL)",
    ['self', process.pid, process.cwd(), new Date().toISOString(), new Date().toISOString()],
  );
  seed.run(`CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, from_id TEXT NOT NULL,
    to_id TEXT NOT NULL, text TEXT NOT NULL, sent_at TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0)`);
  const ins = seed.prepare(
    'INSERT INTO messages (from_id, to_id, text, sent_at, delivered) VALUES (?, ?, ?, ?, ?)',
  );
  for (const r of rows) ins.run(r.from_id, r.to_id, r.text, r.sent_at, r.delivered);
  seed.close();
}

test('creates the poll and prune indexes on a fresh database', async () => {
  const inspect = new Database(DB);
  const names = (inspect.query(
    `SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_messages%'`,
  ).all() as { name: string }[])
    .map((r) => r.name)
    .sort();
  inspect.close();
  expect(names).toEqual(['idx_messages_delivered_sent', 'idx_messages_to_delivered']);
});

test('archives old delivered messages, keeps recent and undelivered ones', async () => {
  const port = PORT + 3;
  const dbPath = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const archiveDir = `/tmp/cp-archive-${crypto.randomUUID()}`;
  const archivePath = `${archiveDir}/messages-archive.jsonl`;
  const old = new Date(Date.now() - 10 * DAY_MS).toISOString();
  const recent = new Date().toISOString();
  seedRetentionDb(dbPath, [
    { from_id: 'a', to_id: 'self', text: 'old delivered', sent_at: old, delivered: 1 },
    { from_id: 'a', to_id: 'self', text: 'recent delivered', sent_at: recent, delivered: 1 },
    { from_id: 'a', to_id: 'self', text: 'old undelivered', sent_at: old, delivered: 0 },
  ]);
  const proc = Bun.spawn([process.execPath, 'broker.ts'], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: dbPath,
      CLAUDE_PEERS_ARCHIVE_DIR: archiveDir },
    cwd: import.meta.dir,
    stdout: 'ignore',
    stderr: 'inherit',
  });
  try {
    await new Promise((r) => setTimeout(r, 1200));

    const lines = (await Bun.file(archivePath).text()).split('\n').filter((l) => l.length > 0);
    expect(lines.length).toBe(1);
    const entry = JSON.parse(lines[0]!);
    expect(Object.keys(entry).sort()).toEqual(['delivered', 'from_id', 'id', 'sent_at', 'text', 'to_id']);
    expect(entry.text).toBe('old delivered');
    expect(entry.delivered).toBe(1);
    expect(typeof entry.id).toBe('number');

    const check = new Database(dbPath);
    const remaining = (check.query('SELECT text FROM messages').all() as { text: string }[])
      .map((r) => r.text)
      .sort();
    check.close();
    expect(remaining).toEqual(['old undelivered', 'recent delivered']);

    expect(statSync(archiveDir).mode & 0o777).toBe(0o700);
    expect(statSync(archivePath).mode & 0o777).toBe(0o600);
  } finally {
    proc.kill('SIGTERM');
    await proc.exited;
    rmSync(archiveDir, { recursive: true, force: true });
    try { Bun.file(dbPath).delete(); } catch {}
    try { Bun.file(dbPath + '-wal').delete(); } catch {}
    try { Bun.file(dbPath + '-shm').delete(); } catch {}
  }
});

test('appends to an existing archive instead of overwriting it', async () => {
  const port = PORT + 4;
  const dbPath = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const archiveDir = `/tmp/cp-archive-${crypto.randomUUID()}`;
  const archivePath = `${archiveDir}/messages-archive.jsonl`;
  mkdirSync(archiveDir, { recursive: true, mode: 0o700 });
  const sentinel = JSON.stringify({ id: 999999, from_id: 'z', to_id: 'z',
    sent_at: '2001-01-01T00:00:00.000Z', delivered: 1, text: 'pre-existing' });
  await Bun.write(archivePath, sentinel + '\n');

  const old = new Date(Date.now() - 10 * DAY_MS).toISOString();
  seedRetentionDb(dbPath, [
    { from_id: 'a', to_id: 'self', text: 'to be archived', sent_at: old, delivered: 1 },
  ]);
  const proc = Bun.spawn([process.execPath, 'broker.ts'], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: dbPath,
      CLAUDE_PEERS_ARCHIVE_DIR: archiveDir },
    cwd: import.meta.dir,
    stdout: 'ignore',
    stderr: 'inherit',
  });
  try {
    await new Promise((r) => setTimeout(r, 1200));
    const lines = (await Bun.file(archivePath).text()).split('\n').filter((l) => l.length > 0);
    expect(lines.length).toBe(2);
    expect(JSON.parse(lines[0]!).text).toBe('pre-existing');
    expect(JSON.parse(lines[1]!).text).toBe('to be archived');
  } finally {
    proc.kill('SIGTERM');
    await proc.exited;
    rmSync(archiveDir, { recursive: true, force: true });
    try { Bun.file(dbPath).delete(); } catch {}
    try { Bun.file(dbPath + '-wal').delete(); } catch {}
    try { Bun.file(dbPath + '-shm').delete(); } catch {}
  }
});


test('deletes nothing when the archive append fails', async () => {
  const port = PORT + 5;
  const dbPath = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const archiveDir = `/tmp/cp-archive-${crypto.randomUUID()}`;
  mkdirSync(archiveDir, { recursive: true, mode: 0o700 });
  // Read-only dir: the broker cannot create/open the archive file for append.
  chmodSync(archiveDir, 0o500);
  const old = new Date(Date.now() - 10 * DAY_MS).toISOString();
  seedRetentionDb(dbPath, [
    { from_id: 'a', to_id: 'self', text: 'must survive', sent_at: old, delivered: 1 },
  ]);
  const proc = Bun.spawn([process.execPath, 'broker.ts'], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: dbPath,
      CLAUDE_PEERS_ARCHIVE_DIR: archiveDir },
    cwd: import.meta.dir,
    stdout: 'ignore',
    stderr: 'inherit',
  });
  try {
    await new Promise((r) => setTimeout(r, 1200));
    const check = new Database(dbPath);
    const remaining = (check.query('SELECT text FROM messages').all() as { text: string }[])
      .map((r) => r.text);
    check.close();
    expect(remaining).toEqual(['must survive']);
    expect(await Bun.file(`${archiveDir}/messages-archive.jsonl`).exists()).toBe(false);
  } finally {
    proc.kill('SIGTERM');
    await proc.exited;
    chmodSync(archiveDir, 0o700);
    rmSync(archiveDir, { recursive: true, force: true });
    try { Bun.file(dbPath).delete(); } catch {}
    try { Bun.file(dbPath + '-wal').delete(); } catch {}
    try { Bun.file(dbPath + '-shm').delete(); } catch {}
  }
});

test('falls back to the default retention when the env value is unparseable', async () => {
  const port = PORT + 6;
  const dbPath = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const archiveDir = `/tmp/cp-archive-${crypto.randomUUID()}`;
  const archivePath = `${archiveDir}/messages-archive.jsonl`;
  const eightDays = new Date(Date.now() - 8 * DAY_MS).toISOString();
  seedRetentionDb(dbPath, [
    { from_id: 'a', to_id: 'self', text: 'past default window', sent_at: eightDays, delivered: 1 },
  ]);
  const proc = Bun.spawn([process.execPath, 'broker.ts'], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: dbPath,
      CLAUDE_PEERS_ARCHIVE_DIR: archiveDir, CLAUDE_PEERS_RETENTION_DAYS: 'abc' },
    cwd: import.meta.dir,
    stdout: 'ignore',
    stderr: 'inherit',
  });
  try {
    await new Promise((r) => setTimeout(r, 1200));
    const lines = (await Bun.file(archivePath).text()).split('\n').filter((l) => l.length > 0);
    expect(lines.length).toBe(1);
    const check = new Database(dbPath);
    const count = check.query('SELECT COUNT(*) AS n FROM messages').get() as { n: number };
    check.close();
    expect(count.n).toBe(0);
  } finally {
    proc.kill('SIGTERM');
    await proc.exited;
    rmSync(archiveDir, { recursive: true, force: true });
    try { Bun.file(dbPath).delete(); } catch {}
    try { Bun.file(dbPath + '-wal').delete(); } catch {}
    try { Bun.file(dbPath + '-shm').delete(); } catch {}
  }
});


// --- Slow-request / event-loop-lag logging ---

function collectStderr(proc: ReturnType<typeof Bun.spawn>): () => string {
  let text = '';
  const decoder = new TextDecoder();
  const reader = (proc.stderr as ReadableStream<Uint8Array>).getReader();
  void (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      text += decoder.decode(value, { stream: true });
    }
  })();
  return () => text;
}

function cleanupDb(dbPath: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { rmSync(dbPath + suffix, { force: true }); } catch {}
  }
}

function spawnLoggingBroker(port: number, extraEnv: Record<string, string>) {
  const dbPath = `/tmp/cp-test-${crypto.randomUUID()}.db`;
  const proc = Bun.spawn([process.execPath, 'broker.ts'], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: dbPath, ...extraEnv },
    cwd: import.meta.dir,
    stdout: 'ignore',
    stderr: 'pipe',
  });
  return { proc, dbPath, stderr: collectStderr(proc) };
}

test('logs slow requests and slow periodic jobs, with a UTC time and load average', async () => {
  const port = PORT + 10;
  const b = spawnLoggingBroker(port, { CLAUDE_PEERS_SLOW_MS: '0' });
  try {
    await new Promise((r) => setTimeout(r, 700));
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    expect(res.status).toBe(200);
    await new Promise((r) => setTimeout(r, 500));

    const text = b.stderr();
    expect(text).toMatch(/slow request \d{4}-\d{2}-\d{2}T[\d:.]+Z \/health \d+ms load=/);
    expect(text).toMatch(/slow job \d{4}-\d{2}-\d{2}T[\d:.]+Z cleanStalePeers \d+ms load=/);
    expect(text).toMatch(/slow job \d{4}-\d{2}-\d{2}T[\d:.]+Z pruneMessages \d+ms load=/);
  } finally {
    b.proc.kill('SIGTERM');
    await b.proc.exited;
    cleanupDb(b.dbPath);
  }
});

test('does not log fast requests or fast jobs', async () => {
  const port = PORT + 11;
  const b = spawnLoggingBroker(port, { CLAUDE_PEERS_SLOW_MS: '60000' });
  try {
    await new Promise((r) => setTimeout(r, 700));
    await fetch(`http://127.0.0.1:${port}/health`);
    await fetch(`http://127.0.0.1:${port}/`);
    await new Promise((r) => setTimeout(r, 500));

    expect(b.stderr()).not.toContain('slow request');
    expect(b.stderr()).not.toContain('slow job');
  } finally {
    b.proc.kill('SIGTERM');
    await b.proc.exited;
    cleanupDb(b.dbPath);
  }
});

test('rate-limits slow-request logging to one line per second, with a suppressed count', async () => {
  const port = PORT + 12;
  const b = spawnLoggingBroker(port, { CLAUDE_PEERS_SLOW_MS: '0' });
  try {
    await new Promise((r) => setTimeout(r, 700));
    for (let i = 0; i < 12; i++) {
      await fetch(`http://127.0.0.1:${port}/health`);
    }
    await new Promise((r) => setTimeout(r, 400));

    const lines = b.stderr().split('\n').filter((l) => l.includes('slow request'));
    expect(lines.length).toBe(1);

    // The dropped lines are reported once the window closes.
    await new Promise((r) => setTimeout(r, 900));
    expect(b.stderr()).toMatch(/slow-request: \d+ more suppressed/);
  } finally {
    b.proc.kill('SIGTERM');
    await b.proc.exited;
    cleanupDb(b.dbPath);
  }
});
