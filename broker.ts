#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;

// --- Uptime tracking ---
const STARTED_AT = Date.now();

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// Migration: add process_start column for PID-reuse-safe liveness checks
// (2026-05-24). Pre-existing rows get NULL; new registrations include it.
// SQLite throws on duplicate-column; catch and ignore so the migration is
// idempotent across restarts.
try {
  db.run(`ALTER TABLE peers ADD COLUMN process_start TEXT`);
} catch (_e) {
  // column already exists — fine
}

// Migration: add draining + handoff_path columns (M2.22, 2026-05-26). When
// a peer self-handoffs (oah-peer soft cap), it sets draining=1 and a path
// to its partial-state handoff doc. /send-message refuses sends to a
// draining peer with a structured error so the sender's tool call gets the
// rejection synchronously — no wasted Gemini turn from the sender having
// to process an asynchronous bounce-back from the draining peer.
try {
  db.run(`ALTER TABLE peers ADD COLUMN draining INTEGER NOT NULL DEFAULT 0`);
} catch (_e) {
  // column already exists — fine
}
try {
  db.run(`ALTER TABLE peers ADD COLUMN handoff_path TEXT`);
} catch (_e) {
  // column already exists — fine
}

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Return `ps -p PID -o lstart=` output, or null if the PID doesn't exist.
// macOS-specific output format but only needs to be byte-comparable across
// calls, not parsed.
function procStartTime(pid: number): string | null {
  try {
    const result = Bun.spawnSync(["ps", "-p", String(pid), "-o", "lstart="]);
    if (result.exitCode !== 0) return null;
    const out = new TextDecoder().decode(result.stdout).trim();
    return out || null;
  } catch {
    return null;
  }
}

// `ps -o lstart=` prints the start time in the Mac's CURRENT time zone, so a
// time-zone change (e.g. travel) shifts every string while the processes are
// unchanged. Treat two values as the same process when they are identical, or
// when they differ only by a whole time-zone offset (multiple of 15 minutes,
// at most 26 hours apart). A recycled PID landing on exactly such an offset,
// to the second, is not a realistic case.
function sameProcessStart(recorded: string, current: string): boolean {
  if (recorded === current) return true;
  const a = Date.parse(recorded);
  const b = Date.parse(current);
  if (Number.isNaN(a) || Number.isNaN(b)) return false;
  const diff = Math.abs(a - b);
  return diff <= 26 * 3600_000 && diff % (15 * 60_000) === 0;
}

// A failed `ps` may mean permission denied or a transient spawn failure.
// Only ESRCH from an independent check proves the PID is gone.
function processIsGone(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ESRCH";
  }
}

// Sentinel round-trip to verify DB is writable and reads back correctly.
function sentinelRoundTrip(): void {
  const v = (db.query("PRAGMA user_version").get() as any).user_version as number;
  db.run(`PRAGMA user_version = ${v + 1}`);
  const back = (db.query("PRAGMA user_version").get() as any).user_version as number;
  if (back !== v + 1) throw new Error("sentinel readback mismatch");
}

// Clean up stale peers — PID dead OR PID recycled (alive but with a
// different start time than what we recorded). Recycling-detection
// matters because macOS reuses PIDs aggressively; without it, a dead
// peer's slot stays in the active list and routes messages to a black
// hole (or to whatever unrelated process now owns that PID).
function cleanStalePeers() {
  // Self-liveness: detect a stuck DB and exit so a fresh broker can take over.
  try {
    sentinelRoundTrip();
  } catch (e) {
    console.error("[broker] DB liveness check failed, exiting so a fresh broker can take the port:", e);
    process.exit(1);
  }

  const peers = db.query("SELECT id, pid, process_start FROM peers")
    .all() as { id: string; pid: number; process_start: string | null }[];
  for (const peer of peers) {
    let stale = false;
    const currentStart = procStartTime(peer.pid);
    if (currentStart === null) {
      stale = processIsGone(peer.pid);
    } else if (peer.process_start !== null && !sameProcessStart(peer.process_start, currentStart)) {
      stale = true;  // PID recycled to a different process
    }
    if (stale) {
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
    }
  }

  // Periodically checkpoint the WAL so it can't balloon.
  db.run("PRAGMA wal_checkpoint(TRUNCATE)");
}

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, process_start)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const updateDraining = db.prepare(`
  UPDATE peers SET draining = ?, handoff_path = ? WHERE id = ?
`);

const selectDraining = db.prepare(`
  SELECT draining, handoff_path FROM peers WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();

  // Remove any existing registration for this PID (re-registration)
  const existing = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }

  // If client didn't send process_start, fall back to looking it up server-side
  // from the PID. Caller is on localhost so this is the same PID we'd see in ps.
  const processStart = body.process_start ?? procStartTime(body.pid);

  insertPeer.run(id, body.pid, body.cwd, body.git_root, body.tty, body.summary, now, now, processStart);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): boolean {
  const result = updateLastSeen.run(new Date().toISOString(), body.id);
  return result.changes > 0;
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Verify each peer's process is still alive AND that the PID hasn't been
  // recycled to a different process (compare current start time with what
  // we recorded at registration). Null process_start indicates a pre-2026-05
  // peer row registered before this check existed — fall back to PID-only.
  return peers.filter((p) => {
    const current = procStartTime(p.pid);
    if (current === null) {
      if (processIsGone(p.pid)) {
        deletePeer.run(p.id);
        return false;
      }
      return true;
    }
    if (p.process_start !== null && !sameProcessStart(p.process_start, current)) {
      // PID recycled to a different process — original peer is gone.
      deletePeer.run(p.id);
      return false;
    }
    return true;
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists and read drain state in one query.
  const target = db.query(
    "SELECT id, draining, handoff_path FROM peers WHERE id = ?",
  ).get(body.to_id) as {
    id: string;
    draining: number | null;
    handoff_path: string | null;
  } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  // M2.22 — refuse sends to peers that have self-handoffed at their
  // soft cap. The sender's send_message tool call gets this error as
  // its synchronous tool result, so the sender's LLM sees "target
  // draining" and moves on without spending another turn processing
  // an asynchronous bounce-back message. Cleared automatically when
  // the peer is unregistered and a successor registers in the same
  // slot (a new id is generated; the draining flag belongs to the old
  // id, which is now gone).
  if (target.draining) {
    const where = target.handoff_path
      ? ` (partial-state handoff at ${target.handoff_path})`
      : "";
    return {
      ok: false,
      error:
        `Peer ${body.to_id} is draining at its --max-context-tokens soft cap${where}. ` +
        `Wait for the coordinator to respawn it before re-routing this task.`,
    };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handleSetDraining(body: {
  id: string;
  draining: boolean;
  handoff_path?: string | null;
}): { ok: boolean; error?: string } {
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.id) as
    | { id: string }
    | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.id} not found` };
  }
  updateDraining.run(
    body.draining ? 1 : 0,
    body.handoff_path ?? null,
    body.id,
  );
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];
  return { messages };
}

function handleMarkDelivered(body: { ids: number[] }): void {
  for (const id of body.ids) {
    markDelivered.run(id);
  }
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        // Honest health: real write+read round-trip + diagnostics.
        try {
          sentinelRoundTrip();
        } catch (e) {
          const msg = e instanceof Error ? e.message : String(e);
          return Response.json({ status: "degraded", error: msg }, { status: 500 });
        }

        const peers = (selectAllPeers.all() as Peer[]).length;
        const uptime_s = Math.round((Date.now() - STARTED_AT) / 1000);
        let wal_bytes = 0;
        try {
          wal_bytes = (await import("node:fs")).statSync(DB_PATH + "-wal").size;
        } catch {
          wal_bytes = 0;
        }
        return Response.json({ status: "ok", peers, db_writable: true, uptime_s, wal_bytes });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          return Response.json({ ok: true, known: handleHeartbeat(body as HeartbeatRequest) });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/set-draining":
          return Response.json(handleSetDraining(body as {
            id: string;
            draining: boolean;
            handoff_path?: string | null;
          }));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/mark-delivered":
          handleMarkDelivered(body as { ids: number[] });
          return Response.json({ ok: true });
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

// Bind first. A duplicate broker must fail before it can mutate the shared DB.
cleanStalePeers();
setInterval(cleanStalePeers, 30_000);

// Idle shutdown: once no peer has been connected for IDLE_SHUTDOWN_MS (default
// 15 minutes), exit. Sleeping panes stay registered, so sleep never triggers
// this; only closing every pane does. The next coordinator or top-level peer
// session starts a fresh broker.
const IDLE_SHUTDOWN_MS = parseInt(process.env.CLAUDE_PEERS_IDLE_SHUTDOWN_MS ?? String(15 * 60_000), 10);
let emptySince: number | null = null;
function shutDownIfIdle() {
  const { n } = db.query("SELECT COUNT(*) AS n FROM peers").get() as { n: number };
  if (n > 0) {
    emptySince = null;
    return;
  }
  emptySince ??= Date.now();
  if (Date.now() - emptySince >= IDLE_SHUTDOWN_MS) {
    console.error(`[claude-peers broker] no peers for ${Math.round(IDLE_SHUTDOWN_MS / 60_000)} min; shutting down`);
    process.exit(0);
  }
}
setInterval(shutDownIfIdle, Math.min(30_000, Math.max(250, IDLE_SHUTDOWN_MS / 4)));

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
