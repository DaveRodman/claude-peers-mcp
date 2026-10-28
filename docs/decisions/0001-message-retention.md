# 0001 — Message retention: archive delivered messages after 7 days

Status: Accepted
Date: 2026-10-28

## Context

The broker's `messages` table had grown to ~76,000 rows (April to October) with no pruning
and no index on the hot poll query:

```sql
SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
```

That query runs on every poll from every peer, and with no index it full-scanned the table.
On 2026-10-28 the broker stalled twice under this load. The table was pruned by hand and the
index was added directly to the live database, but the broker code did not create the index,
so a fresh database would lack it and the table would grow again.

## Decision

- The broker creates `idx_messages_to_delivered ON messages(to_id, delivered)` at startup.
  The index name matches the one already present in the live database, so this is a no-op
  there and a fresh database now gets it too.
- Delivered messages older than **7 days** are appended to a JSONL archive at
  `$CLAUDE_PEERS_ARCHIVE_DIR` (default `~/.claude-peers-archive/`), file
  `messages-archive.jsonl`, one object per line with `id, from_id, to_id, sent_at, delivered, text`.
  The file is always **appended, never truncated**. Rows are deleted only after the append is
  flushed and fsynced, in a single transaction; if the append fails, nothing is deleted.
- **Undelivered messages are never pruned**, regardless of age.
- The prune runs once shortly after startup and then hourly, 5,000 rows per pass so a large
  backlog cannot block the broker's event loop.
- Retention is configurable via `CLAUDE_PEERS_RETENTION_DAYS` (default 7).

## Consequences / accepted risk

- Message history older than the retention window no longer lives in the broker database;
  it lives only in the archive file. Reading old messages means reading the JSONL archive.
- The archive grows without bound on disk. This is acceptable for now (it is plain text and
  was already ~75k rows); if it becomes a problem, add rotation.
- The 7-day window is measured from `sent_at`, not from delivery time, so a message that sits
  undelivered for a long time and is then delivered can be archived on the next pass. This is
  intended — undelivered rows are the ones that must never be dropped, and once delivered the
  clock is the send time.

## Revisit trigger

This change takes effect at the broker's **next natural restart**. Never force a restart to
pick it up: restarting the broker drops every connected session. Revisit if the archive file
size becomes a problem, or if 7 days proves too short for debugging message flows.
