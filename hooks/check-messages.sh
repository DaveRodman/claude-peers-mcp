#!/bin/bash
# claude-peers Stop hook — surfaces pending peer messages as a block decision.
# On any error or no messages: exit 0 with empty stdout so Stop proceeds normally.

RUNTIME_DIR="$HOME/.claude-peers-runtime"
DB_PATH="$HOME/.claude-peers.db"
LOG_FILE="$RUNTIME_DIR/hook.log"

log() {
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [check-messages] $*" >> "$LOG_FILE" 2>/dev/null || true
}

mkdir -p "$RUNTIME_DIR" 2>/dev/null || true

# Read stdin JSON payload
input=$(cat 2>/dev/null) || exit 0

# Guard: if stop_hook_active, we're already blocking — don't re-block
stop_hook_active=$(printf '%s' "$input" | python3 -c \
  "import sys,json; print('true' if json.load(sys.stdin).get('stop_hook_active') else 'false')" \
  2>/dev/null) || exit 0
[ "$stop_hook_active" = "true" ] && exit 0

# Claude Code's PID is our parent (hook is a direct subprocess of Claude Code)
claude_pid=$PPID

# Find MCP server PID: child of Claude Code running server.ts via bun
mcp_pid=$(ps -eo pid,ppid,args 2>/dev/null \
  | awk -v ppid="$claude_pid" '$2 == ppid && /server\.ts/ {print $1; exit}') \
  || { log "ps failed"; exit 0; }

if [ -z "$mcp_pid" ]; then
  log "No MCP server found for Claude Code PID $claude_pid"
  exit 0
fi

# Read peer_id from runtime file written by server.ts at registration
runtime_file="$RUNTIME_DIR/${mcp_pid}.json"
if [ ! -f "$runtime_file" ]; then
  log "Runtime file not found: $runtime_file (mcp_pid=$mcp_pid)"
  exit 0
fi

peer_id=$(python3 -c \
  "import json; print(json.load(open('${runtime_file}'))['peer_id'])" \
  2>/dev/null) || { log "Failed to parse runtime file"; exit 0; }

[ -z "$peer_id" ] && { log "Empty peer_id in $runtime_file"; exit 0; }

[ ! -f "$DB_PATH" ] && { log "DB not found: $DB_PATH"; exit 0; }

# Query + format + mark delivered — all in Python for correct JSON escaping
output=$(python3 - "$peer_id" "$DB_PATH" "$LOG_FILE" <<'PYEOF'
import sys, json, sqlite3, datetime

peer_id = sys.argv[1]
db_path  = sys.argv[2]
log_path = sys.argv[3]

def log(msg):
    try:
        with open(log_path, 'a') as f:
            f.write(f"{datetime.datetime.utcnow().isoformat()}Z [check-messages-py] {msg}\n")
    except Exception:
        pass

try:
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """SELECT m.id, m.from_id, m.text, m.sent_at,
                  COALESCE(p.summary, ''), COALESCE(p.cwd, '')
           FROM messages m
           LEFT JOIN peers p ON m.from_id = p.id
           WHERE m.to_id = ? AND m.delivered = 0
           ORDER BY m.sent_at ASC""",
        (peer_id,)
    ).fetchall()

    if not rows:
        sys.exit(0)

    ids = [r[0] for r in rows]
    blocks = []
    for _, from_id, text, sent_at, from_summary, from_cwd in rows:
        # XML-escape attribute values to avoid breaking the tag structure
        def xesc(s):
            return s.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
        block = (
            f'<channel source="claude-peers"'
            f' from_id="{xesc(from_id)}"'
            f' from_summary="{xesc(from_summary)}"'
            f' from_cwd="{xesc(from_cwd)}"'
            f' sent_at="{xesc(sent_at)}">\n'
            f'{text}\n'
            f'</channel>'
        )
        blocks.append(block)

    reason = '\n\n'.join(blocks)

    # Mark messages delivered before printing output to avoid re-surfacing on error
    placeholders = ','.join('?' * len(ids))
    conn.execute(f"UPDATE messages SET delivered=1 WHERE id IN ({placeholders})", ids)
    conn.commit()
    conn.close()

    log(f"Surfaced {len(ids)} message(s) for peer {peer_id}, ids={ids}")

    print(json.dumps({"decision": "block", "reason": reason}))

except Exception as e:
    log(f"Error: {e}")
    sys.exit(0)
PYEOF
) || exit 0

[ -n "$output" ] && printf '%s' "$output"
