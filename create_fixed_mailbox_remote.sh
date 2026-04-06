#!/usr/bin/env bash
set -euo pipefail

SSH_USER="zanbo"
SSH_HOST="192.168.5.58"
SSH_PASSWORD="zanbo123"

if ! command -v sshpass >/dev/null 2>&1; then
  echo "missing dependency: sshpass" >&2
  echo "install with: sudo apt-get update && sudo apt-get install -y sshpass" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <full_email> [ttl_minutes]" >&2
  echo "example: $0 tm2d5c43db718a@tianbo.asia 1440" >&2
  exit 1
fi

ADDRESS="$1"
TTL_MINUTES="${2:-1440}"
ACCOUNT="${ADDRESS%@*}"

sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no "$SSH_USER@$SSH_HOST" "ADDRESS='$ADDRESS' ACCOUNT='$ACCOUNT' TTL_MINUTES='$TTL_MINUTES' SUDO_PASSWORD='$SSH_PASSWORD' bash -s" <<'REMOTE'
set -euo pipefail

cat >/tmp/create_fixed_mailbox.py <<'PY'
import hashlib
import secrets
import sqlite3
import sys
import time

DB = "/var/lib/tempmail-api/state.sqlite3"
address = sys.argv[1]
account = sys.argv[2]
ttl_minutes = int(sys.argv[3])

now = int(time.time())
expires = now + ttl_minutes * 60
token = secrets.token_urlsafe(32)
token_hash = hashlib.sha256(token.encode()).hexdigest()

conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("SELECT 1 FROM mailboxes WHERE address=?", (address,))
if cur.fetchone():
    cur.execute(
        "UPDATE mailboxes SET token_hash=?,created_at=?,expires_at=? WHERE address=?",
        (token_hash, now, expires, address),
    )
    action = "updated"
else:
    cur.execute(
        "INSERT INTO mailboxes(account,address,password,token_hash,created_at,expires_at) VALUES(?,?,?,?,?,?)",
        (account, address, "", token_hash, now, expires),
    )
    action = "created"

conn.commit()
conn.close()

print("action=", action)
print("address=", address)
print("token=", token)
print("created_at=", now)
print("expires_at=", expires)
PY

echo "$SUDO_PASSWORD" | sudo -S python3 /tmp/create_fixed_mailbox.py "$ADDRESS" "$ACCOUNT" "$TTL_MINUTES"
rm -f /tmp/create_fixed_mailbox.py
REMOTE
