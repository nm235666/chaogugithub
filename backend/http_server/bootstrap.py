#!/usr/bin/env python3
from __future__ import annotations

import db_compat as sqlite3
from db_compat import assert_database_ready
from http.server import ThreadingHTTPServer

from services.ai_retrieval_service import ensure_retrieval_tables as ai_retrieval_ensure_tables
from services.agent_service.chief_roundtable_v1 import ensure_roundtable_tables as _ensure_roundtable_tables
from services.agent_service.multi_role_v3 import ensure_multi_role_v3_tables

from backend.http_server.auth_users import _ensure_auth_tables
from backend.http_server.config import DB_PATH, HOST, PORT
from backend.http_server.handler import ApiHandler


def main() -> None:
    assert_database_ready()
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        ensure_multi_role_v3_tables(conn)
        _ensure_roundtable_tables(conn)
    finally:
        conn.close()
    ai_retrieval_ensure_tables(sqlite3_module=sqlite3, db_path=DB_PATH)

    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend API running on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
