#!/usr/bin/env python3
"""
Compatibility facade for the monolithic server.

Implementation lives in backend.http_server; keep importing from backend.server for scripts and tests.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.http_server.config import (
    BUILD_ID,
    DB_PATH,
    HOST,
    PORT,
    SERVER_STARTED_AT_UTC,
    WEB_DIST_DIR,
)
from backend.http_server.handler import ApiHandler
from backend.http_server.legacy_queries import _build_stock_score_universe, _sanitize_json_value
from backend.http_server.llm_workbench import (
    build_signal_event_logic_view,
    build_signal_logic_view,
    build_multi_role_context,
    ensure_logic_view_cache_table,
    extract_logic_view_from_markdown,
    get_or_build_cached_logic_view,
)
from backend.http_server.multi_role_runtime import build_agent_service_deps
from backend.http_server.rbac import (
    _get_dynamic_rbac_payload,
    _get_navigation_groups,
    _role_permission_matrix,
)

if __name__ == "__main__":
    from backend.http_server.bootstrap import main as _bootstrap_main

    _bootstrap_main()
