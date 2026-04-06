#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.server as server  # noqa: E402


def _read_frontend_config() -> dict:
    path = ROOT / "apps" / "web" / "src" / "app" / "navigation.config.json"
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    frontend_payload = _read_frontend_config()
    backend_payload = server._get_navigation_groups()
    frontend_groups = frontend_payload.get("groups") or []
    backend_groups = backend_payload.get("groups") or []
    if frontend_groups != backend_groups:
        print("[FAIL] navigation groups mismatch between frontend config and backend payload")
        print(f"frontend_groups={len(frontend_groups)} backend_groups={len(backend_groups)}")
        return 1
    print("[OK] navigation groups aligned")
    print(f"version(frontend)={frontend_payload.get('version')} version(backend)={backend_payload.get('version')}")
    print(f"source(frontend)={frontend_payload.get('source')} source(backend)={backend_payload.get('source')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
