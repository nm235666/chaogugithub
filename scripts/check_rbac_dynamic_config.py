#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "rbac_dynamic.config.json"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.server as server  # noqa: E402


def main() -> int:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        print("[FAIL] config root must be object")
        return 1
    catalog = payload.get("permission_catalog") if isinstance(payload.get("permission_catalog"), list) else []
    route_permissions = payload.get("route_permissions") if isinstance(payload.get("route_permissions"), dict) else {}
    nav_groups = payload.get("navigation_groups") if isinstance(payload.get("navigation_groups"), list) else []
    codes = {str(item.get("code") or "").strip() for item in catalog if isinstance(item, dict)}
    codes = {x for x in codes if x}
    if not codes:
        print("[FAIL] permission_catalog is empty")
        return 1
    invalid_routes = [k for k, v in route_permissions.items() if str(v or "").strip() not in codes]
    if invalid_routes:
        print(f"[FAIL] route_permissions has unknown permission codes: {invalid_routes[:5]}")
        return 1
    invalid_nav = []
    for g in nav_groups:
        if not isinstance(g, dict):
            invalid_nav.append("<group-not-object>")
            continue
        for item in (g.get("items") if isinstance(g.get("items"), list) else []):
            if not isinstance(item, dict):
                continue
            perm = str(item.get("permission") or "").strip()
            if perm not in codes:
                invalid_nav.append(f"{g.get('id')}:{item.get('to')}")
    if invalid_nav:
        print(f"[FAIL] navigation_groups has unknown permission codes: {invalid_nav[:5]}")
        return 1
    backend_payload = server._get_dynamic_rbac_payload()
    if str(backend_payload.get("version")) != str(payload.get("version")):
        print("[FAIL] backend loaded version mismatch")
        return 1
    print("[OK] rbac_dynamic config consistency passed")
    print(f"version={payload.get('version')} schema={payload.get('schema_version')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
