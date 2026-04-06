#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.server as server  # noqa: E402


def _has_permission(perms: set[str], permission: str) -> bool:
    return "*" in perms or permission in perms


def main() -> int:
    payload = server._get_navigation_groups()
    groups = payload.get("groups") or []
    if not isinstance(groups, list) or not groups:
        print("[FAIL] /api/navigation-groups has empty groups")
        return 1
    required_keys = {"id", "title", "order", "items"}
    for index, group in enumerate(groups):
        if not isinstance(group, dict) or not required_keys.issubset(group.keys()):
            print(f"[FAIL] invalid group shape at index={index}")
            return 1
        if not isinstance(group.get("items"), list) or not group["items"]:
            print(f"[FAIL] empty items in group={group.get('id')}")
            return 1
    role_matrix = server._role_permission_matrix()
    for role in ("admin", "pro", "limited"):
        perms = set(role_matrix.get(role) or [])
        visible = 0
        for group in groups:
            items = [item for item in (group.get("items") or []) if isinstance(item, dict) and _has_permission(perms, str(item.get("permission") or ""))]
            if items:
                visible += 1
        if visible <= 0:
            print(f"[FAIL] role={role} has no visible navigation groups after permission filter")
            return 1
    print("[OK] navigation permission smoke passed for admin/pro/limited")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
