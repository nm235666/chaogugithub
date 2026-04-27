#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

import db_compat as sqlite3
from backend.http_server.config import (
    BUILD_ID,
    DB_PATH,
    DEFAULT_ROLE_POLICIES,
    NAVIGATION_GROUPS_FALLBACK,
    PERMISSION_CATALOG_FALLBACK,
    PORT,
    RBAC_DYNAMIC_CONFIG_PATH,
    RBAC_DYNAMIC_SCHEMA_VERSION,
    REQUIRED_PUBLIC_ROUTE_PERMISSIONS,
    REQUIRED_ROLE_PERMISSIONS,
    ROLE_PERMISSIONS,
    ROUTE_PERMISSIONS_FALLBACK,
    SERVER_STARTED_AT_UTC,
)


def _normalize_permission_catalog(raw_catalog: object) -> tuple[list[dict[str, object]], int]:
    catalog_raw = raw_catalog if isinstance(raw_catalog, list) else []
    normalized: list[dict[str, object]] = []
    invalid = 0
    seen: set[str] = set()
    for item in catalog_raw:
        if not isinstance(item, dict):
            invalid += 1
            continue
        code = str(item.get("code") or "").strip()
        label = str(item.get("label") or code).strip() or code
        group = str(item.get("group") or "default").strip() or "default"
        if not code or code in seen:
            invalid += 1
            continue
        seen.add(code)
        normalized.append(
            {
                "code": code,
                "label": label,
                "group": group,
                "system_reserved": bool(item.get("system_reserved")),
            }
        )
    return normalized, invalid


def _normalize_route_permissions(raw_map: object, permission_allowlist: set[str]) -> tuple[dict[str, str], int]:
    if not isinstance(raw_map, dict):
        return {}, 0
    normalized: dict[str, str] = {}
    invalid = 0
    for k, v in raw_map.items():
        path = str(k or "").strip()
        permission = str(v or "").strip()
        if not path or not permission or permission not in permission_allowlist:
            invalid += 1
            continue
        normalized[path] = permission
    return normalized, invalid


def _ensure_required_public_routes(route_permissions: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    normalized = dict(route_permissions or {})
    fixed: list[str] = []
    for path, required in REQUIRED_PUBLIC_ROUTE_PERMISSIONS.items():
        current = str(normalized.get(path) or "").strip()
        if current == required:
            continue
        fixed.append(path if not current else f"{path}({current}->{required})")
        normalized[path] = required
    return normalized, fixed


def _normalize_navigation_groups(raw_groups: object, permission_allowlist: set[str]) -> tuple[list[dict[str, object]], int, int]:
    groups_raw = raw_groups if isinstance(raw_groups, list) else []
    normalized: list[dict[str, object]] = []
    invalid_groups = 0
    invalid_items = 0
    for group in groups_raw:
        if not isinstance(group, dict):
            invalid_groups += 1
            continue
        gid = str(group.get("id") or "").strip()
        title = str(group.get("title") or "").strip()
        try:
            order = int(group.get("order") or 0)
        except Exception:
            order = 0
        if not gid or not title:
            invalid_groups += 1
            continue
        items_raw = group.get("items")
        if not isinstance(items_raw, list):
            invalid_groups += 1
            continue
        items: list[dict[str, object]] = []
        for item in items_raw:
            if not isinstance(item, dict):
                invalid_items += 1
                continue
            to = str(item.get("to") or "").strip()
            label = str(item.get("label") or "").strip()
            desc = str(item.get("desc") or "").strip()
            permission = str(item.get("permission") or "").strip()
            if not to or not label or not permission or permission not in permission_allowlist:
                invalid_items += 1
                continue
            items.append({"to": to, "label": label, "desc": desc, "permission": permission})
        if not items:
            invalid_groups += 1
            continue
        normalized.append({"id": gid, "title": title, "order": order, "items": items})
    normalized.sort(key=lambda item: int(item.get("order") or 0))
    return normalized, invalid_groups, invalid_items


def _build_fallback_dynamic_rbac() -> dict[str, object]:
    return {
        "schema_version": RBAC_DYNAMIC_SCHEMA_VERSION,
        "version": "fallback",
        "source": "server_fallback",
        "permission_catalog": PERMISSION_CATALOG_FALLBACK,
        "route_permissions": ROUTE_PERMISSIONS_FALLBACK,
        "navigation_groups": NAVIGATION_GROUPS_FALLBACK,
        "validation": {
            "invalid_catalog_items": 0,
            "invalid_route_items": 0,
            "invalid_nav_groups": 0,
            "invalid_nav_items": 0,
            "fixed_public_route_items": 0,
        },
    }


def _load_dynamic_rbac_config() -> dict[str, object]:
    fallback = _build_fallback_dynamic_rbac()
    try:
        payload = json.loads(RBAC_DYNAMIC_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[rbac-dynamic] load failed from {RBAC_DYNAMIC_CONFIG_PATH}: {exc}")
        return fallback
    if not isinstance(payload, dict):
        print("[rbac-dynamic] config payload is not an object; use fallback")
        return fallback
    catalog, invalid_catalog = _normalize_permission_catalog(payload.get("permission_catalog"))
    if not catalog:
        print("[rbac-dynamic] no valid permission_catalog; use fallback")
        return fallback
    allowlist = {str(item.get("code") or "").strip() for item in catalog if str(item.get("code") or "").strip()}
    route_permissions, invalid_route_items = _normalize_route_permissions(payload.get("route_permissions"), allowlist)
    route_permissions, fixed_public_routes = _ensure_required_public_routes(route_permissions)
    nav_groups, invalid_nav_groups, invalid_nav_items = _normalize_navigation_groups(payload.get("navigation_groups"), allowlist)
    if fixed_public_routes:
        print(f"[rbac-dynamic] patched required public route mappings: {', '.join(fixed_public_routes)}")
    if not route_permissions or not nav_groups:
        print("[rbac-dynamic] route_permissions/navigation_groups invalid; use fallback")
        return fallback
    return {
        "schema_version": str(payload.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "version": str(payload.get("version") or "unknown"),
        "source": str(payload.get("source") or "repo_dynamic_config"),
        "permission_catalog": catalog,
        "route_permissions": route_permissions,
        "navigation_groups": nav_groups,
        "validation": {
            "invalid_catalog_items": invalid_catalog,
            "invalid_route_items": invalid_route_items,
            "invalid_nav_groups": invalid_nav_groups,
            "invalid_nav_items": invalid_nav_items,
            "fixed_public_route_items": len(fixed_public_routes),
        },
    }


RBAC_DYNAMIC_CONFIG = _load_dynamic_rbac_config()
ROLE_POLICIES_CACHE_SECONDS = 10.0
ROLE_POLICIES_CACHE: dict[str, object] = {"value": None, "expires_at": 0.0}
ROLE_POLICIES_LOCK = threading.Lock()


def _normalize_role_policy_permissions(value: object) -> set[str]:
    if isinstance(value, str):
        parts = [x.strip() for x in value.split(",")]
        return {x for x in parts if x}
    if isinstance(value, (list, tuple, set)):
        return {str(x or "").strip() for x in value if str(x or "").strip()}
    return set()


def _normalize_role_policy_limit(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw or raw in {"none", "null", "unlimited", "inf"}:
        return None
    try:
        num = int(raw)
    except Exception:
        return None
    return max(num, 0)


def _invalidate_role_policies_cache() -> None:
    with ROLE_POLICIES_LOCK:
        ROLE_POLICIES_CACHE["value"] = None
        ROLE_POLICIES_CACHE["expires_at"] = 0.0


def _effective_role_policies(force: bool = False) -> dict[str, dict[str, object]]:
    now = time.time()
    with ROLE_POLICIES_LOCK:
        cached = ROLE_POLICIES_CACHE.get("value")
        if not force and isinstance(cached, dict) and now < float(ROLE_POLICIES_CACHE.get("expires_at") or 0.0):
            return cached
    policies: dict[str, dict[str, object]] = {
        str(role): {
            "permissions": set(_normalize_role_policy_permissions(payload.get("permissions"))),
            "trend_daily_limit": _normalize_role_policy_limit(payload.get("trend_daily_limit")),
            "multi_role_daily_limit": _normalize_role_policy_limit(payload.get("multi_role_daily_limit")),
        }
        for role, payload in DEFAULT_ROLE_POLICIES.items()
    }
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        from backend.http_server import auth_users as _auth_users_mod

        _auth_users_mod._ensure_auth_tables(conn)
        rows = conn.execute(
            """
            SELECT role, permissions_json, trend_daily_limit, multi_role_daily_limit
            FROM app_auth_role_policies
            """
        ).fetchall()
        for row in rows:
            role = str(row["role"] or "").strip().lower()
            if not role:
                continue
            permissions_raw = str(row["permissions_json"] or "").strip()
            permissions: set[str] = set()
            if permissions_raw:
                try:
                    parsed = json.loads(permissions_raw)
                except Exception:
                    parsed = None
                if parsed is not None:
                    permissions = _normalize_role_policy_permissions(parsed)
            # Only fallback when DB field is truly empty.
            # If the row explicitly stores [], keep it as an empty permission set.
            if not permissions_raw and role in DEFAULT_ROLE_POLICIES:
                permissions = set(_normalize_role_policy_permissions(DEFAULT_ROLE_POLICIES[role].get("permissions")))
            policies[role] = {
                "permissions": permissions,
                "trend_daily_limit": _normalize_role_policy_limit(row["trend_daily_limit"]),
                "multi_role_daily_limit": _normalize_role_policy_limit(row["multi_role_daily_limit"]),
            }
    finally:
        conn.close()
    with ROLE_POLICIES_LOCK:
        ROLE_POLICIES_CACHE["value"] = policies
        ROLE_POLICIES_CACHE["expires_at"] = now + ROLE_POLICIES_CACHE_SECONDS
    for role, required in REQUIRED_ROLE_PERMISSIONS.items():
        if not required:
            continue
        payload = policies.get(role) or {}
        current = set(_normalize_role_policy_permissions(payload.get("permissions")))
        payload["permissions"] = current | set(required)
        policies[role] = payload
    return policies


def _role_permission_matrix() -> dict[str, list[str]]:
    policies = _effective_role_policies()
    out: dict[str, list[str]] = {}
    for role, payload in policies.items():
        out[str(role)] = sorted(str(x) for x in _normalize_role_policy_permissions(payload.get("permissions")))
    return out


def _effective_permissions_for_user(user: dict | None) -> list[str]:
    if not user:
        return []
    role = str((user or {}).get("role") or (user or {}).get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    policy = policies.get(role) or {}
    perms = _normalize_role_policy_permissions(policy.get("permissions"))
    return sorted(str(x) for x in perms)


def _build_info_payload() -> dict:
    return {
        "build_id": BUILD_ID,
        "port": PORT,
        "pid": os.getpid(),
        "started_at": SERVER_STARTED_AT_UTC,
    }


def _get_navigation_groups() -> dict:
    groups = RBAC_DYNAMIC_CONFIG.get("navigation_groups") if isinstance(RBAC_DYNAMIC_CONFIG, dict) else []
    if not isinstance(groups, list):
        groups = []
    return {
        "ok": True,
        "groups": groups,
        "version": str(RBAC_DYNAMIC_CONFIG.get("version") or "unknown"),
        "source": str(RBAC_DYNAMIC_CONFIG.get("source") or "unknown"),
        "schema_version": str(RBAC_DYNAMIC_CONFIG.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "validation": dict(RBAC_DYNAMIC_CONFIG.get("validation") or {}),
    }


def _get_dynamic_rbac_payload() -> dict:
    return {
        "schema_version": str(RBAC_DYNAMIC_CONFIG.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "version": str(RBAC_DYNAMIC_CONFIG.get("version") or "unknown"),
        "source": str(RBAC_DYNAMIC_CONFIG.get("source") or "unknown"),
        "permission_catalog": list(RBAC_DYNAMIC_CONFIG.get("permission_catalog") or []),
        "route_permissions": dict(RBAC_DYNAMIC_CONFIG.get("route_permissions") or {}),
        "navigation_groups": list(RBAC_DYNAMIC_CONFIG.get("navigation_groups") or []),
        "validation": dict(RBAC_DYNAMIC_CONFIG.get("validation") or {}),
    }

__all__ = [
    "_normalize_permission_catalog",
    "_normalize_route_permissions",
    "_ensure_required_public_routes",
    "_normalize_navigation_groups",
    "_build_fallback_dynamic_rbac",
    "_load_dynamic_rbac_config",
    "RBAC_DYNAMIC_CONFIG",
    "ROLE_POLICIES_CACHE_SECONDS",
    "ROLE_POLICIES_CACHE",
    "ROLE_POLICIES_LOCK",
    "_normalize_role_policy_permissions",
    "_normalize_role_policy_limit",
    "_invalidate_role_policies_cache",
    "_effective_role_policies",
    "_role_permission_matrix",
    "_effective_permissions_for_user",
    "_build_info_payload",
    "_get_navigation_groups",
    "_get_dynamic_rbac_payload",
]
