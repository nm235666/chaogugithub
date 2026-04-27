#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db_compat as sqlite3
from backend.http_server import config


def _effective_role_policies(force: bool = False):
    from backend.http_server import rbac as _rbac_mod

    return _rbac_mod._effective_role_policies(force=force)


def _invalidate_role_policies_cache() -> None:
    from backend.http_server import rbac as _rbac_mod

    _rbac_mod._invalidate_role_policies_cache()


def _normalize_role_policy_permissions(value: object):
    from backend.http_server import rbac as _rbac_mod

    return _rbac_mod._normalize_role_policy_permissions(value)


def _normalize_role_policy_limit(value: object):
    from backend.http_server import rbac as _rbac_mod

    return _rbac_mod._normalize_role_policy_limit(value)


def _ensure_auth_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            session_token_hash TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_sessions_user ON app_auth_sessions(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_sessions_expire ON app_auth_sessions(expires_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invite_code TEXT NOT NULL UNIQUE,
            max_uses INTEGER NOT NULL DEFAULT 1,
            used_count INTEGER NOT NULL DEFAULT 0,
            expires_at TIMESTAMP,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_by TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_email_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            email TEXT NOT NULL,
            verify_code TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_usage_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            usage_date TEXT NOT NULL,
            trend_count INTEGER NOT NULL DEFAULT 0,
            multi_role_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_app_auth_usage_user_date ON app_auth_usage_daily(user_id, usage_date)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_role_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL UNIQUE,
            permissions_json TEXT NOT NULL DEFAULT '[]',
            trend_daily_limit INTEGER,
            multi_role_daily_limit INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_verify_user ON app_auth_email_verifications(user_id, used_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_verify_email ON app_auth_email_verifications(email, used_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            username TEXT,
            user_id INTEGER,
            result TEXT NOT NULL DEFAULT 'ok',
            detail TEXT,
            ip TEXT,
            user_agent TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_audit_time ON app_auth_audit_logs(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_audit_user ON app_auth_audit_logs(username, user_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_password_resets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            reset_code TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_pwd_reset_user ON app_auth_password_resets(user_id, used_at)")

    # migrate columns for existing deployments
    try:
        columns = {
            str(r["name"]) if isinstance(r, dict) else str(r[1])
            for r in conn.execute("PRAGMA table_info(app_auth_users)").fetchall()
        }
    except Exception:
        columns = set()
    if "email" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN email TEXT")
    if "email_verified" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN email_verified INTEGER NOT NULL DEFAULT 0")
    if "role" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN role TEXT NOT NULL DEFAULT 'limited'")
    if "tier" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN tier TEXT NOT NULL DEFAULT 'limited'")
    if "invite_code_used" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN invite_code_used TEXT")
    if "failed_login_count" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN failed_login_count INTEGER NOT NULL DEFAULT 0")
    if "locked_until" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN locked_until TIMESTAMP")
    if "last_login_at" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN last_login_at TIMESTAMP")

    try:
        usage_columns = {
            str(r["name"]) if isinstance(r, dict) else str(r[1])
            for r in conn.execute("PRAGMA table_info(app_auth_usage_daily)").fetchall()
        }
    except Exception:
        usage_columns = set()
    if "multi_role_count" not in usage_columns:
        conn.execute("ALTER TABLE app_auth_usage_daily ADD COLUMN multi_role_count INTEGER NOT NULL DEFAULT 0")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_app_auth_users_email ON app_auth_users(email)")
    for role, payload in config.DEFAULT_ROLE_POLICIES.items():
        perms_json = json.dumps(
            sorted(_normalize_role_policy_permissions(payload.get("permissions"))),
            ensure_ascii=False,
        )
        trend_limit = _normalize_role_policy_limit(payload.get("trend_daily_limit"))
        multi_role_limit = _normalize_role_policy_limit(payload.get("multi_role_daily_limit"))
        conn.execute(
            """
            INSERT OR IGNORE INTO app_auth_role_policies
            (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (str(role), perms_json, trend_limit, multi_role_limit),
        )


def _hash_password(password: str, salt_hex: str | None = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 210_000)
    return f"pbkdf2_sha256$210000${salt.hex()}${digest.hex()}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, rounds_s, salt_hex, digest_hex = (stored_hash or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        rounds = int(rounds_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
        return secrets.compare_digest(actual, expected)
    except Exception:
        return False


def _invalidate_auth_users_count_cache():
    with config.AUTH_USERS_COUNT_LOCK:
        config.AUTH_USERS_COUNT_CACHE["value"] = -1
        config.AUTH_USERS_COUNT_CACHE["expires_at"] = 0.0


def _active_auth_users_count(force: bool = False) -> int:
    now = time.time()
    with config.AUTH_USERS_COUNT_LOCK:
        if not force and config.AUTH_USERS_COUNT_CACHE["value"] >= 0 and now < float(config.AUTH_USERS_COUNT_CACHE["expires_at"]):
            return int(config.AUTH_USERS_COUNT_CACHE["value"])
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE is_active = 1").fetchone()
        count = int((row[0] if row else 0) or 0)
    finally:
        conn.close()
    with config.AUTH_USERS_COUNT_LOCK:
        config.AUTH_USERS_COUNT_CACHE["value"] = count
        config.AUTH_USERS_COUNT_CACHE["expires_at"] = now + config.AUTH_USERS_COUNT_CACHE_SECONDS
    return count


def _new_session_token() -> str:
    return f"u_{secrets.token_urlsafe(32)}"


def _record_auth_audit(
    event_type: str,
    username: str = "",
    user_id: int | None = None,
    result: str = "ok",
    detail: str = "",
    ip: str = "",
    user_agent: str = "",
):
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_audit_logs (event_type, username, user_id, result, detail, ip, user_agent, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                (event_type or "").strip(),
                (username or "").strip(),
                int(user_id or 0) if user_id is not None else None,
                (result or "ok").strip(),
                (detail or "").strip()[:500],
                (ip or "").strip()[:100],
                (user_agent or "").strip()[:300],
            ),
        )
    finally:
        conn.close()


def _user_locked(locked_until) -> bool:
    raw = str(locked_until or "").strip()
    if not raw:
        return False
    norm = raw.replace("T", " ").replace("Z", "")
    try:
        when = datetime.strptime(norm[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return False
    return when > datetime.utcnow()


def _register_login_failure(conn, user_id: int):
    row = conn.execute(
        "SELECT failed_login_count FROM app_auth_users WHERE id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    current = int((row[0] if row else 0) or 0)
    next_count = current + 1
    if next_count >= max(config.AUTH_LOCK_THRESHOLD, 1):
        lock_until = (datetime.now(timezone.utc) + timedelta(minutes=max(config.AUTH_LOCK_MINUTES, 1))).replace(tzinfo=None)
        conn.execute(
            "UPDATE app_auth_users SET failed_login_count = ?, locked_until = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (next_count, lock_until, user_id),
        )
    else:
        conn.execute(
            "UPDATE app_auth_users SET failed_login_count = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (next_count, user_id),
        )


def _clear_login_failure(conn, user_id: int):
    conn.execute(
        "UPDATE app_auth_users SET failed_login_count = 0, locked_until = NULL, last_login_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (user_id,),
    )


def _generate_invite_code() -> str:
    return f"INV-{secrets.token_hex(4).upper()}"


def _create_invite_code(
    max_uses: int = 1,
    expires_at: str = "",
    created_by: str = "",
    explicit_code: str = "",
) -> dict:
    code = (explicit_code or _generate_invite_code()).strip().upper()
    max_uses = max(1, int(max_uses or 1))
    expiry = (expires_at or "").strip() or None
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_invites (invite_code, max_uses, used_count, expires_at, is_active, created_by, created_at, updated_at)
            VALUES (?, ?, 0, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (code, max_uses, expiry, (created_by or "").strip()),
        )
    finally:
        conn.close()
    return {"invite_code": code, "max_uses": max_uses, "expires_at": expiry}


def _assert_invite_valid(conn, invite_code: str) -> str:
    code = (invite_code or "").strip().upper()
    if not code:
        raise ValueError("邀请码不能为空")
    row = conn.execute(
        """
        SELECT invite_code, max_uses, used_count, is_active, expires_at
        FROM app_auth_invites
        WHERE invite_code = ?
        LIMIT 1
        """,
        (code,),
    ).fetchone()
    if not row:
        raise RuntimeError("邀请码不存在")
    max_uses = int((row[1] or 0) or 0)
    used_count = int((row[2] or 0) or 0)
    is_active = int((row[3] or 0) or 0) == 1
    expires_at = row[4]
    if not is_active:
        raise RuntimeError("邀请码已失效")
    if max_uses > 0 and used_count >= max_uses:
        raise RuntimeError("邀请码已用尽")
    if expires_at:
        expires_s = str(expires_at).strip().replace("T", " ").replace("Z", "")
        if expires_s and datetime.strptime(expires_s[:19], "%Y-%m-%d %H:%M:%S") < datetime.utcnow():
            raise RuntimeError("邀请码已过期")
    return code


def _reserve_invite_use(conn, invite_code: str):
    conn.execute(
        "UPDATE app_auth_invites SET used_count = COALESCE(used_count, 0) + 1, updated_at = CURRENT_TIMESTAMP WHERE invite_code = ?",
        (invite_code,),
    )


def _create_email_verification(conn, user_id: int, email: str) -> dict:
    verify_code = f"{secrets.randbelow(1000000):06d}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(tzinfo=None)
    conn.execute(
        "UPDATE app_auth_email_verifications SET used_at = CURRENT_TIMESTAMP WHERE user_id = ? AND used_at IS NULL",
        (user_id,),
    )
    conn.execute(
        """
        INSERT INTO app_auth_email_verifications (user_id, email, verify_code, expires_at, used_at, created_at)
        VALUES (?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)
        """,
        (user_id, email, verify_code, expires_at),
    )
    return {
        "sent": True,
        "expires_minutes": 15,
        "email_masked": re.sub(r"(^.).+(@.+$)", r"\1***\2", email),
        "dev_verify_code": verify_code if os.getenv("AUTH_DEV_EXPOSE_VERIFY_CODE", "1").strip().lower() not in {"0", "false", "no"} else "",
    }


def _register_auth_user(
    username: str,
    password: str,
    display_name: str = "",
    email: str = "",
    invite_code: str = "",
) -> tuple[str, dict, dict]:
    normalized_username = (username or "").strip()
    pwd = (password or "").strip()
    display = (display_name or "").strip()
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        normalized_email = f"{normalized_username.lower()}@local.invalid"
    if not re.fullmatch(r"[A-Za-z0-9_.\-]{3,32}", normalized_username):
        raise ValueError("用户名仅支持 3-32 位英文、数字、下划线、点和中划线")
    if len(pwd) < 6:
        raise ValueError("密码至少 6 位")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        invite = _assert_invite_valid(conn, invite_code)
        exists = conn.execute("SELECT id FROM app_auth_users WHERE username = ?", (normalized_username,)).fetchone()
        if exists:
            raise RuntimeError("用户名已存在")
        email_exists = conn.execute("SELECT id FROM app_auth_users WHERE email = ? LIMIT 1", (normalized_email,)).fetchone()
        if email_exists:
            raise RuntimeError("邮箱已被注册")
        conn.execute(
            """
            INSERT INTO app_auth_users (
                username, password_hash, display_name, email, email_verified, role, tier, invite_code_used, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, 'limited', 'limited', ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (normalized_username, _hash_password(pwd), display, normalized_email, invite),
        )
        _reserve_invite_use(conn, invite)
        _invalidate_auth_users_count_cache()
        row = conn.execute(
            "SELECT id, username, display_name, email, role, tier, email_verified FROM app_auth_users WHERE username = ? LIMIT 1",
            (normalized_username,),
        ).fetchone()
        if not row:
            raise RuntimeError("注册后未找到用户")
        user_id = int(row[0])
        token = _new_session_token()
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=max(config.AUTH_SESSION_DAYS, 1))).replace(tzinfo=None)
        conn.execute("DELETE FROM app_auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute(
            "INSERT INTO app_auth_sessions (user_id, session_token_hash, expires_at, created_at, last_seen_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (user_id, token_hash, expires_at),
        )
        conn.execute(
            """
            DELETE FROM app_auth_sessions
            WHERE user_id = ? AND id NOT IN (
                SELECT id FROM app_auth_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 10
            )
            """,
            (user_id, user_id),
        )
        user = {
            "id": user_id,
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[3] or ""),
            "role": str(row[4] or "limited").strip().lower(),
            "tier": str(row[5] or "limited").strip().lower(),
            "email_verified": int((row[6] or 0)) == 1,
        }
        return token, user, {}
    finally:
        conn.close()


def _login_auth_user(username: str, password: str) -> tuple[str, dict]:
    normalized_username = (username or "").strip()
    pwd = (password or "").strip()
    if not normalized_username or not pwd:
        raise ValueError("用户名和密码不能为空")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT id, username, display_name, password_hash, is_active, email, role, tier, email_verified, failed_login_count, locked_until FROM app_auth_users WHERE username = ? LIMIT 1",
            (normalized_username,),
        ).fetchone()
        if not row or int((row[4] or 0)) != 1:
            raise RuntimeError("用户名或密码错误")
        if _user_locked(row[10]):
            raise RuntimeError(f"账号已临时锁定，请 {config.AUTH_LOCK_MINUTES} 分钟后再试")
        if not _verify_password(pwd, str(row[3] or "")):
            _register_login_failure(conn, int(row[0]))
            raise RuntimeError("用户名或密码错误")
        user_id = int(row[0])
        _clear_login_failure(conn, user_id)
        token = _new_session_token()
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=max(config.AUTH_SESSION_DAYS, 1))).replace(tzinfo=None)
        conn.execute("DELETE FROM app_auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute(
            "INSERT INTO app_auth_sessions (user_id, session_token_hash, expires_at, created_at, last_seen_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (user_id, token_hash, expires_at),
        )
        conn.execute(
            """
            DELETE FROM app_auth_sessions
            WHERE user_id = ? AND id NOT IN (
                SELECT id FROM app_auth_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 10
            )
            """,
            (user_id, user_id),
        )
        return token, {
            "id": user_id,
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[5] or ""),
            "role": str(row[6] or "limited").strip().lower(),
            "tier": str(row[7] or "limited").strip().lower(),
            "email_verified": int((row[8] or 0)) == 1,
        }
    finally:
        conn.close()


def _verify_email_code(username: str = "", email: str = "", verify_code: str = "") -> dict:
    normalized_username = (username or "").strip()
    normalized_email = (email or "").strip().lower()
    normalized_code = (verify_code or "").strip()
    if not normalized_code:
        raise ValueError("验证码不能为空")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if normalized_username:
            user = conn.execute("SELECT id, username, email FROM app_auth_users WHERE username = ? LIMIT 1", (normalized_username,)).fetchone()
        elif normalized_email:
            user = conn.execute("SELECT id, username, email FROM app_auth_users WHERE email = ? LIMIT 1", (normalized_email,)).fetchone()
        else:
            raise ValueError("请提供用户名或邮箱")
        if not user:
            raise RuntimeError("用户不存在")
        user_id = int(user[0])
        user_email = str(user[2] or "").strip().lower()
        row = conn.execute(
            """
            SELECT id
            FROM app_auth_email_verifications
            WHERE user_id = ?
              AND email = ?
              AND verify_code = ?
              AND used_at IS NULL
              AND expires_at > CURRENT_TIMESTAMP
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, user_email, normalized_code),
        ).fetchone()
        if not row:
            raise RuntimeError("验证码无效或已过期")
        conn.execute("UPDATE app_auth_email_verifications SET used_at = CURRENT_TIMESTAMP WHERE id = ?", (int(row[0]),))
        conn.execute(
            "UPDATE app_auth_users SET email_verified = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,),
        )
        return {"user_id": user_id, "username": str(user[1] or ""), "email": user_email, "verified": True}
    finally:
        conn.close()


def _resend_email_verification(username: str = "", email: str = "") -> dict:
    normalized_username = (username or "").strip()
    normalized_email = (email or "").strip().lower()
    if not normalized_username and not normalized_email:
        raise ValueError("请提供用户名或邮箱")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if normalized_username:
            user = conn.execute(
                "SELECT id, username, email, email_verified FROM app_auth_users WHERE username = ? LIMIT 1",
                (normalized_username,),
            ).fetchone()
        else:
            user = conn.execute(
                "SELECT id, username, email, email_verified FROM app_auth_users WHERE email = ? LIMIT 1",
                (normalized_email,),
            ).fetchone()
        if not user:
            raise RuntimeError("用户不存在，请先注册")
        if int((user[3] or 0)) == 1:
            raise RuntimeError("该账号邮箱已验证，无需重复发送")
        payload = _create_email_verification(conn, int(user[0]), str(user[2] or "").strip().lower())
        payload["username"] = str(user[1] or "")
        return payload
    finally:
        conn.close()


def _forgot_password(username_or_email: str = "") -> dict:
    key = (username_or_email or "").strip()
    if not key:
        raise ValueError("请输入账号或邮箱")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            """
            SELECT id, username, email, is_active
            FROM app_auth_users
            WHERE username = ? OR email = ?
            LIMIT 1
            """,
            (key, key.lower()),
        ).fetchone()
        if not row or int((row[3] or 0)) != 1:
            return {"sent": True}
        user_id = int(row[0])
        username = str(row[1] or "")
        code = f"{secrets.randbelow(1000000):06d}"
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(tzinfo=None)
        conn.execute(
            "UPDATE app_auth_password_resets SET used_at = CURRENT_TIMESTAMP WHERE user_id = ? AND used_at IS NULL",
            (user_id,),
        )
        conn.execute(
            """
            INSERT INTO app_auth_password_resets (user_id, username, reset_code, expires_at, used_at, created_at)
            VALUES (?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)
            """,
            (user_id, username, code, expires_at),
        )
        return {
            "sent": True,
            "username": username,
            "expires_minutes": 15,
            "dev_reset_code": code if os.getenv("AUTH_DEV_EXPOSE_VERIFY_CODE", "1").strip().lower() not in {"0", "false", "no"} else "",
        }
    finally:
        conn.close()


def _reset_password_with_code(username: str = "", reset_code: str = "", new_password: str = "") -> dict:
    uname = (username or "").strip()
    code = (reset_code or "").strip()
    pwd = (new_password or "").strip()
    if not uname or not code or not pwd:
        raise ValueError("缺少用户名、验证码或新密码")
    if len(pwd) < 6:
        raise ValueError("新密码至少6位")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        user = conn.execute("SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not user:
            raise RuntimeError("用户不存在")
        user_id = int(user[0])
        row = conn.execute(
            """
            SELECT id
            FROM app_auth_password_resets
            WHERE user_id = ?
              AND username = ?
              AND reset_code = ?
              AND used_at IS NULL
              AND expires_at > CURRENT_TIMESTAMP
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, uname, code),
        ).fetchone()
        if not row:
            raise RuntimeError("重置码无效或已过期")
        conn.execute("UPDATE app_auth_password_resets SET used_at = CURRENT_TIMESTAMP WHERE id = ?", (int(row[0]),))
        conn.execute(
            "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (_hash_password(pwd), user_id),
        )
        conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (user_id,))
        return {"ok": True, "username": uname}
    finally:
        conn.close()


def _consume_trend_daily_quota(user: dict | None) -> dict:
    if not user:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("trend_daily_limit"))
    if not limit:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"allowed": False, "limit": limit, "used": limit, "remaining": 0}
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT trend_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        if used >= limit:
            return {"allowed": False, "limit": limit, "used": used, "remaining": 0}
        if row:
            conn.execute(
                "UPDATE app_auth_usage_daily SET trend_count = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (used + 1, user_id, usage_date),
            )
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, created_at, updated_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (user_id, usage_date),
            )
        return {"allowed": True, "limit": limit, "used": used + 1, "remaining": max(limit - (used + 1), 0)}
    finally:
        conn.close()


def _get_trend_daily_quota_status(user: dict | None) -> dict:
    if not user:
        return {"limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("trend_daily_limit"))
    if not limit:
        return {"limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT trend_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        return {"limit": limit, "used": used, "remaining": max(limit - used, 0)}
    finally:
        conn.close()


def _consume_multi_role_daily_quota(user: dict | None) -> dict:
    if not user:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("multi_role_daily_limit"))
    if not limit:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"allowed": False, "limit": limit, "used": limit, "remaining": 0}
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        if used >= limit:
            return {"allowed": False, "limit": limit, "used": used, "remaining": 0}
        if row:
            conn.execute(
                "UPDATE app_auth_usage_daily SET multi_role_count = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (used + 1, user_id, usage_date),
            )
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, multi_role_count, created_at, updated_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (user_id, usage_date),
            )
        return {"allowed": True, "limit": limit, "used": used + 1, "remaining": max(limit - (used + 1), 0)}
    finally:
        conn.close()


def _get_multi_role_daily_quota_status(user: dict | None) -> dict:
    if not user:
        return {"limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("multi_role_daily_limit"))
    if not limit:
        return {"limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        return {"limit": limit, "used": used, "remaining": max(limit - used, 0)}
    finally:
        conn.close()


def _validate_auth_session(token: str) -> dict | None:
    normalized = (token or "").strip()
    if not normalized:
        return None
    token_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            """
            SELECT u.id, u.username, u.display_name, u.email, u.role, u.tier, u.email_verified
            FROM app_auth_sessions s
            JOIN app_auth_users u ON u.id = s.user_id
            WHERE s.session_token_hash = ?
              AND s.expires_at > CURRENT_TIMESTAMP
              AND u.is_active = 1
            ORDER BY s.id DESC
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE app_auth_sessions SET last_seen_at = CURRENT_TIMESTAMP WHERE session_token_hash = ?",
            (token_hash,),
        )
        return {
            "id": int(row[0]),
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[3] or ""),
            "role": str(row[4] or "limited").strip().lower(),
            "tier": str(row[5] or "limited").strip().lower(),
            "email_verified": int((row[6] or 0)) == 1,
        }
    finally:
        conn.close()


def _revoke_auth_session(token: str) -> int:
    normalized = (token or "").strip()
    if not normalized:
        return 0
    token_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE session_token_hash = ?", (token_hash,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _query_auth_users(keyword: str = "", role: str = "", active: str = "", page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    role_s = (role or "").strip().lower()
    active_s = (active or "").strip().lower()
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = []
    vals: list[object] = []
    if kw:
        where.append("(username LIKE ? OR display_name LIKE ? OR email LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    if role_s:
        where.append("LOWER(role) = ?")
        vals.append(role_s)
    if active_s in {"1", "true", "yes", "on"}:
        where.append("is_active = 1")
    elif active_s in {"0", "false", "no", "off"}:
        where.append("is_active = 0")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int((conn.execute(f"SELECT COUNT(*) FROM app_auth_users {where_sql}", tuple(vals)).fetchone()[0]) or 0)
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT id, username, display_name, email, role, tier, is_active, email_verified, failed_login_count, locked_until, created_at, updated_at, last_login_at
            FROM app_auth_users
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "id": int(r[0]),
            "username": str(r[1] or ""),
            "display_name": str(r[2] or ""),
            "email": str(r[3] or ""),
            "role": str(r[4] or "limited"),
            "tier": str(r[5] or "limited"),
            "is_active": int((r[6] or 0)) == 1,
            "email_verified": int((r[7] or 0)) == 1,
            "failed_login_count": int((r[8] or 0) or 0),
            "locked_until": r[9],
            "created_at": r[10],
            "updated_at": r[11],
            "last_login_at": r[12],
        }
        for r in rows
    ]
    usage_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usage_map: dict[int, dict[str, int]] = {}
    if items:
        user_ids = [int(it["id"]) for it in items]
        conn = sqlite3.connect(config.DB_PATH)
        try:
            _ensure_auth_tables(conn)
            placeholders = ",".join(["?"] * len(user_ids))
            usage_rows = conn.execute(
                f"""
                SELECT user_id, trend_count, multi_role_count
                FROM app_auth_usage_daily
                WHERE usage_date = ? AND user_id IN ({placeholders})
                """,
                tuple([usage_date_utc, *user_ids]),
            ).fetchall()
            usage_map = {
                int(r[0]): {
                    "trend": int((r[1] or 0) or 0),
                    "multi_role": int((r[2] or 0) or 0),
                }
                for r in usage_rows
            }
        finally:
            conn.close()
    policies = _effective_role_policies()
    for item in items:
        role_key = str(item.get("role") or item.get("tier") or "limited").strip().lower()
        usage = usage_map.get(int(item["id"]), {"trend": 0, "multi_role": 0})
        policy = policies.get(role_key) or {}
        trend_limit = _normalize_role_policy_limit(policy.get("trend_daily_limit"))
        trend_used = int(usage.get("trend", 0))
        multi_role_limit = _normalize_role_policy_limit(policy.get("multi_role_daily_limit"))
        multi_role_used = int(usage.get("multi_role", 0))
        item["trend_usage_date_utc"] = usage_date_utc
        item["trend_used_today"] = trend_used
        item["trend_limit"] = trend_limit
        item["trend_remaining_today"] = (None if trend_limit is None else max(int(trend_limit) - trend_used, 0))
        item["multi_role_used_today"] = multi_role_used
        item["multi_role_limit"] = multi_role_limit
        item["multi_role_remaining_today"] = (
            None if multi_role_limit is None else max(int(multi_role_limit) - multi_role_used, 0)
        )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


def _get_auth_role_policies() -> dict:
    policies = _effective_role_policies(force=True)
    roles = []
    for role in sorted(policies.keys()):
        payload = policies.get(role) or {}
        roles.append(
            {
                "role": role,
                "permissions": sorted(_normalize_role_policy_permissions(payload.get("permissions"))),
                "trend_daily_limit": _normalize_role_policy_limit(payload.get("trend_daily_limit")),
                "multi_role_daily_limit": _normalize_role_policy_limit(payload.get("multi_role_daily_limit")),
            }
        )
    return {"ok": True, "roles": roles, "effective_source": "db"}


def _update_auth_role_policy(role: str, permissions: list[str], trend_daily_limit: object, multi_role_daily_limit: object) -> dict:
    role_key = str(role or "").strip().lower()
    if role_key not in {"admin", "pro", "limited"}:
        raise ValueError("role 必须是 admin/pro/limited")
    normalized_perms = sorted(_normalize_role_policy_permissions(permissions))
    if role_key == "admin":
        normalized_perms = ["*"]
    elif "*" in normalized_perms:
        raise ValueError("仅 admin 角色允许使用 * 权限")
    trend_limit = _normalize_role_policy_limit(trend_daily_limit)
    multi_role_limit = _normalize_role_policy_limit(multi_role_daily_limit)
    perms_json = json.dumps(normalized_perms, ensure_ascii=False)
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_role_policies (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(role) DO UPDATE SET
              permissions_json = excluded.permissions_json,
              trend_daily_limit = excluded.trend_daily_limit,
              multi_role_daily_limit = excluded.multi_role_daily_limit,
              updated_at = CURRENT_TIMESTAMP
            """,
            (role_key, perms_json, trend_limit, multi_role_limit),
        )
    finally:
        conn.close()
    _invalidate_role_policies_cache()
    return {"ok": True, "role": role_key}


def _reset_auth_role_policies_to_default() -> dict:
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        for role, payload in config.DEFAULT_ROLE_POLICIES.items():
            perms_json = json.dumps(
                sorted(_normalize_role_policy_permissions(payload.get("permissions"))),
                ensure_ascii=False,
            )
            trend_limit = _normalize_role_policy_limit(payload.get("trend_daily_limit"))
            multi_role_limit = _normalize_role_policy_limit(payload.get("multi_role_daily_limit"))
            conn.execute(
                """
                INSERT INTO app_auth_role_policies (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(role) DO UPDATE SET
                  permissions_json = excluded.permissions_json,
                  trend_daily_limit = excluded.trend_daily_limit,
                  multi_role_daily_limit = excluded.multi_role_daily_limit,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (str(role), perms_json, trend_limit, multi_role_limit),
            )
    finally:
        conn.close()
    _invalidate_role_policies_cache()
    return _get_auth_role_policies()


def _update_auth_user(user_id: int | None = None, username: str = "", role: str | None = None, is_active: int | None = None, display_name: str | None = None) -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    updates = []
    vals: list[object] = []
    if role is not None:
        role_s = str(role or "").strip().lower()
        if role_s not in {"limited", "pro", "admin"}:
            raise ValueError("role 必须是 limited/pro/admin")
        updates.extend(["role = ?", "tier = ?"])
        vals.extend([role_s, role_s])
    if is_active is not None:
        updates.append("is_active = ?")
        vals.append(1 if int(is_active) else 0)
    if display_name is not None:
        updates.append("display_name = ?")
        vals.append(str(display_name or "").strip())
    if not updates:
        raise ValueError("未提供可更新字段")
    updates.append("updated_at = CURRENT_TIMESTAMP")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            vals.append(uid)
            conn.execute(f"UPDATE app_auth_users SET {', '.join(updates)} WHERE id = ?", tuple(vals))
            row = conn.execute("SELECT id, username, role, tier, is_active, display_name FROM app_auth_users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        else:
            vals.append(uname)
            conn.execute(f"UPDATE app_auth_users SET {', '.join(updates)} WHERE username = ?", tuple(vals))
            row = conn.execute("SELECT id, username, role, tier, is_active, display_name FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        _invalidate_auth_users_count_cache()
        return {
            "id": int(row[0]),
            "username": str(row[1] or ""),
            "role": str(row[2] or "limited").strip().lower(),
            "tier": str(row[3] or "limited").strip().lower(),
            "is_active": int((row[4] or 0)) == 1,
            "display_name": str(row[5] or ""),
        }
    finally:
        conn.close()


def _admin_reset_user_password(user_id: int | None = None, username: str = "", new_password: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    pwd = (new_password or "").strip()
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    if len(pwd) < 6:
        raise ValueError("新密码至少6位")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            conn.execute(
                "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (_hash_password(pwd), uid),
            )
            row = conn.execute("SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        else:
            conn.execute(
                "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
                (_hash_password(pwd), uname),
            )
            row = conn.execute("SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (int(row[0]),))
        return {"id": int(row[0]), "username": str(row[1] or ""), "session_revoked": True}
    finally:
        conn.close()


def _admin_reset_user_trend_quota(user_id: int | None = None, username: str = "", usage_date: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1",
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1",
                (uname,),
            ).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        target_user_id = int(row[0])
        row_usage = conn.execute(
            "SELECT trend_count, multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (target_user_id, usage_date_s),
        ).fetchone()
        if row_usage:
            conn.execute(
                "UPDATE app_auth_usage_daily SET trend_count = 0, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (target_user_id, usage_date_s),
            )
            old_trend = int((row_usage[0] or 0) or 0)
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, multi_role_count, created_at, updated_at) VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (target_user_id, usage_date_s),
            )
            old_trend = 0
        return {
            "id": target_user_id,
            "username": str(row[1] or ""),
            "usage_date": usage_date_s,
            "previous_trend_count": old_trend,
            "trend_count": 0,
        }
    finally:
        conn.close()


def _admin_reset_user_multi_role_quota(user_id: int | None = None, username: str = "", usage_date: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1",
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1",
                (uname,),
            ).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        target_user_id = int(row[0])
        row_usage = conn.execute(
            "SELECT trend_count, multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (target_user_id, usage_date_s),
        ).fetchone()
        if row_usage:
            conn.execute(
                "UPDATE app_auth_usage_daily SET multi_role_count = 0, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (target_user_id, usage_date_s),
            )
            old_multi = int((row_usage[1] or 0) or 0)
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, multi_role_count, created_at, updated_at) VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (target_user_id, usage_date_s),
            )
            old_multi = 0
        return {
            "id": target_user_id,
            "username": str(row[1] or ""),
            "usage_date": usage_date_s,
            "previous_multi_role_count": old_multi,
            "multi_role_count": 0,
        }
    finally:
        conn.close()


def _admin_reset_quota_batch(usage_date: str = "", role: str = "", usernames: list[str] | None = None) -> dict:
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    role_s = (role or "").strip().lower()
    name_list = [str(x or "").strip() for x in (usernames or []) if str(x or "").strip()]
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        where = ["is_active = 1"]
        vals: list[object] = []
        if role_s:
            where.append("LOWER(role) = ?")
            vals.append(role_s)
        if name_list:
            placeholders = ",".join(["?"] * len(name_list))
            where.append(f"username IN ({placeholders})")
            vals.extend(name_list)
        rows = conn.execute(
            f"SELECT id, username, role FROM app_auth_users WHERE {' AND '.join(where)} ORDER BY id ASC",
            tuple(vals),
        ).fetchall()
        if not rows:
            return {
                "usage_date": usage_date_s,
                "matched_users": 0,
                "affected_rows": 0,
                "items": [],
            }
        user_ids = [int(r[0]) for r in rows]
        placeholders = ",".join(["?"] * len(user_ids))
        args = tuple([usage_date_s, *user_ids])
        cur = conn.execute(
            f"""
            UPDATE app_auth_usage_daily
            SET trend_count = 0,
                multi_role_count = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE usage_date = ? AND user_id IN ({placeholders})
            """,
            args,
        )
        affected_rows = int(getattr(cur, "rowcount", 0) or 0)
        items = [
            {"id": int(r[0]), "username": str(r[1] or ""), "role": str(r[2] or "")}
            for r in rows
        ]
        return {
            "usage_date": usage_date_s,
            "matched_users": len(items),
            "affected_rows": affected_rows,
            "items": items,
        }
    finally:
        conn.close()


def _query_auth_sessions(keyword: str = "", user_id: int | None = None, page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    uid = int(user_id or 0)
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = ["s.expires_at > CURRENT_TIMESTAMP"]
    vals: list[object] = []
    if uid > 0:
        where.append("s.user_id = ?")
        vals.append(uid)
    if kw:
        where.append("(u.username LIKE ? OR u.display_name LIKE ? OR u.email LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    where_sql = f"WHERE {' AND '.join(where)}"
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int(
            (
                conn.execute(
                    f"SELECT COUNT(*) FROM app_auth_sessions s JOIN app_auth_users u ON u.id = s.user_id {where_sql}",
                    tuple(vals),
                ).fetchone()[0]
            )
            or 0
        )
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT s.id, s.user_id, u.username, u.display_name, s.expires_at, s.created_at, s.last_seen_at, s.session_token_hash
            FROM app_auth_sessions s
            JOIN app_auth_users u ON u.id = s.user_id
            {where_sql}
            ORDER BY s.last_seen_at DESC, s.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "session_id": int(r[0]),
            "user_id": int(r[1]),
            "username": str(r[2] or ""),
            "display_name": str(r[3] or ""),
            "expires_at": r[4],
            "created_at": r[5],
            "last_seen_at": r[6],
            "token_hash_preview": f"{str(r[7] or '')[:12]}...",
        }
        for r in rows
    ]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


def _revoke_auth_session_by_id(session_id: int) -> int:
    sid = int(session_id or 0)
    if sid <= 0:
        return 0
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE id = ?", (sid,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _revoke_auth_sessions_by_user(user_id: int) -> int:
    uid = int(user_id or 0)
    if uid <= 0:
        return 0
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (uid,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _query_auth_audit_logs(keyword: str = "", event_type: str = "", result: str = "", page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    evt = (event_type or "").strip()
    res = (result or "").strip()
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = []
    vals: list[object] = []
    if kw:
        where.append("(username LIKE ? OR detail LIKE ? OR ip LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    if evt:
        where.append("event_type = ?")
        vals.append(evt)
    if res:
        where.append("result = ?")
        vals.append(res)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = sqlite3.connect(config.DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int((conn.execute(f"SELECT COUNT(*) FROM app_auth_audit_logs {where_sql}", tuple(vals)).fetchone()[0]) or 0)
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT id, event_type, username, user_id, result, detail, ip, user_agent, created_at
            FROM app_auth_audit_logs
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "id": int(r[0]),
            "event_type": str(r[1] or ""),
            "username": str(r[2] or ""),
            "user_id": int((r[3] or 0) or 0),
            "result": str(r[4] or ""),
            "detail": str(r[5] or ""),
            "ip": str(r[6] or ""),
            "user_agent": str(r[7] or ""),
            "created_at": r[8],
        }
        for r in rows
    ]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }

__all__ = ['_effective_role_policies', '_invalidate_role_policies_cache', '_normalize_role_policy_permissions', '_normalize_role_policy_limit', '_ensure_auth_tables', '_hash_password', '_verify_password', '_invalidate_auth_users_count_cache', '_active_auth_users_count', '_new_session_token', '_record_auth_audit', '_user_locked', '_register_login_failure', '_clear_login_failure', '_generate_invite_code', '_create_invite_code', '_assert_invite_valid', '_reserve_invite_use', '_create_email_verification', '_register_auth_user', '_login_auth_user', '_verify_email_code', '_resend_email_verification', '_forgot_password', '_reset_password_with_code', '_consume_trend_daily_quota', '_get_trend_daily_quota_status', '_consume_multi_role_daily_quota', '_get_multi_role_daily_quota_status', '_validate_auth_session', '_revoke_auth_session', '_query_auth_users', '_get_auth_role_policies', '_update_auth_role_policy', '_reset_auth_role_policies_to_default', '_update_auth_user', '_admin_reset_user_password', '_admin_reset_user_trend_quota', '_admin_reset_user_multi_role_quota', '_admin_reset_quota_batch', '_query_auth_sessions', '_revoke_auth_session_by_id', '_revoke_auth_sessions_by_user', '_query_auth_audit_logs']
