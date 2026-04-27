from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import parse_qs

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
METRICS_DAILY_DIR = ROOT_DIR / "docs" / "metrics" / "daily"
_METRICS_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.json$")


def _guard_governance_write(deps: dict, *, scope: str) -> str | None:
    guard = deps.get("assert_write_allowed")
    if not callable(guard):
        return None
    try:
        guard(scope=scope, layer="layer4_backoffice_governance")
        return None
    except Exception as exc:
        return str(exc)


def _safe_rate_value(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _load_daily_metrics_items() -> list[dict]:
    if not METRICS_DAILY_DIR.exists():
        return []
    items: list[dict] = []
    for file_path in sorted(METRICS_DAILY_DIR.glob("*.json")):
        m = _METRICS_DATE_RE.match(file_path.name)
        if not m:
            continue
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        items.append({
            "date": str(payload.get("date") or m.group(1)),
            "pipeline_success_rate": _safe_rate_value(payload.get("pipeline_success_rate")),
            "traceability_rate": _safe_rate_value(payload.get("traceability_rate")),
            "closure_rate": _safe_rate_value(payload.get("closure_rate")),
            "sample_insufficient": bool(payload.get("sample_insufficient")),
        })
    items.sort(key=lambda x: str(x.get("date") or ""))
    return items


def _build_metrics_summary_payload() -> dict:
    items = _load_daily_metrics_items()
    trend_7d = items[-7:]
    latest = trend_7d[-1] if trend_7d else None
    return {"latest": latest, "trend_7d": trend_7d}


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if parsed.path == "/api/system/data-readiness/run":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可运行数据就绪 Agent"}, status=403)
            return True
        error = _guard_governance_write(deps, scope="data_readiness.run")
        if error:
            handler._send_json({"ok": False, "error": error}, status=403)
            return True
        try:
            result = deps["run_data_readiness_agent"](
                auto_fix=bool(payload.get("auto_fix", True)),
                dry_run=bool(payload.get("dry_run", False)),
                ai_enabled=bool(payload.get("ai_enabled", True)),
                path_selection_enabled=bool(payload.get("path_selection_enabled", True)),
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"数据就绪 Agent 运行失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {
        "/api/system/llm-providers/multi-role-v2-policies",
        "/api/llm-providers/multi-role-v2-policies",
    }:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新多角色 V2 模型策略"}, status=403)
            return True
        try:
            result = deps["update_multi_role_v2_policies"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"更新多角色 V2 策略失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/create", "/api/llm-providers/create"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可新增 LLM 节点"}, status=403)
            return True
        try:
            result = deps["create_llm_provider"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"新增失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/update", "/api/llm-providers/update"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新 LLM 节点"}, status=403)
            return True
        try:
            result = deps["update_llm_provider"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"更新失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/delete", "/api/llm-providers/delete"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可删除 LLM 节点"}, status=403)
            return True
        try:
            result = deps["delete_llm_provider"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"删除失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/test-one", "/api/llm-providers/test-one"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可测试 LLM 节点"}, status=403)
            return True
        try:
            result = deps["test_one_llm_provider"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"测试失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/test-model", "/api/llm-providers/test-model"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可一键测试模型节点"}, status=403)
            return True
        try:
            result = deps["test_model_llm_providers"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"一键测试失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path in {"/api/system/llm-providers/default-rate-limit", "/api/llm-providers/default-rate-limit"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新全局限速"}, status=403)
            return True
        try:
            result = deps["update_default_rate_limit"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"更新全局限速失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/auth/invite/create":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可创建邀请码"}, status=403)
            return True
        max_uses = payload.get("max_uses", 1)
        expires_at = str(payload.get("expires_at", "") or "").strip()
        explicit_code = str(payload.get("invite_code", "") or "").strip()
        creator = ((auth_ctx.get("user") or {}).get("username") or "admin").strip()
        try:
            invite = deps["create_invite_code"](max_uses=max_uses, expires_at=expires_at, created_by=creator, explicit_code=explicit_code)
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"邀请码创建失败: {exc}"}, status=400)
            return True
        handler._send_json({"ok": True, **invite})
        return True

    if parsed.path == "/api/auth/invite/update":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新邀请码"}, status=403)
            return True
        invite_code = str(payload.get("invite_code", "") or "").strip().upper()
        if not invite_code:
            handler._send_json({"ok": False, "error": "invite_code 不能为空"}, status=400)
            return True
        max_uses = payload.get("max_uses", None)
        is_active = payload.get("is_active", None)
        expires_at = payload.get("expires_at", None)
        conn = deps["sqlite3"].connect(deps["DB_PATH"])
        try:
            deps["ensure_auth_tables"](conn)
            row = conn.execute(
                "SELECT invite_code FROM app_auth_invites WHERE invite_code = ? LIMIT 1",
                (invite_code,),
            ).fetchone()
            if not row:
                handler._send_json({"ok": False, "error": "邀请码不存在"}, status=404)
                return True
            updates = []
            values = []
            if max_uses is not None:
                updates.append("max_uses = ?")
                values.append(max(1, int(max_uses)))
            if is_active is not None:
                active_flag = 1 if str(is_active).strip().lower() in {"1", "true", "yes", "y", "on"} else 0
                updates.append("is_active = ?")
                values.append(active_flag)
            if expires_at is not None:
                expires_s = str(expires_at or "").strip() or None
                updates.append("expires_at = ?")
                values.append(expires_s)
            if not updates:
                handler._send_json({"ok": False, "error": "未提供可更新字段"}, status=400)
                return True
            updates.append("updated_at = CURRENT_TIMESTAMP")
            values.append(invite_code)
            conn.execute(
                f"UPDATE app_auth_invites SET {', '.join(updates)} WHERE invite_code = ?",
                tuple(values),
            )
            item = conn.execute(
                """
                SELECT invite_code, max_uses, used_count, is_active, expires_at, created_by, created_at, updated_at
                FROM app_auth_invites
                WHERE invite_code = ?
                LIMIT 1
                """,
                (invite_code,),
            ).fetchone()
        finally:
            conn.close()
        max_uses_v = int((item[1] or 0) or 0)
        used_v = int((item[2] or 0) or 0)
        remaining = None if max_uses_v <= 0 else max(max_uses_v - used_v, 0)
        handler._send_json(
            {
                "ok": True,
                "item": {
                    "invite_code": str(item[0] or ""),
                    "max_uses": max_uses_v,
                    "used_count": used_v,
                    "remaining_uses": remaining,
                    "is_active": int((item[3] or 0)) == 1,
                    "expires_at": item[4],
                    "created_by": str(item[5] or ""),
                    "created_at": item[6],
                    "updated_at": item[7],
                },
            }
        )
        return True

    if parsed.path == "/api/auth/invite/delete":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可删除邀请码"}, status=403)
            return True
        invite_code = str(payload.get("invite_code", "") or "").strip().upper()
        if not invite_code:
            handler._send_json({"ok": False, "error": "invite_code 不能为空"}, status=400)
            return True
        conn = deps["sqlite3"].connect(deps["DB_PATH"])
        try:
            deps["ensure_auth_tables"](conn)
            cur = conn.execute("DELETE FROM app_auth_invites WHERE invite_code = ?", (invite_code,))
            deleted = int(getattr(cur, "rowcount", 0) or 0)
        finally:
            conn.close()
        handler._send_json({"ok": True, "deleted": deleted})
        return True

    if parsed.path == "/api/auth/register":
        username = str(payload.get("username", "") or "").strip()
        password = str(payload.get("password", "") or "").strip()
        display_name = str(payload.get("display_name", "") or "").strip()
        email = str(payload.get("email", "") or "").strip()
        invite_code = str(payload.get("invite_code", "") or "").strip()
        try:
            token, user, verify_payload = deps["register_auth_user"](username, password, display_name, email, invite_code)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=409)
            return True
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"注册失败: {exc}"}, status=500)
            return True
        deps["record_auth_audit"](
            event_type="register",
            username=user.get("username", ""),
            user_id=int(user.get("id") or 0),
            result="ok",
            detail="invite register",
            ip=(handler.client_address[0] if getattr(handler, "client_address", None) else ""),
            user_agent=str(handler.headers.get("User-Agent", "") or ""),
        )
        handler._send_json(
            {
                "ok": True,
                "auth_required": bool(deps["admin_token_required"]()),
                "token_valid": True,
                "token": token,
                "user": user,
                "mode": "account",
                "requires_email_verification": False,
            }
        )
        return True

    if parsed.path == "/api/auth/verify-email":
        username = str(payload.get("username", "") or "").strip()
        email = str(payload.get("email", "") or "").strip()
        verify_code = str(payload.get("verify_code", "") or "").strip()
        try:
            result = deps["verify_email_code"](username, email, verify_code)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=401)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"邮箱验证失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, "verified": True, "user": result})
        return True

    if parsed.path == "/api/auth/send-verify-code":
        username = str(payload.get("username", "") or "").strip()
        email = str(payload.get("email", "") or "").strip()
        try:
            result = deps["resend_email_verification"](username, email)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=409)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"发送验证码失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, "verify": result})
        return True

    if parsed.path == "/api/auth/forgot-password":
        key = str(payload.get("username_or_email", "") or "").strip()
        try:
            result = deps["forgot_password"](key)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"发送重置码失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/reset-password":
        username = str(payload.get("username", "") or "").strip()
        reset_code = str(payload.get("reset_code", "") or "").strip()
        new_password = str(payload.get("new_password", "") or "").strip()
        try:
            result = deps["reset_password_with_code"](username, reset_code, new_password)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=401)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"密码重置失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/login":
        username = str(payload.get("username", "") or "").strip()
        password = str(payload.get("password", "") or "").strip()
        if username or password:
            try:
                token, user = deps["login_auth_user"](username, password)
            except ValueError as exc:
                handler._send_json({"ok": False, "error": str(exc)}, status=400)
                return True
            except RuntimeError as exc:
                deps["record_auth_audit"](
                    event_type="login",
                    username=username,
                    result="fail",
                    detail=str(exc),
                    ip=(handler.client_address[0] if getattr(handler, "client_address", None) else ""),
                    user_agent=str(handler.headers.get("User-Agent", "") or ""),
                )
                handler._send_json({"ok": False, "error": str(exc)}, status=401)
                return True
            except Exception as exc:  # pragma: no cover
                handler._send_json({"ok": False, "error": f"账号登录失败: {exc}"}, status=500)
                return True
            deps["record_auth_audit"](
                event_type="login",
                username=user.get("username", ""),
                user_id=int(user.get("id") or 0),
                result="ok",
                detail="account login",
                ip=(handler.client_address[0] if getattr(handler, "client_address", None) else ""),
                user_agent=str(handler.headers.get("User-Agent", "") or ""),
            )
            handler._send_json(
                {
                    "ok": True,
                    "auth_required": bool(deps["admin_token_required"]()),
                    "token_valid": True,
                    "token": token,
                    "user": user,
                    "mode": "account",
                }
            )
            return True

        token = str(payload.get("token", "") or "").strip()
        required = bool(deps["admin_token_required"]())
        valid = bool(deps["token_valid"](token))
        if required and not valid:
            handler._send_json({"ok": False, "error": "管理令牌无效", "auth_required": True, "token_valid": False}, status=401)
            return True
        user = deps["validate_auth_session"](token) if token else None
        handler._send_json({"ok": True, "auth_required": required, "token_valid": valid, "user": user, "mode": "token"})
        return True

    if parsed.path == "/api/auth/logout":
        token = str(deps["extract_admin_token"]() or "").strip()
        revoked = int(deps["revoke_auth_session"](token) if token else 0)
        auth_ctx = deps.get("auth_context") or {}
        user = auth_ctx.get("user") or {}
        deps["record_auth_audit"](
            event_type="logout",
            username=str(user.get("username") or ""),
            user_id=int(user.get("id") or 0) if user else None,
            result="ok",
            detail=f"revoked={revoked}",
            ip=(handler.client_address[0] if getattr(handler, "client_address", None) else ""),
            user_agent=str(handler.headers.get("User-Agent", "") or ""),
        )
        handler._send_json({"ok": True, "revoked": revoked})
        return True

    if parsed.path == "/api/auth/user/update":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新用户"}, status=403)
            return True
        try:
            user = deps["update_auth_user"](
                user_id=payload.get("user_id"),
                username=str(payload.get("username", "") or "").strip(),
                role=payload.get("role"),
                is_active=payload.get("is_active") if "is_active" in payload else None,
                display_name=payload.get("display_name") if "display_name" in payload else None,
            )
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=404)
            return True
        handler._send_json({"ok": True, "user": user})
        return True

    if parsed.path == "/api/auth/user/reset-password":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可重置密码"}, status=403)
            return True
        new_password = str(payload.get("new_password", "") or "").strip()
        try:
            result = deps["admin_reset_user_password"](
                user_id=payload.get("user_id"),
                username=str(payload.get("username", "") or "").strip(),
                new_password=new_password,
            )
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=404)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/user/reset-trend-quota":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可重置走势次数"}, status=403)
            return True
        try:
            result = deps["admin_reset_user_trend_quota"](
                user_id=payload.get("user_id"),
                username=str(payload.get("username", "") or "").strip(),
                usage_date=str(payload.get("usage_date", "") or "").strip(),
            )
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=404)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/user/reset-multi-role-quota":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可重置多角色次数"}, status=403)
            return True
        try:
            result = deps["admin_reset_user_multi_role_quota"](
                user_id=payload.get("user_id"),
                username=str(payload.get("username", "") or "").strip(),
                usage_date=str(payload.get("usage_date", "") or "").strip(),
            )
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except RuntimeError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=404)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/quota/reset-batch":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可批量重置额度"}, status=403)
            return True
        usage_date = str(payload.get("usage_date", "") or "").strip()
        role = str(payload.get("role", "") or "").strip()
        usernames = payload.get("usernames", [])
        if isinstance(usernames, str):
            usernames = [x.strip() for x in usernames.split(",") if x.strip()]
        if not isinstance(usernames, list):
            handler._send_json({"ok": False, "error": "usernames 必须是数组或逗号分隔字符串"}, status=400)
            return True
        try:
            result = deps["admin_reset_quota_batch"](usage_date=usage_date, role=role, usernames=usernames)
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"批量重置失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/auth/role-policies/update":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可更新角色权限策略"}, status=403)
            return True
        try:
            result = deps["update_auth_role_policy"](
                role=str(payload.get("role", "") or "").strip(),
                permissions=payload.get("permissions") or [],
                trend_daily_limit=payload.get("trend_daily_limit"),
                multi_role_daily_limit=payload.get("multi_role_daily_limit"),
            )
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"更新角色策略失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/auth/role-policies/reset-default":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可恢复默认角色策略"}, status=403)
            return True
        try:
            result = deps["reset_auth_role_policies_to_default"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"恢复默认策略失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/auth/session/revoke":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可强制下线会话"}, status=403)
            return True
        session_id = int(payload.get("session_id", 0) or 0)
        revoked = deps["revoke_auth_session_by_id"](session_id)
        handler._send_json({"ok": True, "revoked": revoked})
        return True

    if parsed.path == "/api/auth/user/revoke-sessions":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可强制下线用户"}, status=403)
            return True
        user_id = int(payload.get("user_id", 0) or 0)
        revoked = deps["revoke_auth_sessions_by_user"](user_id)
        handler._send_json({"ok": True, "revoked": revoked})
        return True

    if parsed.path == "/api/signal-quality/rules/save":
        items = payload.get("items", [])
        if not isinstance(items, list):
            handler._send_json({"error": "items 必须是数组"}, status=400)
            return True
        try:
            result = deps["save_signal_quality_rules"](items)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"规则保存失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/signal-quality/blocklist/save":
        items = payload.get("items", [])
        if not isinstance(items, list):
            handler._send_json({"error": "items 必须是数组"}, status=400)
            return True
        try:
            result = deps["save_signal_mapping_blocklist"](items)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"黑名单保存失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    return False


def dispatch_get(handler, parsed, host: str, deps: dict) -> bool:
    if parsed.path in {
        "/api/system/llm-providers/multi-role-v2-policies",
        "/api/llm-providers/multi-role-v2-policies",
    }:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看多角色 V2 模型策略"}, status=403)
            return True
        try:
            payload = deps["get_multi_role_v2_policies"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"多角色 V2 策略查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path in {"/api/system/llm-providers", "/api/llm-providers"}:
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看 LLM 节点"}, status=403)
            return True
        try:
            payload = deps["list_llm_providers"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"LLM 节点查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api":
        handler._send_json(
            {
                "service": "stock-codes-api",
                "message": "这是后端 API 服务，不是前端页面。",
                "frontend_url": str(deps.get("frontend_url") or f"http://{host}:{deps['build_info']().get('port')}/"),
                "endpoints": deps["api_endpoints_catalog"],
            }
        )
        return True

    if parsed.path == "/":
        if bool(deps.get("frontend_dist_exists")):
            return False
        handler._send_json(
            {
                "service": "stock-codes-api",
                "message": "这是后端 API 服务，不是前端页面。",
                "frontend_url": str(deps.get("frontend_url") or f"http://{host}:{deps['build_info']().get('port')}/"),
                "endpoints": deps["api_endpoints_catalog"],
            }
        )
        return True

    if parsed.path == "/api/health":
        handler._send_json({"ok": True, "db": deps["db_label"](), **deps["build_info"]()})
        return True

    if parsed.path == "/api/system/data-readiness":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看数据就绪报告"}, status=403)
            return True
        try:
            result = deps["query_latest_data_readiness_report"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"数据就绪报告查询失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/auth/status":
        auth_ctx = deps.get("auth_context") or {}
        required = bool(deps["admin_token_required"]())
        token = str(deps["extract_admin_token"]() or "").strip()
        token_valid = bool(deps["token_valid"](token))
        user = auth_ctx.get("user")
        if user is None and token:
            user = deps["validate_auth_session"](token)
        has_user_accounts = int(deps["active_auth_users_count"]()) > 0
        trend_quota = deps["get_trend_daily_quota_status"](user)
        multi_role_quota = deps["get_multi_role_daily_quota_status"](user)
        permission_matrix = deps["permission_matrix"]()
        effective_permissions = deps["effective_permissions_for_user"](user)
        dynamic_rbac = deps["get_dynamic_rbac_payload"]()
        handler._send_json(
            {
                "ok": True,
                "auth_required": required,
                "token_present": bool(token),
                "token_valid": token_valid,
                "has_user_accounts": has_user_accounts,
                "user": user,
                "trend_quota": trend_quota,
                "multi_role_quota": multi_role_quota,
                "permission_matrix": permission_matrix,
                "effective_permissions": effective_permissions,
                "rbac_dynamic_enforced": bool(deps.get("rbac_dynamic_enforced")),
                "rbac_dynamic_version": str(dynamic_rbac.get("version") or "unknown"),
                "rbac_dynamic_source": str(dynamic_rbac.get("source") or "unknown"),
                "rbac_schema_version": str(dynamic_rbac.get("schema_version") or ""),
                "dynamic_rbac": dynamic_rbac,
            }
        )
        return True

    if parsed.path == "/api/auth/permissions":
        auth_ctx = deps.get("auth_context") or {}
        user = auth_ctx.get("user")
        dynamic_rbac = deps["get_dynamic_rbac_payload"]()
        handler._send_json(
            {
                "ok": True,
                "permission_matrix": deps["permission_matrix"](),
                "effective_permissions": deps["effective_permissions_for_user"](user),
                "role": str((user or {}).get("role") or (user or {}).get("tier") or ""),
                "permission_catalog": dynamic_rbac.get("permission_catalog") or [],
                "route_permissions": dynamic_rbac.get("route_permissions") or {},
                "navigation_groups": dynamic_rbac.get("navigation_groups") or [],
                "schema_version": dynamic_rbac.get("schema_version") or "",
                "version": dynamic_rbac.get("version") or "unknown",
                "source": dynamic_rbac.get("source") or "unknown",
                "rbac_dynamic_enforced": bool(deps.get("rbac_dynamic_enforced")),
                "validation": dynamic_rbac.get("validation") or {},
            }
        )
        return True

    if parsed.path == "/api/navigation-groups":
        try:
            payload = deps["get_navigation_groups"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"导航分组查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/auth/role-policies":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看角色策略"}, status=403)
            return True
        try:
            payload = deps["get_auth_role_policies"]()
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"角色策略查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/auth/users/summary":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看账号汇总"}, status=403)
            return True
        conn = deps["sqlite3"].connect(deps["DB_PATH"])
        try:
            deps["ensure_auth_tables"](conn)
            total = int((conn.execute("SELECT COUNT(*) FROM app_auth_users").fetchone()[0]) or 0)
            active = int((conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE is_active = 1").fetchone()[0]) or 0)
            verified = int((conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE email_verified = 1").fetchone()[0]) or 0)
            limited = int((conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE role = 'limited'").fetchone()[0]) or 0)
            pro = int((conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE role = 'pro'").fetchone()[0]) or 0)
            admin = int((conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE role = 'admin'").fetchone()[0]) or 0)
        finally:
            conn.close()
        handler._send_json(
            {
                "ok": True,
                "summary": {
                    "total_users": total,
                    "active_users": active,
                    "verified_users": verified,
                    "limited_users": limited,
                    "pro_users": pro,
                    "admin_users": admin,
                },
            }
        )
        return True

    if parsed.path == "/api/auth/users":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看用户列表"}, status=403)
            return True
        params = parse_qs(parsed.query)
        try:
            payload = deps["query_auth_users"](
                keyword=params.get("keyword", [""])[0],
                role=params.get("role", [""])[0],
                active=params.get("active", [""])[0],
                page=int(params.get("page", ["1"])[0]),
                page_size=int(params.get("page_size", ["20"])[0]),
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"用户列表查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/auth/sessions":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看会话"}, status=403)
            return True
        params = parse_qs(parsed.query)
        try:
            payload = deps["query_auth_sessions"](
                keyword=params.get("keyword", [""])[0],
                user_id=int(params.get("user_id", ["0"])[0] or 0),
                page=int(params.get("page", ["1"])[0]),
                page_size=int(params.get("page_size", ["20"])[0]),
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"会话查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/auth/audit-logs":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看审计日志"}, status=403)
            return True
        params = parse_qs(parsed.query)
        try:
            payload = deps["query_auth_audit_logs"](
                keyword=params.get("keyword", [""])[0],
                event_type=params.get("event_type", [""])[0],
                result=params.get("result", [""])[0],
                page=int(params.get("page", ["1"])[0]),
                page_size=int(params.get("page_size", ["20"])[0]),
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"审计日志查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/auth/invites":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可查看邀请码"}, status=403)
            return True
        params = parse_qs(parsed.query)
        keyword = str(params.get("keyword", [""])[0] or "").strip().upper()
        active = str(params.get("active", [""])[0] or "").strip().lower()
        try:
            page = max(int(params.get("page", ["1"])[0]), 1)
            page_size = max(min(int(params.get("page_size", ["20"])[0]), 200), 1)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        where = []
        values = []
        if keyword:
            where.append("invite_code LIKE ?")
            values.append(f"%{keyword}%")
        if active in {"1", "true", "yes", "on"}:
            where.append("is_active = 1")
        elif active in {"0", "false", "no", "off"}:
            where.append("is_active = 0")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        conn = deps["sqlite3"].connect(deps["DB_PATH"])
        try:
            deps["ensure_auth_tables"](conn)
            total_row = conn.execute(f"SELECT COUNT(*) FROM app_auth_invites {where_sql}", tuple(values)).fetchone()
            total = int((total_row[0] if total_row else 0) or 0)
            offset = (page - 1) * page_size
            rows = conn.execute(
                f"""
                SELECT invite_code, max_uses, used_count, is_active, expires_at, created_by, created_at, updated_at
                FROM app_auth_invites
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                tuple([*values, page_size, offset]),
            ).fetchall()
        finally:
            conn.close()
        items = []
        for row in rows:
            max_uses_v = int((row[1] or 0) or 0)
            used_v = int((row[2] or 0) or 0)
            remaining = None if max_uses_v <= 0 else max(max_uses_v - used_v, 0)
            items.append(
                {
                    "invite_code": str(row[0] or ""),
                    "max_uses": max_uses_v,
                    "used_count": used_v,
                    "remaining_uses": remaining,
                    "is_active": int((row[3] or 0)) == 1,
                    "expires_at": row[4],
                    "created_by": str(row[5] or ""),
                    "created_at": row[6],
                    "updated_at": row[7],
                }
            )
        handler._send_json(
            {
                "ok": True,
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size if page_size else 0,
            }
        )
        return True

    if parsed.path == "/api/jobs":
        try:
            payload = deps["query_job_definitions"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"任务定义查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/job-runs":
        params = parse_qs(parsed.query)
        job_key = params.get("job_key", [""])[0]
        status = params.get("status", [""])[0]
        try:
            limit = int(params.get("limit", ["50"])[0])
        except ValueError:
            handler._send_json({"error": "limit 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_job_runs"](job_key=job_key, status=status, limit=limit)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"任务运行记录查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/job-alerts":
        params = parse_qs(parsed.query)
        job_key = params.get("job_key", [""])[0]
        unresolved_raw = params.get("unresolved_only", ["1"])[0].strip().lower()
        unresolved_only = unresolved_raw not in {"0", "false", "no", "off"}
        try:
            limit = int(params.get("limit", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "limit 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_job_alerts"](job_key=job_key, unresolved_only=unresolved_only, limit=limit)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"任务告警查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/jobs/trigger":
        denied = _guard_governance_write(deps, scope="jobs.trigger")
        if denied:
            handler._send_json({"error": denied}, status=403)
            return True
        params = parse_qs(parsed.query)
        job_key = params.get("job_key", [""])[0].strip()
        if not job_key:
            handler._send_json({"error": "缺少 job_key"}, status=400)
            return True
        try:
            payload = deps["run_job"](job_key, trigger_mode="api")
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"任务触发失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/jobs/dry-run":
        denied = _guard_governance_write(deps, scope="jobs.dry_run")
        if denied:
            handler._send_json({"error": denied}, status=403)
            return True
        params = parse_qs(parsed.query)
        job_key = params.get("job_key", [""])[0].strip()
        if not job_key:
            handler._send_json({"error": "缺少 job_key"}, status=400)
            return True
        try:
            payload = deps["dry_run_job"](job_key, trigger_mode="api_dry_run")
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"任务 dry-run 失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/dashboard":
        try:
            payload = deps["query_dashboard"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"工作台查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/source-monitor":
        try:
            payload = deps["query_source_monitor"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"监控查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/database-audit":
        params = parse_qs(parsed.query)
        refresh_raw = params.get("refresh", ["0"])[0].strip().lower()
        refresh = refresh_raw in {"1", "true", "yes", "y", "on"}
        try:
            payload = deps["query_database_audit"](refresh=refresh)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"审核报告查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/db-health":
        try:
            payload = deps["query_database_health"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"数据库健康查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/signal-audit":
        params = parse_qs(parsed.query)
        scope = params.get("scope", ["7d"])[0]
        try:
            payload = deps["query_signal_audit"](scope=scope)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"信号审计查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/signal-quality/config":
        try:
            payload = deps["query_signal_quality_config"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"信号质量配置查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/metrics/summary":
        try:
            payload = _build_metrics_summary_payload()
        except Exception as exc:
            handler._send_json({"error": f"指标汇总查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/system/stale-tasks":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json(
                {
                    "error": "无权限访问此功能",
                    "code": "PERMISSION_DENIED",
                    "action": "contact_admin",
                },
                status=403,
            )
            return True
        _handle_stale_tasks(handler)
        return True

    return False


def _handle_stale_tasks(handler) -> None:
    """GET /api/system/stale-tasks - List stale pending multi-role jobs (>30min running)."""
    import datetime
    from db_compat import get_db_connection

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cutoff = (
            datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
        ).isoformat()
        try:
            cur.execute(
                """
                SELECT id, ts_code, status, created_at, updated_at
                FROM multi_role_v3_jobs
                WHERE status IN ('queued', 'running')
                AND updated_at < %s
                ORDER BY created_at DESC
                LIMIT 20
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
            stale = [
                {
                    "id": r[0],
                    "ts_code": r[1],
                    "status": r[2],
                    "created_at": str(r[3]),
                    "updated_at": str(r[4]),
                }
                for r in rows
            ]
        except Exception:
            stale = []
        cur.close()
        conn.close()
        handler._send_json({"stale_tasks": stale, "count": len(stale)})
    except Exception as exc:
        handler._send_json(
            {
                "error": "数据库暂时不可用，请稍后重试",
                "code": "DB_UNAVAILABLE",
                "action": "retry_in_30s",
            },
            status=500,
        )
