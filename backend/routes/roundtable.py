"""首席圆桌 API 路由层."""
from __future__ import annotations

from urllib.parse import parse_qs


def _auth_owner(auth_ctx: dict) -> str:
    """Return the username of the authenticated user, or '' for anonymous."""
    user = auth_ctx.get("user") or {}
    return str(user.get("username") or user.get("display_name") or "")


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/llm/chief-roundtable"):
        return False

    if parsed.path == "/api/llm/chief-roundtable/jobs":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        auth_ctx = deps.get("auth_context") or {}
        owner = "" if auth_ctx.get("is_admin") else _auth_owner(auth_ctx)
        try:
            payload = deps["roundtable_list"](ts_code=ts_code, page=page, page_size=page_size, owner=owner)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"圆桌列表查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    # /api/llm/chief-roundtable/jobs/<job_id>
    if parsed.path.startswith("/api/llm/chief-roundtable/jobs/"):
        job_id = parsed.path.split("/api/llm/chief-roundtable/jobs/", 1)[-1].split("/")[0].strip()
        if not job_id:
            handler._send_json({"ok": False, "error": "缺少 job_id"}, status=400)
            return True
        try:
            job = deps["roundtable_get"](job_id=job_id)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"圆桌任务查询失败: {exc}"}, status=500)
            return True
        if not job:
            handler._send_json({"ok": False, "error": "任务不存在"}, status=404)
            return True
        # Owner isolation: non-admin users may only view their own jobs
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            current_user = _auth_owner(auth_ctx)
            job_owner = str(job.get("owner") or "")
            if job_owner and current_user != job_owner:
                handler._send_json({"ok": False, "error": "无权访问该任务"}, status=403)
                return True
        handler._send_json({"ok": True, **job})
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/llm/chief-roundtable"):
        return False

    if parsed.path == "/api/llm/chief-roundtable/jobs":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再创建圆桌任务"}, status=401)
            return True
        # Inject owner into payload for the service layer
        payload = dict(payload)
        payload["_owner"] = _auth_owner(auth_ctx)
        try:
            result = deps["roundtable_create"](payload)
        except ValueError as exc:
            handler._send_json({"ok": False, "error": str(exc)}, status=400)
            return True
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"创建圆桌任务失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    return False
