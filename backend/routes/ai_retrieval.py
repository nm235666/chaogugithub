from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if parsed.path == "/api/ai-retrieval/search":
        query = str((payload or {}).get("query") or "").strip()
        scene = str((payload or {}).get("scene") or "news").strip()
        requested_model = str((payload or {}).get("requested_model") or "").strip()
        try:
            top_k = int((payload or {}).get("top_k") or 8)
        except Exception:
            top_k = 8
        if not query:
            handler._send_json({"ok": False, "error": "query 不能为空"}, status=400)
            return True
        try:
            data = deps["ai_retrieval_search"](
                query=query,
                scene=scene,
                top_k=max(1, min(top_k, 30)),
                requested_model=requested_model,
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"检索失败: {exc}"}, status=500)
            return True
        handler._send_json(data)
        return True

    if parsed.path == "/api/ai-retrieval/context":
        query = str((payload or {}).get("query") or "").strip()
        scene = str((payload or {}).get("scene") or "news").strip()
        requested_model = str((payload or {}).get("requested_model") or "").strip()
        try:
            top_k = int((payload or {}).get("top_k") or 6)
        except Exception:
            top_k = 6
        try:
            max_chars = int((payload or {}).get("max_chars") or 2400)
        except Exception:
            max_chars = 2400
        if not query:
            handler._send_json({"ok": False, "error": "query 不能为空"}, status=400)
            return True
        try:
            data = deps["ai_retrieval_context"](
                query=query,
                scene=scene,
                top_k=max(1, min(top_k, 20)),
                max_chars=max(600, min(max_chars, 8000)),
                requested_model=requested_model,
            )
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"上下文构建失败: {exc}"}, status=500)
            return True
        handler._send_json(data)
        return True

    if parsed.path == "/api/ai-retrieval/sync":
        scene = str((payload or {}).get("scene") or "news").strip()
        try:
            limit = int((payload or {}).get("limit") or 300)
        except Exception:
            limit = 300
        try:
            data = deps["ai_retrieval_sync"](scene=scene, limit=max(20, min(limit, 5000)))
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"索引同步失败: {exc}"}, status=500)
            return True
        handler._send_json(data)
        return True

    return False


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path == "/api/ai-retrieval/metrics":
        params = parse_qs(parsed.query)
        try:
            days = int(params.get("days", ["1"])[0])
        except Exception:
            days = 1
        try:
            data = deps["ai_retrieval_metrics"](days=max(1, min(days, 30)))
        except Exception as exc:
            handler._send_json({"ok": False, "error": f"指标查询失败: {exc}"}, status=500)
            return True
        handler._send_json(data)
        return True
    return False
