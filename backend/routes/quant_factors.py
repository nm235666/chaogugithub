from __future__ import annotations

from urllib.parse import parse_qs


def _normalize_profile(value: str, *, default: str) -> str:
    text = str(value or "").strip()
    return text or default


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if parsed.path.startswith("/api/quant-factors") and not bool(deps.get("quant_factors_enabled", True)):
        handler._send_json({"ok": False, "error": "quant_factors_enabled 已关闭"}, status=503)
        return True

    if parsed.path == "/api/quant-factors/mine/start":
        direction = str(payload.get("direction", "") or "").strip()
        if not direction:
            handler._send_json({"ok": False, "error": "direction 不能为空"}, status=400)
            return True
        try:
            lookback = int(payload.get("lookback", 120) or 120)
        except ValueError:
            handler._send_json({"ok": False, "error": "lookback 必须是整数"}, status=400)
            return True
        try:
            task = deps["start_quantaalpha_mine_task"](
                direction=direction,
                market_scope=_normalize_profile(str(payload.get("market_scope", "") or ""), default="A_share"),
                lookback=lookback,
                config_profile=_normalize_profile(str(payload.get("config_profile", "") or ""), default="default"),
                llm_profile=_normalize_profile(str(payload.get("llm_profile", "") or ""), default="auto"),
                extra_args=payload.get("extra_args") or [],
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"启动因子挖掘失败: {exc}"}, status=500)
            return True
        handler._send_json(task)
        return True

    if parsed.path == "/api/quant-factors/backtest/start":
        direction = str(payload.get("direction", "") or "").strip()
        if not direction:
            handler._send_json({"ok": False, "error": "direction 不能为空"}, status=400)
            return True
        try:
            lookback = int(payload.get("lookback", 120) or 120)
        except ValueError:
            handler._send_json({"ok": False, "error": "lookback 必须是整数"}, status=400)
            return True
        try:
            task = deps["start_quantaalpha_backtest_task"](
                direction=direction,
                market_scope=_normalize_profile(str(payload.get("market_scope", "") or ""), default="A_share"),
                lookback=lookback,
                config_profile=_normalize_profile(str(payload.get("config_profile", "") or ""), default="default"),
                llm_profile=_normalize_profile(str(payload.get("llm_profile", "") or ""), default="auto"),
                extra_args=payload.get("extra_args") or [],
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"启动回测失败: {exc}"}, status=500)
            return True
        handler._send_json(task)
        return True

    return False


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path.startswith("/api/quant-factors") and not bool(deps.get("quant_factors_enabled", True)):
        handler._send_json({"ok": False, "error": "quant_factors_enabled 已关闭"}, status=503)
        return True

    if parsed.path == "/api/quant-factors/task":
        params = parse_qs(parsed.query)
        task_id = params.get("task_id", [""])[0].strip()
        if not task_id:
            handler._send_json({"ok": False, "error": "缺少 task_id"}, status=400)
            return True
        try:
            payload = deps["get_quantaalpha_task"](task_id)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"任务查询失败: {exc}"}, status=500)
            return True
        if not payload:
            handler._send_json({"ok": False, "error": f"任务不存在: {task_id}"}, status=404)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/quant-factors/results":
        params = parse_qs(parsed.query)
        task_type = params.get("task_type", [""])[0].strip()
        status = params.get("status", [""])[0].strip()
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_quantaalpha_results"](
                task_type=task_type,
                status=status,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"结果查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    return False
