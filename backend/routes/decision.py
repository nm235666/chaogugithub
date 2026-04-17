from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/decision"):
        return False

    if parsed.path == "/api/decision/board":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_board"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"决策板查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/scores":
        params = parse_qs(parsed.query)
        try:
            page_size = int(params.get("page_size", ["8"])[0] or 8)
        except ValueError:
            handler._send_json({"ok": False, "error": "page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_scoreboard"](page_size=page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"评分总览查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/stock":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        keyword = params.get("keyword", [""])[0].strip()
        if not ts_code and not keyword:
            handler._send_json({"ok": False, "error": "缺少 ts_code 或 keyword"}, status=400)
            return True
        try:
            payload = deps["query_decision_stock"](ts_code=ts_code, keyword=keyword)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"股票决策查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/plan":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_trade_plan"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"决策板查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/strategies":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["12"])[0] or 12)
            run_id = int(params.get("run_id", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_strategy_lab"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
                run_id=run_id,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略生成查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/strategy-runs":
        params = parse_qs(parsed.query)
        run_id_raw = params.get("run_id", [""])[0].strip()
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
            run_id = int(run_id_raw) if run_id_raw else 0
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            if run_id:
                payload = deps["query_decision_strategy_lab"](
                    page=page,
                    page_size=page_size,
                    ts_code=params.get("ts_code", [""])[0].strip().upper(),
                    keyword=params.get("keyword", [""])[0].strip(),
                    run_id=run_id,
                )
                handler._send_json({"ok": True, **payload})
                return True
            payload = deps["query_decision_strategy_runs"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
                keyword=params.get("keyword", [""])[0].strip(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略运行历史查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/history":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_history"](page=page, page_size=page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"历史查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/actions":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_actions"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"动作查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/calibration":
        params = parse_qs(parsed.query)
        try:
            page = int(params.get("page", ["1"])[0] or 1)
            page_size = int(params.get("page_size", ["20"])[0] or 20)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_decision_calibration"](
                page=page,
                page_size=page_size,
                ts_code=params.get("ts_code", [""])[0].strip().upper(),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"裁决精准度查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/decision/kill-switch":
        try:
            payload = deps["get_decision_kill_switch"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"状态查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/decision"):
        return False

    if parsed.path == "/api/decision/kill-switch":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可切换 Kill Switch"}, status=403)
            return True
        allow_trading = payload.get("allow_trading", payload.get("enabled", True))
        reason = str(payload.get("reason", "") or "").strip()
        try:
            result = deps["set_decision_kill_switch"](allow_trading=bool(allow_trading), reason=reason)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"状态更新失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/snapshot/run":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可触发快照"}, status=403)
            return True
        try:
            payload = deps["run_decision_snapshot"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"快照运行失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/decision/strategy-runs/run":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再生成策略批次"}, status=401)
            return True
        ts_code = str(payload.get("ts_code", "") or "").strip().upper()
        keyword = str(payload.get("keyword", "") or "").strip()
        try:
            page = int(payload.get("page", 1) or 1)
            page_size = int(payload.get("page_size", 12) or 12)
        except ValueError:
            handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            result = deps["run_decision_strategy_generation"](
                page=page,
                page_size=page_size,
                ts_code=ts_code,
                keyword=keyword,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"策略批次生成失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    if parsed.path == "/api/decision/actions":
        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再记录决策动作"}, status=401)
            return True
        user = auth_ctx.get("user") or {}
        action_type = str(payload.get("action_type", "") or "").strip().lower()
        ts_code = str(payload.get("ts_code", "") or "").strip().upper()
        stock_name = str(payload.get("stock_name", "") or "").strip()
        note = str(payload.get("note", "") or "").strip()
        snapshot_date = str(payload.get("snapshot_date", "") or "").strip()
        context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        # Structured evidence and review fields — stored in payload JSON blob
        evidence_sources_raw = payload.get("evidence_sources")
        evidence_sources = evidence_sources_raw if isinstance(evidence_sources_raw, list) else []
        execution_status = str(payload.get("execution_status", "") or "").strip()
        review_conclusion = str(payload.get("review_conclusion", "") or "").strip()
        if not ts_code:
            handler._send_json({"ok": False, "error": "缺少 ts_code"}, status=400)
            return True
        if not action_type:
            handler._send_json({"ok": False, "error": "缺少 action_type"}, status=400)
            return True
        action_payload: dict = {
            "context": context,
            "source": str(context.get("source") or "decision_board"),
        }
        if evidence_sources:
            action_payload["evidence_sources"] = evidence_sources
        if execution_status:
            action_payload["execution_status"] = execution_status
        if review_conclusion:
            action_payload["review_conclusion"] = review_conclusion
        try:
            result = deps["record_decision_action"](
                action_type=action_type,
                ts_code=ts_code,
                stock_name=stock_name,
                note=note,
                actor=str(user.get("username") or user.get("display_name") or "anonymous"),
                snapshot_date=snapshot_date,
                payload=action_payload,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"动作记录失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **result})
        return True

    return False
