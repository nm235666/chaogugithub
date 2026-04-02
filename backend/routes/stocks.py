from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path == "/api/stock-detail":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        keyword = params.get("keyword", [""])[0].strip()
        try:
            lookback = int(params.get("lookback", ["60"])[0])
        except ValueError:
            handler._send_json({"error": "lookback 必须是整数"}, status=400)
            return True
        if not ts_code and not keyword:
            handler._send_json({"error": "缺少 ts_code 或 keyword"}, status=400)
            return True
        try:
            payload = deps["query_stock_detail"](ts_code=ts_code, keyword=keyword, lookback=lookback)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"详情查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/stocks":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        status = params.get("status", [""])[0]
        market = params.get("market", [""])[0]
        area = params.get("area", [""])[0]

        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True

        try:
            payload = deps["query_stocks"](keyword, status, market, area, page, page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True

        handler._send_json(payload)
        return True

    if parsed.path == "/api/stocks/filters":
        try:
            payload = deps["query_stock_filters"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/stock-scores/filters":
        try:
            payload = deps["query_stock_score_filters"]()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/stock-scores":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        market = params.get("market", [""])[0]
        area = params.get("area", [""])[0]
        industry = params.get("industry", [""])[0]
        sort_by = params.get("sort_by", ["total_score"])[0]
        sort_order = params.get("sort_order", ["desc"])[0]
        try:
            min_score = float(params.get("min_score", ["0"])[0] or 0)
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "min_score/page/page_size 参数格式错误"}, status=400)
            return True
        try:
            payload = deps["query_stock_scores"](
                keyword=keyword,
                market=market,
                area=area,
                industry=industry,
                min_score=min_score,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_order=sort_order,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/prices":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0]
        start_date = params.get("start_date", [""])[0]
        end_date = params.get("end_date", [""])[0]

        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True

        try:
            payload = deps["query_prices"](ts_code, start_date, end_date, page, page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True

        handler._send_json(payload)
        return True

    if parsed.path == "/api/minline":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0]
        trade_date = params.get("trade_date", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["240"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_minline"](ts_code, trade_date, page, page_size)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/llm/trend":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        if not ts_code:
            handler._send_json({"error": "缺少 ts_code"}, status=400)
            return True
        try:
            lookback = int(params.get("lookback", ["120"])[0])
        except ValueError:
            handler._send_json({"error": "lookback 必须是整数"}, status=400)
            return True
        auth_ctx = deps.get("auth_context") or {}
        quota = deps["consume_trend_daily_quota"](auth_ctx.get("user"))
        if not quota.get("allowed", True):
            handler._send_json(
                {
                    "error": f"LLM走势分析今日次数已用完（{quota.get('limit')} 次/日），请明日再试或升级权限",
                    "quota": quota,
                },
                status=429,
            )
            return True
        try:
            payload = deps["run_trend_analysis"](
                deps,
                ts_code=ts_code,
                lookback=lookback,
                model=model,
                temperature=0.2,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"分析失败: {exc}"}, status=500)
            return True
        payload["quota"] = quota
        handler._send_json(payload)
        return True

    if parsed.path == "/api/llm/multi-role":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        roles_raw = params.get("roles", [""])[0]
        if not ts_code:
            handler._send_json({"error": "缺少 ts_code"}, status=400)
            return True
        try:
            lookback = int(params.get("lookback", ["120"])[0])
        except ValueError:
            handler._send_json({"error": "lookback 必须是整数"}, status=400)
            return True
        auth_ctx = deps.get("auth_context") or {}
        quota = deps["consume_multi_role_daily_quota"](auth_ctx.get("user"))
        if not quota.get("allowed", True):
            handler._send_json(
                {
                    "error": f"LLM多角色分析今日次数已用完（{quota.get('limit')} 次/日），请明日再试或升级权限",
                    "quota": quota,
                },
                status=429,
            )
            return True
        roles = deps["_resolve_roles"](roles_raw)
        try:
            payload = deps["run_multi_role_analysis"](
                deps,
                ts_code=ts_code,
                lookback=lookback,
                roles=roles,
                model=model,
                temperature=0.2,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"分析失败: {exc}"}, status=500)
            return True
        payload["quota"] = quota
        handler._send_json(payload)
        return True

    if parsed.path == "/api/llm/multi-role/start":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        roles_raw = params.get("roles", [""])[0]
        if not ts_code:
            handler._send_json({"error": "缺少 ts_code"}, status=400)
            return True
        try:
            lookback = int(params.get("lookback", ["120"])[0])
        except ValueError:
            handler._send_json({"error": "lookback 必须是整数"}, status=400)
            return True
        auth_ctx = deps.get("auth_context") or {}
        quota = deps["consume_multi_role_daily_quota"](auth_ctx.get("user"))
        if not quota.get("allowed", True):
            handler._send_json(
                {
                    "error": f"LLM多角色分析今日次数已用完（{quota.get('limit')} 次/日），请明日再试或升级权限",
                    "quota": quota,
                },
                status=429,
            )
            return True
        roles = deps["_resolve_roles"](roles_raw)
        try:
            job = deps["start_async_multi_role_job"](ts_code, lookback, model, roles)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"启动分析失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, "quota": quota, **job})
        return True

    if parsed.path == "/api/llm/multi-role/task":
        params = parse_qs(parsed.query)
        job_id = params.get("job_id", [""])[0].strip()
        if not job_id:
            handler._send_json({"error": "缺少 job_id"}, status=400)
            return True
        job = deps["get_async_multi_role_job"](job_id)
        if not job:
            handler._send_json({"error": f"任务不存在或已过期: {job_id}"}, status=404)
            return True
        handler._send_json({"ok": True, **job})
        return True

    return False
