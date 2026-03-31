from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path == "/api/stock-news":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0]
        company_name = params.get("company_name", [""])[0]
        keyword = params.get("keyword", [""])[0]
        source = params.get("source", [""])[0]
        finance_levels = params.get("finance_levels", [""])[0]
        date_from = params.get("date_from", [""])[0]
        date_to = params.get("date_to", [""])[0]
        scored = params.get("scored", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_stock_news"](
                ts_code,
                company_name,
                keyword,
                source,
                finance_levels,
                date_from,
                date_to,
                scored,
                page,
                page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/stock-news/sources":
        try:
            payload = {"items": deps["query_stock_news_sources"]()}
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"来源查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/stock-news/fetch":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        company_name = params.get("company_name", [""])[0].strip()
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        score = params.get("score", ["1"])[0].strip() not in {"0", "false", "False"}
        try:
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page_size 必须是整数"}, status=400)
            return True
        if not ts_code and not company_name:
            handler._send_json({"error": "缺少 ts_code 或 company_name"}, status=400)
            return True
        try:
            fetch_info = deps["fetch_stock_news_now"](
                ts_code=ts_code,
                company_name=company_name,
                page_size=page_size,
            )
            score_info = None
            if score and ts_code:
                score_info = deps["score_stock_news_now"](
                    ts_code=ts_code,
                    limit=min(page_size, 10),
                    model=model,
                )
            payload = deps["query_stock_news"](ts_code, company_name, "", "", "", "", "", "", 1, min(page_size, 20))
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"采集失败: {exc}"}, status=500)
            return True
        handler._send_json(
            {
                "ok": True,
                "ts_code": ts_code,
                "company_name": company_name,
                "model": model,
                "requested_model": model,
                "used_model": ((score_info or {}).get("meta") or {}).get("used_models", [None])[0] if score_info else "",
                "attempts": ((score_info or {}).get("meta") or {}).get("items", []),
                "fetch_stdout": fetch_info.get("stdout", ""),
                "score_stdout": score_info.get("stdout", "") if score_info else "",
                "items": payload.get("items", []),
                "total": payload.get("total", 0),
            }
        )
        return True

    if parsed.path == "/api/stock-news/score":
        params = parse_qs(parsed.query)
        ts_code = params.get("ts_code", [""])[0].strip().upper()
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        try:
            row_id = int(params.get("row_id", ["0"])[0])
            limit = int(params.get("limit", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "row_id/limit 必须是整数"}, status=400)
            return True
        force = params.get("force", ["0"])[0].strip().lower() in {"1", "true", "yes", "on"}
        if not ts_code and row_id <= 0:
            handler._send_json({"error": "缺少 ts_code 或 row_id"}, status=400)
            return True
        try:
            score_info = deps["score_stock_news_now"](
                ts_code=ts_code,
                limit=max(limit, 1),
                model=model,
                row_id=row_id,
                force=force,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"评分失败: {exc}"}, status=500)
            return True
        handler._send_json(
            {
                "ok": True,
                "ts_code": ts_code,
                "row_id": row_id,
                "requested_model": model,
                "used_model": ((score_info or {}).get("meta") or {}).get("used_models", [None])[0] if score_info else "",
                "attempts": ((score_info or {}).get("meta") or {}).get("items", []),
                "stdout": (score_info or {}).get("stdout", ""),
                "meta": (score_info or {}).get("meta", {}),
            }
        )
        return True

    if parsed.path == "/api/news/sources":
        try:
            payload = {"items": deps["query_news_sources"]()}
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/news":
        params = parse_qs(parsed.query)
        source = params.get("source", [""])[0]
        source_prefixes = params.get("source_prefixes", [""])[0]
        keyword = params.get("keyword", [""])[0]
        date_from = params.get("date_from", [""])[0]
        date_to = params.get("date_to", [""])[0]
        finance_levels = params.get("finance_levels", [""])[0]
        exclude_sources = params.get("exclude_sources", [""])[0]
        exclude_source_prefixes = params.get("exclude_source_prefixes", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_news"](
                source=source,
                source_prefixes=source_prefixes,
                keyword=keyword,
                date_from=date_from,
                date_to=date_to,
                finance_levels=finance_levels,
                exclude_sources=exclude_sources,
                exclude_source_prefixes=exclude_source_prefixes,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/news/daily-summaries":
        params = parse_qs(parsed.query)
        summary_date = params.get("summary_date", [""])[0]
        source_filter = params.get("source_filter", [""])[0]
        model = params.get("model", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_news_daily_summaries"](
                summary_date=summary_date,
                source_filter=source_filter,
                model=model,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/news/daily-summaries/generate":
        params = parse_qs(parsed.query)
        model = deps["normalize_model_name"](params.get("model", [deps["DEFAULT_LLM_MODEL"]])[0])
        summary_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            job = deps["start_async_daily_summary_job"](model=model, summary_date=summary_date)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"启动生成失败: {exc}"}, status=500)
            return True
        handler._send_json(
            {
                "ok": True,
                **job,
            }
        )
        return True

    if parsed.path == "/api/news/daily-summaries/task":
        params = parse_qs(parsed.query)
        job_id = params.get("job_id", [""])[0].strip()
        if not job_id:
            handler._send_json({"error": "缺少 job_id"}, status=400)
            return True
        job = deps["get_async_daily_summary_job"](job_id)
        if not job:
            handler._send_json({"error": f"任务不存在或已过期: {job_id}"}, status=404)
            return True
        handler._send_json({"ok": True, **job})
        return True

    return False
