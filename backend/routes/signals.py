from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if parsed.path == "/api/investment-signals":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        signal_type = params.get("signal_type", [""])[0]
        signal_group = params.get("signal_group", [""])[0]
        scope = params.get("scope", ["7d"])[0]
        source_filter = params.get("source_filter", [""])[0]
        direction = params.get("direction", [""])[0]
        signal_status = params.get("signal_status", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_investment_signals"](
                keyword=keyword,
                signal_type=signal_type,
                signal_group=signal_group,
                scope=scope,
                source_filter=source_filter,
                direction=direction,
                signal_status=signal_status,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/theme-hotspots":
        params = parse_qs(parsed.query)
        keyword = params.get("keyword", [""])[0]
        theme_group = params.get("theme_group", [""])[0]
        direction = params.get("direction", [""])[0]
        heat_level = params.get("heat_level", [""])[0]
        state_filter = params.get("state", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_theme_hotspots"](
                keyword=keyword,
                theme_group=theme_group,
                direction=direction,
                heat_level=heat_level,
                state_filter=state_filter,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/signal-state/timeline":
        params = parse_qs(parsed.query)
        signal_scope = params.get("signal_scope", [""])[0]
        signal_key = params.get("signal_key", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_signal_state_timeline"](
                signal_scope=signal_scope,
                signal_key=signal_key,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"时间线查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/research-reports":
        params = parse_qs(parsed.query)
        report_type = params.get("report_type", [""])[0]
        keyword = params.get("keyword", [""])[0]
        report_date = params.get("report_date", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_research_reports"](
                report_type=report_type,
                keyword=keyword,
                report_date=report_date,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/investment-signals/timeline":
        params = parse_qs(parsed.query)
        signal_key = params.get("signal_key", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["20"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_investment_signal_timeline"](
                signal_key=signal_key,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"时间线查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/macro/indicators":
        try:
            payload = {"items": deps["query_macro_indicators"](limit=1000)}
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/macro":
        params = parse_qs(parsed.query)
        indicator_code = params.get("indicator_code", [""])[0]
        freq = params.get("freq", [""])[0]
        period_start = params.get("period_start", [""])[0]
        period_end = params.get("period_end", [""])[0]
        keyword = params.get("keyword", [""])[0]
        try:
            page = int(params.get("page", ["1"])[0])
            page_size = int(params.get("page_size", ["200"])[0])
        except ValueError:
            handler._send_json({"error": "page/page_size 必须是整数"}, status=400)
            return True
        try:
            payload = deps["query_macro_series"](
                indicator_code=indicator_code,
                freq=freq,
                period_start=period_start,
                period_end=period_end,
                keyword=keyword,
                page=page,
                page_size=page_size,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"error": f"查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    return False
