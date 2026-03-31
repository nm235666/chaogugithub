from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
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
    if parsed.path in {"/", "/api"}:
        handler._send_json(
            {
                "service": "stock-codes-api",
                "message": "这是后端 API 服务，不是前端页面。",
                "frontend_url": f"http://{host}:8080/",
                "endpoints": deps["api_endpoints_catalog"],
            }
        )
        return True

    if parsed.path == "/api/health":
        handler._send_json({"ok": True, "db": deps["db_label"]()})
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

    return False
