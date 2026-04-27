from __future__ import annotations

from urllib.parse import parse_qs


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/macro/regime"):
        return False

    if parsed.path == "/api/macro/regime/agent-draft":
        from services.macro_service import get_latest_macro_regime_draft
        try:
            payload = get_latest_macro_regime_draft()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"三周期 Agent 草案查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    if parsed.path == "/api/macro/regime":
        from services.macro_service.regime import get_latest_regime, list_regimes

        params = parse_qs(parsed.query)

        if params.get("suggest", [""])[0] == "1":
            from services.macro_service import suggest_regime
            try:
                result = suggest_regime()
            except Exception as exc:  # pragma: no cover
                handler._send_json({"ok": False, "error": f"建议生成失败: {exc}"}, status=500)
                return True
            handler._send_json(result)
            return True

        history = params.get("history", ["0"])[0].strip() == "1"
        if history:
            try:
                page = int(params.get("page", ["1"])[0] or 1)
                page_size = int(params.get("page_size", ["10"])[0] or 10)
            except ValueError:
                handler._send_json({"ok": False, "error": "page/page_size 必须是整数"}, status=400)
                return True
            try:
                payload = list_regimes(page=page, page_size=page_size)
            except Exception as exc:  # pragma: no cover
                handler._send_json({"ok": False, "error": f"历史查询失败: {exc}"}, status=500)
                return True
            handler._send_json({"ok": True, **payload})
            return True

        try:
            payload = get_latest_regime()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"宏观三周期状态查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload)
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/macro/regime"):
        return False

    if parsed.path == "/api/macro/regime/agent-draft/run":
        from services.macro_service import run_macro_regime_agent

        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("is_admin"):
            handler._send_json({"ok": False, "error": "仅管理员可触发三周期状态 Agent"}, status=403)
            return True
        try:
            result = run_macro_regime_agent()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"三周期状态 Agent 运行失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/macro/regime/agent-draft/confirm":
        from services.macro_service import confirm_macro_regime_draft

        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再确认三周期草案"}, status=401)
            return True
        draft_id = str(payload.get("draft_id") or "").strip()
        if not draft_id:
            handler._send_json({"ok": False, "error": "缺少 draft_id"}, status=400)
            return True
        user = auth_ctx.get("user") or {}
        actor = str(user.get("username") or user.get("display_name") or "")
        overrides = payload.get("overrides") if isinstance(payload.get("overrides"), dict) else {}
        try:
            result = confirm_macro_regime_draft(draft_id, confirmed_by=actor, overrides=overrides)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"三周期草案确认失败: {exc}"}, status=500)
            return True
        handler._send_json(result)
        return True

    if parsed.path == "/api/macro/regime":
        from services.macro_service.regime import record_regime, VALID_STATES

        auth_ctx = deps.get("auth_context") or {}
        if not auth_ctx.get("authenticated"):
            handler._send_json({"ok": False, "error": "请先登录后再记录宏观状态"}, status=401)
            return True

        short_state = str(payload.get("short_term_state") or "volatile").strip()
        medium_state = str(payload.get("medium_term_state") or "volatile").strip()
        long_state = str(payload.get("long_term_state") or "volatile").strip()

        for state, name in [
            (short_state, "short_term_state"),
            (medium_state, "medium_term_state"),
            (long_state, "long_term_state"),
        ]:
            if state not in VALID_STATES:
                handler._send_json(
                    {"ok": False, "error": f"{name} 无效，有效值: {sorted(VALID_STATES)}"},
                    status=400,
                )
                return True

        try:
            short_conf = float(payload.get("short_term_confidence") or 0.7)
            medium_conf = float(payload.get("medium_term_confidence") or 0.7)
            long_conf = float(payload.get("long_term_confidence") or 0.7)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "confidence 必须是 0-1 之间的数字"}, status=400)
            return True

        user = auth_ctx.get("user") or {}
        created_by = str(user.get("username") or user.get("display_name") or "")

        try:
            result = record_regime(
                short_term_state=short_state,
                medium_term_state=medium_state,
                long_term_state=long_state,
                short_term_confidence=short_conf,
                medium_term_confidence=medium_conf,
                long_term_confidence=long_conf,
                short_term_change_reason=str(payload.get("short_term_change_reason") or ""),
                medium_term_change_reason=str(payload.get("medium_term_change_reason") or ""),
                long_term_change_reason=str(payload.get("long_term_change_reason") or ""),
                short_term_changed=bool(payload.get("short_term_changed")),
                medium_term_changed=bool(payload.get("medium_term_changed")),
                long_term_changed=bool(payload.get("long_term_changed")),
                created_by=created_by,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"宏观状态记录失败: {exc}"}, status=500)
            return True

        handler._send_json(result)
        return True

    return False


def dispatch_patch(handler, parsed, payload: dict, deps: dict) -> bool:
    if parsed.path != "/api/macro/regime":
        return False

    from urllib.parse import parse_qs
    params = parse_qs(parsed.query)
    regime_id = str(params.get("id", [""])[0] or "").strip()
    if not regime_id:
        handler._send_json({"ok": False, "error": "id required"}, status=400)
        return True

    outcome_notes = str(payload.get("outcome_notes", "") or "").strip()
    outcome_rating = str(payload.get("outcome_rating", "") or "").strip()
    correction_suggestion = str(payload.get("correction_suggestion", "") or "").strip()
    valid_ratings = {"effective", "partial", "ineffective", ""}
    if outcome_rating not in valid_ratings:
        handler._send_json(
            {"ok": False, "error": f"outcome_rating must be one of {valid_ratings}"},
            status=400,
        )
        return True

    from services.macro_service import update_regime_outcome
    try:
        result = update_regime_outcome(regime_id, outcome_notes, outcome_rating, correction_suggestion)
    except Exception as exc:
        handler._send_json({"ok": False, "error": str(exc)}, status=500)
        return True
    handler._send_json(result)
    return True
