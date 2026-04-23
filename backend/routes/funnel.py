from __future__ import annotations

import re
from urllib.parse import parse_qs

from backend.layers.layer1_user_decision import (
    create_candidate,
    get_candidate,
    get_funnel_metrics,
    list_candidates,
    transition_candidate,
    VALID_STATES,
)

# Match /api/funnel/candidates/<id> and /api/funnel/candidates/<id>/transition
_CANDIDATE_ID_RE = re.compile(r"^/api/funnel/candidates/([^/]+)(/transition)?$")


def _guard_write(deps: dict, *, scope: str) -> str | None:
    guard = deps.get("assert_write_allowed")
    if not callable(guard):
        return None
    try:
        guard(scope=scope, layer="layer1_user_decision")
        return None
    except Exception as exc:
        return str(exc)


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/funnel"):
        return False

    if parsed.path == "/api/funnel/candidates":
        params = parse_qs(parsed.query)
        state = params.get("state", [""])[0].strip()
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
            offset = int(params.get("offset", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit/offset 必须是整数"}, status=400)
            return True
        limit = max(1, min(limit, 200))
        try:
            payload = list_candidates(state=state, limit=limit, offset=offset)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"候选标的查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/funnel/metrics":
        try:
            payload = get_funnel_metrics()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"漏斗指标查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    m = _CANDIDATE_ID_RE.match(parsed.path)
    if m and not m.group(2):
        candidate_id = m.group(1)
        try:
            candidate = get_candidate(candidate_id)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"候选标的查询失败: {exc}"}, status=500)
            return True
        if candidate is None:
            handler._send_json({"ok": False, "error": "候选标的不存在"}, status=404)
            return True
        handler._send_json({"ok": True, **candidate})
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/funnel"):
        return False

    if parsed.path == "/api/funnel/candidates":
        denied = _guard_write(deps, scope="funnel.candidates")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        ts_code = str(payload.get("ts_code") or "").strip().upper()
        name = str(payload.get("name") or "").strip()
        source = str(payload.get("source") or "").strip()
        trigger_source = str(payload.get("trigger_source") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        evidence_ref = str(payload.get("evidence_ref") or "").strip()
        if not ts_code:
            handler._send_json({"ok": False, "error": "缺少 ts_code"}, status=400)
            return True
        if not name:
            handler._send_json({"ok": False, "error": "缺少 name"}, status=400)
            return True
        try:
            result = create_candidate(
                ts_code=ts_code,
                name=name,
                source=source,
                trigger_source=trigger_source,
                reason=reason,
                evidence_ref=evidence_ref,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"创建候选标的失败: {exc}"}, status=500)
            return True
        status_code = 200 if result.get("ok") else 500
        handler._send_json(result, status=status_code)
        return True

    m = _CANDIDATE_ID_RE.match(parsed.path)
    if m and m.group(2) == "/transition":
        denied = _guard_write(deps, scope="funnel.transition")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        candidate_id = m.group(1)
        to_state = str(payload.get("to_state") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        evidence_ref = str(payload.get("evidence_ref") or "").strip()
        trigger_source = str(payload.get("trigger_source") or "").strip()
        operator = str(payload.get("operator") or "system").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        try:
            state_version = int(payload.get("state_version") or 0)
        except (TypeError, ValueError):
            state_version = 0

        if not to_state:
            handler._send_json({"ok": False, "error": "缺少 to_state"}, status=400)
            return True
        if to_state not in VALID_STATES:
            handler._send_json(
                {"ok": False, "error": f"无效目标状态: {to_state}", "valid_states": sorted(VALID_STATES)},
                status=400,
            )
            return True
        try:
            result = transition_candidate(
                candidate_id,
                to_state=to_state,
                reason=reason,
                evidence_ref=evidence_ref,
                trigger_source=trigger_source,
                operator=operator,
                idempotency_key=idempotency_key,
                state_version=state_version,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"状态转换失败: {exc}"}, status=500)
            return True
        if not result.get("ok"):
            handler._send_json(result, status=409)
            return True
        handler._send_json(result)
        return True

    return False
