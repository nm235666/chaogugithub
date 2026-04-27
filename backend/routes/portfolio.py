from __future__ import annotations

import re
from urllib.parse import parse_qs

from services.portfolio_service import (
    add_review,
    create_order,
    create_order_from_decision_action,
    delete_review,
    get_trade_chain,
    list_orders,
    list_positions,
    list_review_chains,
    list_review_groups,
    list_reviews,
    update_order,
    VALID_ACTION_TYPES,
    VALID_ORDER_STATUSES,
)

# Match /api/portfolio/orders/<id>
_ORDER_ID_RE = re.compile(r"^/api/portfolio/orders/([^/]+)$")
_REVIEW_ID_RE = re.compile(r"^/api/portfolio/review/([^/]+)$")
_TRADE_CHAIN_RE = re.compile(r"^/api/portfolio/trade-chains/([^/]+)$")


def _guard_write(deps: dict, *, scope: str) -> str | None:
    guard = deps.get("assert_write_allowed")
    if not callable(guard):
        return None
    try:
        guard(scope=scope, layer="layer1_user_decision")
        return None
    except Exception as exc:
        return str(exc)


def _owner_key(deps: dict) -> str:
    auth_ctx = deps.get("auth_context") or {}
    user = auth_ctx.get("user") or {}
    return str(user.get("username") or user.get("id") or "anonymous")


def _trade_plan_risk_gate(deps: dict) -> dict:
    errors: list[str] = []
    warnings: list[str] = []

    get_switch = deps.get("get_decision_kill_switch")
    if callable(get_switch):
        try:
            switch = get_switch() or {}
            if int(switch.get("allow_trading", 1) or 0) != 1:
                errors.append(f"Kill Switch 已暂停交易：{switch.get('reason') or '未填写原因'}")
        except Exception as exc:
            warnings.append(f"Kill Switch 状态读取失败：{exc}")

    query_board = deps.get("query_decision_board")
    if callable(query_board):
        try:
            board = query_board(page=1, page_size=1, ts_code="", keyword="") or {}
            pipeline = board.get("pipeline_health") or {}
            status = str(pipeline.get("status") or "").strip()
            if status in {"empty", "not_initialized"}:
                errors.append(f"决策链路未就绪：{status}")
            elif status and status != "ready":
                warnings.append(f"决策链路状态：{status}")
        except Exception as exc:
            warnings.append(f"链路健康读取失败：{exc}")

    return {"ok": not errors, "errors": errors, "warnings": warnings}


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/portfolio"):
        return False

    if parsed.path == "/api/portfolio/positions":
        try:
            payload = list_positions()
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"持仓查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    m = _TRADE_CHAIN_RE.match(parsed.path)
    if m:
        order_no = m.group(1)
        try:
            payload = get_trade_chain(order_no)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"交易链查询失败: {exc}"}, status=500)
            return True
        handler._send_json(payload, status=200 if payload.get("ok") else 404)
        return True

    if parsed.path == "/api/portfolio/orders":
        params = parse_qs(parsed.query)
        status = params.get("status", [""])[0].strip()
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
            offset = int(params.get("offset", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit/offset 必须是整数"}, status=400)
            return True
        limit = max(1, min(limit, 200))
        decision_action_id = str(params.get("decision_action_id", [""])[0] or "").strip()
        try:
            payload = list_orders(status=status, decision_action_id=decision_action_id, limit=limit, offset=offset)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"订单查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/portfolio/review/chains":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
            offset = int(params.get("offset", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit/offset 必须是整数"}, status=400)
            return True
        limit = max(1, min(limit, 200))
        try:
            payload = list_review_chains(limit=limit, offset=offset)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"复盘交易链查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/portfolio/review/groups":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
            offset = int(params.get("offset", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit/offset 必须是整数"}, status=400)
            return True
        limit = max(1, min(limit, 200))
        order_id = str(params.get("order_id", [""])[0] or "").strip()
        try:
            payload = list_review_groups(order_id=order_id, limit=limit, offset=offset)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"复盘分组查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    if parsed.path == "/api/portfolio/review":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
            offset = int(params.get("offset", ["0"])[0] or 0)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit/offset 必须是整数"}, status=400)
            return True
        limit = max(1, min(limit, 200))
        order_id = str(params.get("order_id", [""])[0] or "").strip()
        try:
            payload = list_reviews(order_id=order_id, limit=limit, offset=offset)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"复盘记录查询失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/portfolio"):
        return False

    if parsed.path == "/api/portfolio/orders":
        denied = _guard_write(deps, scope="portfolio.orders")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        ts_code = str(payload.get("ts_code") or "").strip().upper()
        action_type = str(payload.get("action_type") or "buy").strip().lower()
        decision_action_id = str(payload.get("decision_action_id") or "").strip()
        chain_order_no = str(payload.get("chain_order_no") or payload.get("order_no") or "").strip()
        note = str(payload.get("note") or "").strip()
        try:
            size = int(payload.get("size") or 0)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "size 必须是整数"}, status=400)
            return True
        planned_price_raw = payload.get("planned_price")
        planned_price: float | None = None
        if planned_price_raw is not None:
            try:
                planned_price = float(planned_price_raw)
            except (TypeError, ValueError):
                handler._send_json({"ok": False, "error": "planned_price 必须是数字"}, status=400)
                return True

        if not ts_code:
            handler._send_json({"ok": False, "error": "缺少 ts_code"}, status=400)
            return True
        if action_type not in VALID_ACTION_TYPES:
            handler._send_json(
                {"ok": False, "error": f"无效操作类型: {action_type}", "valid": sorted(VALID_ACTION_TYPES)},
                status=400,
            )
            return True
        try:
            result = create_order(
                ts_code=ts_code,
                action_type=action_type,
                planned_price=planned_price,
                size=size,
                decision_action_id=decision_action_id,
                note=note,
                owner_key=_owner_key(deps),
                chain_order_no=chain_order_no,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"创建订单失败: {exc}"}, status=500)
            return True
        status_code = 200 if result.get("ok") else 400
        handler._send_json(result, status=status_code)
        return True

    if parsed.path == "/api/portfolio/orders/from-decision":
        denied = _guard_write(deps, scope="portfolio.orders")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        decision_action_id = str(payload.get("decision_action_id") or "").strip()
        action_type = str(payload.get("action_type") or "buy").strip().lower()
        note = str(payload.get("note") or "").strip()
        try:
            size = int(payload.get("size") or 0)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "size 必须是整数"}, status=400)
            return True
        planned_price_raw = payload.get("planned_price")
        try:
            planned_price = float(planned_price_raw)
        except (TypeError, ValueError):
            handler._send_json({"ok": False, "error": "planned_price 必须是数字"}, status=400)
            return True
        gate = _trade_plan_risk_gate(deps)
        if not gate.get("ok"):
            handler._send_json(
                {
                    "ok": False,
                    "error": "风控检查未通过，不能从决策动作创建计划单",
                    "risk_gate": gate,
                },
                status=400,
            )
            return True
        try:
            result = create_order_from_decision_action(
                decision_action_id=decision_action_id,
                action_type=action_type,
                planned_price=planned_price,
                size=size,
                note=note,
                owner_key=_owner_key(deps),
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"从决策动作创建订单失败: {exc}"}, status=500)
            return True
        status_code = 200 if result.get("ok") else 400
        if result.get("ok"):
            result["risk_gate"] = gate
        handler._send_json(result, status=status_code)
        return True

    if parsed.path == "/api/portfolio/review":
        denied = _guard_write(deps, scope="portfolio.review")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        order_id = str(payload.get("order_id") or "").strip()
        review_tag = str(payload.get("review_tag") or "").strip()
        review_note = str(payload.get("review_note") or "").strip()
        slippage_raw = payload.get("slippage")
        latency_ms_raw = payload.get("latency_ms")
        slippage: float | None = None
        latency_ms: int | None = None
        if slippage_raw is not None:
            try:
                slippage = float(slippage_raw)
            except (TypeError, ValueError):
                pass
        if latency_ms_raw is not None:
            try:
                latency_ms = int(latency_ms_raw)
            except (TypeError, ValueError):
                pass
        if not order_id:
            handler._send_json({"ok": False, "error": "缺少 order_id"}, status=400)
            return True
        try:
            result = add_review(
                order_id=order_id,
                review_tag=review_tag,
                review_note=review_note,
                slippage=slippage,
                latency_ms=latency_ms,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"添加复盘记录失败: {exc}"}, status=500)
            return True
        status_code = 200 if result.get("ok") else 500
        handler._send_json(result, status=status_code)
        return True

    return False


def dispatch_patch(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/portfolio"):
        return False

    m = _ORDER_ID_RE.match(parsed.path)
    if m:
        denied = _guard_write(deps, scope="portfolio.orders")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        order_id = m.group(1)
        status = payload.get("status")
        executed_price_raw = payload.get("executed_price")
        executed_at = payload.get("executed_at")
        executed_price: float | None = None
        if executed_price_raw is not None:
            try:
                executed_price = float(executed_price_raw)
            except (TypeError, ValueError):
                handler._send_json({"ok": False, "error": "executed_price 必须是数字"}, status=400)
                return True
        if status is not None:
            status = str(status).strip()
            if status not in VALID_ORDER_STATUSES:
                handler._send_json(
                    {"ok": False, "error": f"无效订单状态: {status}", "valid": sorted(VALID_ORDER_STATUSES)},
                    status=400,
                )
                return True
        try:
            result = update_order(
                order_id,
                status=status,
                executed_price=executed_price,
                executed_at=str(executed_at).strip() if executed_at is not None else None,
            )
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"更新订单失败: {exc}"}, status=500)
            return True
        if not result.get("ok"):
            handler._send_json(result, status=404)
            return True
        handler._send_json(result)
        return True

    return False


def dispatch_delete(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/portfolio"):
        return False

    m = _REVIEW_ID_RE.match(parsed.path)
    if m:
        denied = _guard_write(deps, scope="portfolio.review")
        if denied:
            handler._send_json({"ok": False, "error": denied}, status=403)
            return True
        review_id = m.group(1)
        try:
            result = delete_review(review_id)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"删除复盘记录失败: {exc}"}, status=500)
            return True
        status_code = 200 if result.get("ok") else 404
        handler._send_json(result, status=status_code)
        return True

    return False
