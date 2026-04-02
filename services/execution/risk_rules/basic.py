from __future__ import annotations


def max_position_check(position_weight: float, limit: float = 0.2) -> dict:
    ok = position_weight <= limit
    return {"rule": "max_position", "ok": ok, "message": "" if ok else f"仓位超过限制 {limit:.0%}"}


def max_drawdown_check(drawdown: float, limit: float = 0.12) -> dict:
    ok = drawdown <= limit
    return {"rule": "max_drawdown", "ok": ok, "message": "" if ok else f"回撤超过限制 {limit:.0%}"}


def volatility_check(volatility: float, limit: float = 0.45) -> dict:
    ok = volatility <= limit
    return {"rule": "volatility", "ok": ok, "message": "" if ok else f"波动率超过限制 {limit:.0%}"}


def liquidity_check(liquidity_score: float, min_score: float = 0.2) -> dict:
    ok = liquidity_score >= min_score
    return {"rule": "liquidity", "ok": ok, "message": "" if ok else f"流动性分数低于阈值 {min_score:.2f}"}


def pre_trade_check(
    signal_payload: dict,
    *,
    max_position_limit: float = 0.2,
    max_drawdown_limit: float = 0.12,
    volatility_limit: float = 0.45,
    min_liquidity_score: float = 0.2,
) -> dict:
    payload = dict(signal_payload or {})
    position_weight = float(payload.get("target_position_weight", 0.0) or 0.0)
    drawdown = float(payload.get("max_drawdown", 0.0) or 0.0)
    volatility = float(payload.get("volatility", 0.0) or 0.0)
    liquidity_score = float(payload.get("liquidity_score", 0.0) or 0.0)
    checks = [
        max_position_check(position_weight, max_position_limit),
        max_drawdown_check(drawdown, max_drawdown_limit),
        volatility_check(volatility, volatility_limit),
        liquidity_check(liquidity_score, min_liquidity_score),
    ]
    reasons = [item["message"] for item in checks if not item["ok"] and item.get("message")]
    return {
        "allowed": len(reasons) == 0,
        "reasons": reasons,
        "checks": checks,
    }
