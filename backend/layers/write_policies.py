from __future__ import annotations


WRITE_SCOPE_LAYER: dict[str, str] = {
    # Layer 1 user-decision writes
    "decision.actions": "layer1_user_decision",
    "decision.snapshot": "layer1_user_decision",
    "decision.kill_switch": "layer1_user_decision",
    "decision.strategy_selection": "layer1_user_decision",
    "decision.strategy_runs": "layer1_user_decision",
    "decision.trade_advisor": "layer1_user_decision",
    "decision.trade_advisor_daily": "layer1_user_decision",
    "funnel.candidates": "layer1_user_decision",
    "funnel.transition": "layer1_user_decision",
    "portfolio.orders": "layer1_user_decision",
    "portfolio.review": "layer1_user_decision",
    # Layer 4 governance writes
    "data_readiness.run": "layer4_backoffice_governance",
    "jobs.trigger": "layer4_backoffice_governance",
    "jobs.dry_run": "layer4_backoffice_governance",
}


def assert_layer_write_allowed(*, scope: str, layer: str) -> None:
    expected = WRITE_SCOPE_LAYER.get(str(scope or "").strip())
    if expected is None:
        raise PermissionError(f"未注册写入 scope: {scope}")
    if str(layer or "").strip() != expected:
        raise PermissionError(f"写入 scope={scope} 仅允许 {expected}，当前层={layer}")
