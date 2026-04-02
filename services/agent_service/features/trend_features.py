from __future__ import annotations


def build_trend_features(deps: dict, ts_code: str, lookback: int) -> dict:
    return deps["build_trend_features"](ts_code, lookback)


def summarize_feature_dimensions(features: dict) -> list[str]:
    dims = ["日线价格", "趋势指标"]
    metrics = features.get("trend_metrics") or {}
    if any(metrics.get(key) is not None for key in ("ma5", "ma10", "ma20", "ma60")):
        dims.append("均线结构")
    if metrics.get("annualized_volatility_pct") is not None:
        dims.append("波动率")
    if features.get("recent_bars"):
        dims.append("近期K线")
    return dims
