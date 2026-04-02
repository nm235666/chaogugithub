from __future__ import annotations


def build_company_context(deps: dict, ts_code: str, lookback: int) -> dict:
    return deps["build_multi_role_context"](ts_code, lookback)


def summarize_context_dimensions(context: dict) -> list[str]:
    mapping = {
        "company_profile": "公司画像",
        "price_summary": "价格行为",
        "financial_summary": "财务数据",
        "valuation_summary": "估值数据",
        "capital_flow_summary": "资金流",
        "event_summary": "公司事件",
        "macro_context": "宏观",
        "fx_context": "汇率",
        "rate_spread_context": "利率利差",
        "governance_summary": "公司治理",
        "risk_summary": "风险情景",
        "stock_news_summary": "股票新闻",
    }
    dims: list[str] = []
    for key, label in mapping.items():
        value = context.get(key)
        if value not in ({}, [], "", None):
            dims.append(label)
    return dims
