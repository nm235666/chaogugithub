#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.agent_service import run_multi_role_analysis, run_trend_analysis


class AgentServiceContractTest(unittest.TestCase):
    def test_trend_service_returns_unified_fields(self):
        deps = {
            "build_trend_features": lambda ts_code, lookback: {
                "name": "测试股票",
                "samples": lookback,
                "date_range": {"start": "2026-01-01", "end": "2026-04-01"},
                "latest": {"trade_date": "2026-04-01", "close": 10.0, "pct_chg": 1.2},
                "trend_metrics": {"ma5": 10.1, "ma10": 9.9, "annualized_volatility_pct": 22.0},
                "recent_bars": [{"trade_date": "2026-04-01"}],
            },
            "call_llm_trend": lambda ts_code, features, model, temperature=0.2: {
                "analysis": "1) 趋势判断：上涨\n2) 置信度：78\n4) 风险点：关注回撤\n5) 未来5-20个交易日观察要点：观察量能",
                "requested_model": model,
                "used_model": "mock-model",
                "attempts": [{"model": "mock-model", "error": ""}],
            },
            "sqlite3": type("SQLiteStub", (), {"connect": staticmethod(lambda _: _ConnStub())}),
            "DB_PATH": "mock.db",
            "get_or_build_cached_logic_view": lambda conn, entity_type, entity_key, source_payload, builder: {
                "summary": {"risk": "关注回撤", "focus": "观察量能"},
                "chains": [],
            },
            "extract_logic_view_from_markdown": lambda text: {"summary": {}, "chains": []},
        }
        payload = run_trend_analysis(deps, ts_code="000001.SZ", lookback=60, model="auto")
        self.assertIn("analysis_markdown", payload)
        self.assertIn("decision_confidence", payload)
        self.assertIn("risk_review", payload)
        self.assertIn("portfolio_view", payload)
        self.assertIn("used_context_dims", payload)
        self.assertEqual(payload["used_model"], "mock-model")

    def test_multi_role_service_returns_role_outputs(self):
        deps = {
            "build_multi_role_context": lambda ts_code, lookback: {"company_profile": {"name": "测试公司"}, "macro_context": {"summary": "稳定"}},
            "call_llm_multi_role": lambda context, roles, model, temperature=0.2: {
                "analysis": "## 宏观经济分析师\n观点A\n\n## 综合结论\n结论B\n\n## 行动清单\n动作C\n\n## 关键分歧\n风险D",
                "requested_model": model,
                "used_model": "mock-model",
                "attempts": [],
            },
            "split_multi_role_analysis": lambda markdown, roles: {
                "role_sections": [{"role": roles[0], "content": "## 宏观经济分析师\n观点A", "matched": True, "logic_view": {}}],
                "common_sections_markdown": "## 综合结论\n结论B",
                "logic_view": {"summary": {"risk": "风险D", "focus": "动作C"}, "chains": []},
            },
            "sqlite3": type("SQLiteStub", (), {"connect": staticmethod(lambda _: _ConnStub())}),
            "DB_PATH": "mock.db",
            "get_or_build_cached_logic_view": lambda conn, entity_type, entity_key, source_payload, builder: {
                "summary": {"risk": "风险D", "focus": "动作C"},
                "chains": [],
            },
            "extract_logic_view_from_markdown": lambda text: {"summary": {}, "chains": []},
        }
        payload = run_multi_role_analysis(
            deps,
            ts_code="000001.SZ",
            lookback=120,
            roles=["宏观经济分析师"],
            model="auto",
        )
        self.assertTrue(payload["role_outputs"])
        self.assertIn("risk_review", payload)
        self.assertIn("portfolio_view", payload)
        self.assertEqual(payload["name"], "测试公司")

    def test_trend_service_supports_optional_risk_and_notification_hooks(self):
        deps = {
            "build_trend_features": lambda ts_code, lookback: {
                "name": "测试股票",
                "samples": lookback,
                "date_range": {"start": "2026-01-01", "end": "2026-04-01"},
                "latest": {"trade_date": "2026-04-01", "close": 10.0, "pct_chg": 1.2},
                "trend_metrics": {"annualized_volatility_pct": 22.0},
                "recent_bars": [{"trade_date": "2026-04-01"}],
            },
            "call_llm_trend": lambda ts_code, features, model, temperature=0.2: {
                "analysis": "趋势分析\n置信度：70",
                "requested_model": model,
                "used_model": "mock-model",
                "attempts": [],
            },
            "sqlite3": type("SQLiteStub", (), {"connect": staticmethod(lambda _: _ConnStub())}),
            "DB_PATH": "mock.db",
            "get_or_build_cached_logic_view": lambda conn, entity_type, entity_key, source_payload, builder: {
                "summary": {"risk": "回撤", "focus": "量能"},
                "chains": [],
            },
            "extract_logic_view_from_markdown": lambda text: {"summary": {}, "chains": []},
            "enable_risk_precheck": True,
            "pre_trade_check_fn": lambda signal: {"allowed": True, "checks": [{"rule": "mock", "ok": True}], "reasons": [], "signal": signal},
            "enable_notifications": True,
            "notify_result_fn": lambda **kwargs: {"ok": True, "meta": kwargs},
        }
        payload = run_trend_analysis(deps, ts_code="000001.SZ", lookback=60, model="auto")
        self.assertIn("pre_trade_check", payload)
        self.assertTrue(payload["pre_trade_check"]["allowed"])
        self.assertIn("notification", payload)
        self.assertTrue(payload["notification"]["ok"])

    def test_multi_role_service_supports_optional_risk_and_notification_hooks(self):
        deps = {
            "build_multi_role_context": lambda ts_code, lookback: {
                "company_profile": {"name": "测试公司"},
                "risk_summary": {
                    "items": [
                        {"max_drawdown": 0.11},
                    ]
                },
            },
            "call_llm_multi_role": lambda context, roles, model, temperature=0.2: {
                "analysis": "## 宏观经济分析师\n观点A\n\n## 综合结论\n结论B\n\n## 行动清单\n动作C\n\n## 关键分歧\n风险D",
                "requested_model": model,
                "used_model": "mock-model",
                "attempts": [],
            },
            "split_multi_role_analysis": lambda markdown, roles: {
                "role_sections": [{"role": roles[0], "content": "## 宏观经济分析师\n观点A", "matched": True, "logic_view": {}}],
                "common_sections_markdown": "## 综合结论\n结论B",
                "logic_view": {"summary": {"risk": "风险D", "focus": "动作C"}, "chains": []},
            },
            "sqlite3": type("SQLiteStub", (), {"connect": staticmethod(lambda _: _ConnStub())}),
            "DB_PATH": "mock.db",
            "get_or_build_cached_logic_view": lambda conn, entity_type, entity_key, source_payload, builder: {
                "summary": {"risk": "风险D", "focus": "动作C"},
                "chains": [],
            },
            "extract_logic_view_from_markdown": lambda text: {"summary": {}, "chains": []},
            "enable_risk_precheck": True,
            "pre_trade_check_fn": lambda signal: {"allowed": True, "checks": [{"rule": "mock", "ok": True}], "reasons": [], "signal": signal},
            "enable_notifications": True,
            "notify_result_fn": lambda **kwargs: {"ok": True, "meta": kwargs},
        }
        payload = run_multi_role_analysis(
            deps,
            ts_code="000001.SZ",
            lookback=120,
            roles=["宏观经济分析师"],
            model="auto",
        )
        self.assertIn("pre_trade_check", payload)
        self.assertTrue(payload["pre_trade_check"]["allowed"])
        self.assertIn("notification", payload)
        self.assertTrue(payload["notification"]["ok"])


class _ConnStub:
    def close(self):
        return None
