#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.signals_service import build_signals_service_deps


class SignalsServiceTest(unittest.TestCase):
    def test_build_signals_service_deps_exposes_expected_keys(self):
        deps = build_signals_service_deps(
            query_investment_signals_fn=lambda **kwargs: {"items": [], "page": kwargs.get("page", 1)},
            query_theme_hotspots_fn=lambda **kwargs: {"items": []},
            query_signal_state_timeline_fn=lambda **kwargs: {"events": []},
            query_research_reports_fn=lambda **kwargs: {"items": []},
            query_investment_signal_timeline_fn=lambda **kwargs: {"events": []},
            query_macro_indicators_fn=lambda **kwargs: {"items": []},
            query_macro_series_fn=lambda **kwargs: {"items": []},
        )
        self.assertIn("query_investment_signals", deps)
        self.assertIn("query_research_reports", deps)
        self.assertIn("query_macro_series", deps)
