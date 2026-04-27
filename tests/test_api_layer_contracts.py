from __future__ import annotations

import unittest

from backend.layers.api_contracts import is_api_method_allowed, resolve_api_layer_contract


class ApiLayerContractsTest(unittest.TestCase):
    def test_decision_scores_is_layer2(self):
        contract = resolve_api_layer_contract("/api/decision/scores")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer2_data_assets")
        self.assertTrue(is_api_method_allowed("/api/decision/scores", "GET"))
        self.assertFalse(is_api_method_allowed("/api/decision/scores", "POST"))

    def test_decision_calibration_is_layer3(self):
        contract = resolve_api_layer_contract("/api/decision/calibration")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer3_verification_research")
        self.assertTrue(is_api_method_allowed("/api/decision/calibration", "GET"))
        self.assertFalse(is_api_method_allowed("/api/decision/calibration", "POST"))

    def test_funnel_transition_is_layer1(self):
        contract = resolve_api_layer_contract("/api/funnel/candidates/123/transition")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer1_user_decision")
        self.assertTrue(is_api_method_allowed("/api/funnel/candidates/123/transition", "POST"))
        self.assertFalse(is_api_method_allowed("/api/funnel/candidates/123/transition", "GET"))

    def test_trade_advisor_refresh_is_layer1_post(self):
        contract = resolve_api_layer_contract("/api/decision/trade-advisor/refresh")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer1_user_decision")
        self.assertTrue(is_api_method_allowed("/api/decision/trade-advisor/refresh", "POST"))
        self.assertFalse(is_api_method_allowed("/api/decision/trade-advisor/refresh", "GET"))

    def test_strategy_selection_run_is_layer1_post(self):
        contract = resolve_api_layer_contract("/api/decision/strategy-selection/run")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer1_user_decision")
        self.assertTrue(is_api_method_allowed("/api/decision/strategy-selection/run", "POST"))
        self.assertFalse(is_api_method_allowed("/api/decision/strategy-selection/run", "GET"))

    def test_data_readiness_is_layer4(self):
        contract = resolve_api_layer_contract("/api/system/data-readiness")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer4_backoffice_governance")
        self.assertTrue(is_api_method_allowed("/api/system/data-readiness", "GET"))
        self.assertTrue(is_api_method_allowed("/api/system/data-readiness/run", "POST"))

    def test_quant_factors_is_layer3(self):
        contract = resolve_api_layer_contract("/api/quant-factors/task")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer3_verification_research")
        self.assertTrue(is_api_method_allowed("/api/quant-factors/task", "GET"))

    def test_system_auth_is_layer4(self):
        contract = resolve_api_layer_contract("/api/auth/users")
        self.assertIsNotNone(contract)
        self.assertEqual(contract.layer, "layer4_backoffice_governance")
        self.assertTrue(is_api_method_allowed("/api/auth/users", "GET"))
        self.assertTrue(is_api_method_allowed("/api/auth/users", "POST"))


if __name__ == "__main__":
    unittest.main()
