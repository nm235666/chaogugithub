from __future__ import annotations

import unittest

from backend.layers.write_policies import assert_layer_write_allowed


class WritePoliciesTest(unittest.TestCase):
    def test_allow_decision_action_in_layer1(self):
        assert_layer_write_allowed(scope="decision.actions", layer="layer1_user_decision")

    def test_block_decision_action_in_layer3(self):
        with self.assertRaises(PermissionError):
            assert_layer_write_allowed(scope="decision.actions", layer="layer3_verification_research")

    def test_allow_jobs_trigger_in_layer4(self):
        assert_layer_write_allowed(scope="jobs.trigger", layer="layer4_backoffice_governance")

    def test_block_unknown_scope(self):
        with self.assertRaises(PermissionError):
            assert_layer_write_allowed(scope="unknown.scope", layer="layer1_user_decision")


if __name__ == "__main__":
    unittest.main()
