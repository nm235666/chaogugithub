from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.system import llm_providers_admin as admin


class LlmProvidersAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_path = os.environ.get("LLM_PROVIDER_CONFIG_FILE")
        self.tempdir = tempfile.TemporaryDirectory()
        os.environ["LLM_PROVIDER_CONFIG_FILE"] = os.path.join(self.tempdir.name, "llm_providers.json")

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        if self._old_path is None:
            os.environ.pop("LLM_PROVIDER_CONFIG_FILE", None)
        else:
            os.environ["LLM_PROVIDER_CONFIG_FILE"] = self._old_path

    @patch("services.system.llm_providers_admin.reload_provider_runtime", return_value=None)
    def test_create_update_delete_provider(self, _reload) -> None:
        created = admin.create_llm_provider(
            {
                "provider_key": "gpt-5.4",
                "model": "gpt-5.4",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test-key",
                "status": "active",
            }
        )
        self.assertTrue(created.get("ok"))
        self.assertEqual(len(created.get("items") or []), 1)
        self.assertIn("observability_7d", (created.get("items") or [])[0])

        updated = admin.update_llm_provider(
            {
                "provider_key": "gpt-5.4",
                "index": 1,
                "status": "disabled",
                "rate_limit_per_minute": 3,
            }
        )
        item = (updated.get("items") or [])[0]
        self.assertEqual(item.get("status"), "disabled")
        self.assertEqual(int(item.get("rate_limit_per_minute") or 0), 3)

        deleted = admin.delete_llm_provider({"provider_key": "gpt-5.4", "index": 1})
        self.assertEqual(len(deleted.get("items") or []), 0)

    @patch("services.system.llm_providers_admin.reload_provider_runtime", return_value=None)
    def test_test_one_persists_health_recommendation(self, _reload) -> None:
        admin.create_llm_provider(
            {
                "provider_key": "gpt-5.4",
                "model": "gpt-5.4",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test-key",
                "status": "active",
            }
        )
        fake_result = SimpleNamespace(
            ok=False,
            status_code=429,
            latency_ms=123,
            error="too many requests",
            used_base_url="https://example.com/v1",
            used_model="gpt-5.4",
        )
        fake_attempts = [{"round": 1, "ok": False, "status_code": 429}]
        with patch("services.system.llm_providers_admin.probe_endpoint_autovalidate", return_value=(fake_result, fake_attempts)):
            out = admin.test_one_llm_provider({"provider_key": "gpt-5.4", "index": 1})
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("result", {}).get("health_recommendation"), "degraded")
        listed = admin.list_llm_providers()
        item = (listed.get("items") or [])[0]
        self.assertEqual(item.get("health_recommendation"), "degraded")


if __name__ == "__main__":
    unittest.main()
