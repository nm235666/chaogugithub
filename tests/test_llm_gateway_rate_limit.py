from __future__ import annotations

import unittest
from unittest.mock import patch

import llm_gateway


class LlmGatewayRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        llm_gateway._LOCAL_RATE_BUCKETS.clear()
        llm_gateway._LOCAL_STATE.clear()
        llm_gateway._LOCAL_METRICS.clear()
        llm_gateway._LOCAL_LATENCIES.clear()

    def test_local_rate_limit_blocks_after_threshold(self) -> None:
        with patch("llm_gateway.get_redis_client", return_value=None):
            allowed = []
            for _ in range(4):
                ok, _, _ = llm_gateway.rate_limit_allow("gpt-5.4|https://example.com/v1", 3)
                allowed.append(ok)
        self.assertEqual(allowed, [True, True, True, False])

    def test_runtime_status_disabled_node(self) -> None:
        status = llm_gateway.get_runtime_rate_limit_status(
            model="gpt-5.4",
            base_url="https://example.com/v1",
            rate_limit_enabled=False,
            rate_limit_per_minute=10,
        )
        self.assertEqual(status.get("runtime_status"), "ok")
        self.assertFalse(bool(status.get("rate_limit_enabled")))

    def test_mark_429_enters_cooldown(self) -> None:
        sig = "gpt-5.4|https://example.com/v1"
        with patch("llm_gateway.get_redis_client", return_value=None):
            llm_gateway.mark_provider_result(sig, success=False, status_code=429)
            ok, _, _ = llm_gateway.rate_limit_allow(sig, 10)
        self.assertFalse(ok)

    def test_observability_snapshot_local(self) -> None:
        sig = "gpt-5.4|https://example.com/v1"
        with patch("llm_gateway.get_redis_client", return_value=None):
            llm_gateway.record_provider_metrics(sig, success=True, status_code=200, latency_ms=120)
            llm_gateway.record_provider_metrics(sig, success=False, status_code=429, latency_ms=350, switch_event=True)
            snap = llm_gateway.get_provider_observability_7d(sig)
        self.assertEqual(int(snap.get("total_calls") or 0), 2)
        self.assertEqual(int(snap.get("http_429") or 0), 1)
        self.assertEqual(int(snap.get("switch_count") or 0), 1)


if __name__ == "__main__":
    unittest.main()
