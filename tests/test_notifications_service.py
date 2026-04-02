#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.notifications import build_notification_payload, notify_with_wecom


class NotificationsServiceTest(unittest.TestCase):
    def test_build_payload_shape(self):
        payload = build_notification_payload(
            title="测试标题",
            summary="测试摘要",
            markdown="测试正文",
            subject_key="stock:600519.SH",
            link="https://example.com",
        )
        self.assertEqual(payload.title, "测试标题")
        self.assertEqual(payload.subject_key, "stock:600519.SH")

    def test_notify_with_wecom_requires_webhook(self):
        payload = build_notification_payload(
            title="测试标题",
            summary="测试摘要",
            markdown="测试正文",
        )
        with self.assertRaises(ValueError):
            notify_with_wecom(payload, webhook_url="")


if __name__ == "__main__":
    unittest.main()
