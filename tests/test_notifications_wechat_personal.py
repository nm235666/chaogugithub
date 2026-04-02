#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.notifications import build_notification_payload, notify_with_wechat_personal


class NotificationsWechatPersonalTest(unittest.TestCase):
    def test_notify_with_wechat_personal_without_itchat_dependency(self):
        payload = build_notification_payload(
            title="测试标题",
            summary="测试摘要",
            markdown="测试正文",
        )
        with self.assertRaises(RuntimeError):
            notify_with_wechat_personal(payload, to_user_name="filehelper")


if __name__ == "__main__":
    unittest.main()
