from __future__ import annotations

from .wechat_personal import send_wechat_personal_message
from .wecom import send_wecom_markdown

__all__ = ["send_wecom_markdown", "send_wechat_personal_message"]
