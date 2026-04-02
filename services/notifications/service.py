from __future__ import annotations

import os

from .channels import send_wechat_personal_message, send_wecom_markdown
from .contracts import NotificationPayload


def notify_with_wecom(payload: NotificationPayload, webhook_url: str | None = None) -> dict:
    webhook = webhook_url or os.getenv("WECOM_BOT_WEBHOOK", "")
    return send_wecom_markdown(webhook_url=webhook, payload=payload)


def notify_with_wechat_personal(
    payload: NotificationPayload,
    *,
    to_user_name: str | None = None,
    hot_reload: bool = True,
    status_storage_dir: str | None = None,
    enable_cmd_qr: bool = False,
) -> dict:
    target = to_user_name or os.getenv("WECHAT_PERSONAL_TO_USER", "filehelper")
    storage = status_storage_dir or os.getenv("WECHAT_PERSONAL_STATUS_FILE", "itchat.pkl")
    return send_wechat_personal_message(
        payload=payload,
        to_user_name=target,
        hot_reload=hot_reload,
        status_storage_dir=storage,
        enable_cmd_qr=enable_cmd_qr,
    )


def notify(payload: NotificationPayload, *, channel: str = "wecom") -> dict:
    normalized = str(channel or "wecom").strip().lower()
    if normalized == "wecom":
        return notify_with_wecom(payload)
    if normalized in {"wechat_personal", "itchat"}:
        return notify_with_wechat_personal(payload)
    raise ValueError(f"不支持的通知通道: {channel}")


def build_notification_payload(
    *,
    title: str,
    summary: str,
    markdown: str,
    subject_key: str = "",
    link: str = "",
) -> NotificationPayload:
    return NotificationPayload(
        title=title.strip(),
        summary=summary.strip(),
        markdown=markdown.strip(),
        subject_key=subject_key.strip(),
        link=link.strip(),
    )
