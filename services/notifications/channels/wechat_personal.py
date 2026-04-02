from __future__ import annotations

from services.notifications.contracts import NotificationPayload


def _build_text(payload: NotificationPayload) -> str:
    text = f"{payload.title}\n\n{payload.summary}\n\n{payload.markdown}"
    if payload.link:
        text += f"\n\n详情: {payload.link}"
    return text


def send_wechat_personal_message(
    *,
    payload: NotificationPayload,
    to_user_name: str = "filehelper",
    hot_reload: bool = True,
    status_storage_dir: str = "itchat.pkl",
    enable_cmd_qr: bool = False,
) -> dict:
    try:
        import itchat  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "未安装 itchat，无法启用个人微信通知。请先安装依赖：pip install itchat-uos"
        ) from exc

    target = str(to_user_name or "filehelper").strip() or "filehelper"
    text = _build_text(payload)
    itchat.auto_login(
        hotReload=bool(hot_reload),
        statusStorageDir=str(status_storage_dir or "itchat.pkl"),
        enableCmdQR=enable_cmd_qr,
    )
    send_ret = itchat.send(text, toUserName=target)
    return {
        "ok": True,
        "provider": "wechat_personal",
        "target": target,
        "result": send_ret,
        "experimental": True,
    }
