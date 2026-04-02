from __future__ import annotations

import json
import urllib.request

from services.notifications.contracts import NotificationPayload


def send_wecom_markdown(*, webhook_url: str, payload: NotificationPayload, timeout_s: int = 8) -> dict:
    webhook = str(webhook_url or "").strip()
    if not webhook:
        raise ValueError("缺少企业微信 webhook_url")
    text = f"## {payload.title}\n\n{payload.summary}\n\n{payload.markdown}"
    if payload.link:
        text += f"\n\n[查看详情]({payload.link})"
    body = {"msgtype": "markdown", "markdown": {"content": text}}
    request = urllib.request.Request(
        webhook,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        raw = response.read().decode("utf-8", errors="ignore")
    try:
        result = json.loads(raw)
    except Exception:
        result = {"raw": raw}
    ok = str(result.get("errcode", 0)) in {"0", "0.0"}
    return {"ok": ok, "provider": "wecom", "result": result}
