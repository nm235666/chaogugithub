from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NotificationPayload:
    title: str
    summary: str
    markdown: str
    subject_key: str = ""
    link: str = ""
