#!/usr/bin/env python3
from __future__ import annotations

import os


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_csv(name: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in env_str(name).split(",") if item.strip())


TUSHARE_TOKEN = env_str("TUSHARE_TOKEN")
BACKEND_ADMIN_TOKEN = env_str("BACKEND_ADMIN_TOKEN")
BACKEND_ALLOWED_ORIGINS = env_csv("BACKEND_ALLOWED_ORIGINS")


def resolve_tushare_token(token: str = "") -> str:
    resolved = (token or "").strip() or TUSHARE_TOKEN
    if resolved:
        return resolved
    raise RuntimeError("缺少 TUSHARE_TOKEN，请在环境变量或 --token 参数中提供。")
