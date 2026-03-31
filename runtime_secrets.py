#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from pathlib import Path


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_csv(name: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in env_str(name).split(",") if item.strip())


def _load_token_from_file() -> str:
    """尝试从文件加载 Tushare Token"""
    # 尝试多个可能的路径
    paths = [
        Path.home() / ".tushare_token",           # ~/.tushare_token
        Path(__file__).parent / "config" / "tushare_token.txt",  # config/tushare_token.txt
    ]
    token_pattern = re.compile(r"\b[a-fA-F0-9]{32,}\b")
    for p in paths:
        if p.exists():
            text = p.read_text(encoding="utf-8", errors="ignore")
            # 1) 优先读取首个非注释、非空行
            for line in text.splitlines():
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                return raw
            # 2) 兜底：从整文件（含注释）提取形如 token 的长 hex 串
            match = token_pattern.search(text)
            if match:
                return match.group(0)
    return ""


TUSHARE_TOKEN = env_str("TUSHARE_TOKEN") or _load_token_from_file()
BACKEND_ADMIN_TOKEN = env_str("BACKEND_ADMIN_TOKEN")
BACKEND_ALLOWED_ORIGINS = env_csv("BACKEND_ALLOWED_ORIGINS")


def resolve_tushare_token(token: str = "") -> str:
    resolved = (token or "").strip() or TUSHARE_TOKEN
    if resolved:
        return resolved
    raise RuntimeError(
        "缺少 TUSHARE_TOKEN。请通过以下任一方式提供：\n"
        "1. 设置环境变量: export TUSHARE_TOKEN=your_token\n"
        "2. 创建文件: echo 'your_token' > ~/.tushare_token\n"
        "3. 创建文件: echo 'your_token' > /home/zanbo/zanbotest/config/tushare_token.txt"
    )
