#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone


def add_common_cli_flags(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--limit", type=int, default=100, help="处理条数上限")
    parser.add_argument("--retry", type=int, default=2, help="失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.0, help="请求间隔秒数")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划，不落库")
    return parser


def now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_step(step: str, **kwargs) -> None:
    kv = " ".join(f"{k}={v}" for k, v in kwargs.items())
    if kv:
        print(f"[{now_utc_text()}] {step} {kv}")
    else:
        print(f"[{now_utc_text()}] {step}")
