#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import backend.server as backend_server
import backend.http_server.config as http_server_config
from llm_gateway import normalize_model_name, normalize_temperature_for_model
from services.agent_service import run_trend_analysis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用大模型分析股票走势")
    parser.add_argument("--ts-code", required=True, help="股票代码，如 000001.SZ")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--lookback", type=int, default=120, help="分析回看交易日数量")
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    ts_code = args.ts_code.strip().upper()

    http_server_config.DB_PATH = Path(args.db_path).resolve()
    payload = run_trend_analysis(
        backend_server.build_agent_service_deps(),
        ts_code=ts_code,
        lookback=args.lookback,
        model=args.model,
        temperature=args.temperature,
    )
    features = payload.get("features") or {}

    print(f"股票: {ts_code} {features.get('name', '')}")
    print(
        f"区间: {(features.get('date_range') or {}).get('start', '-')} ~ "
        f"{(features.get('date_range') or {}).get('end', '-')}"
    )
    print("调用大模型分析中...\n")
    print(f"实际模型: {payload.get('used_model') or args.model}\n")
    print(payload.get("analysis_markdown") or payload.get("analysis") or "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
