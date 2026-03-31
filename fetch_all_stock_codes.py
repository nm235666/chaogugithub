#!/usr/bin/env python3
"""
使用 Tushare 获取全部股票代码并导出 CSV。

用法:
  python fetch_all_stock_codes.py
  python fetch_all_stock_codes.py --output all_stock_codes.csv

可选环境变量:
  TUSHARE_TOKEN=你的token
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

# 自动加载项目本地依赖目录，避免系统环境缺包
LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts


DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="获取全部股票代码")
    parser.add_argument(
        "--output",
        default="all_stock_codes.csv",
        help="导出的 CSV 文件名 (默认: all_stock_codes.csv)",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        token = resolve_tushare_token(args.token or os.getenv("TUSHARE_TOKEN", DEFAULT_TOKEN))
    except RuntimeError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1

    # 直接传 token，避免 tushare 在家目录写 tk.csv
    pro = ts.pro_api(token)

    # list_status:
    # L = 上市, D = 退市, P = 暂停上市
    df = pro.stock_basic(
        exchange="",
        list_status="L,D,P",
        fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status",
    )

    if df is None or df.empty:
        print("未获取到数据，请检查 token 或网络连接。", file=sys.stderr)
        return 2

    df = df.sort_values(by=["ts_code"]).reset_index(drop=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"获取成功，共 {len(df)} 条记录。")
    print(f"已导出: {args.output}")
    print("仅股票代码示例(前10条):")
    print(df["ts_code"].head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
