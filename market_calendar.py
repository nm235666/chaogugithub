#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token


DEFAULT_TOKEN = TUSHARE_TOKEN


def beijing_today() -> str:
    cn_tz = timezone(timedelta(hours=8))
    return datetime.now(cn_tz).strftime("%Y%m%d")


def recent_open_trade_dates(token: str, count: int = 1, end_date: str = "") -> list[str]:
    count = max(int(count), 1)
    end_date = (end_date or "").strip() or beijing_today()
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    # 留足窗口，覆盖春节/国庆等长假
    start_dt = end_dt - timedelta(days=max(40, count * 20))

    pro = ts.pro_api(resolve_tushare_token(token or DEFAULT_TOKEN))
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_dt.strftime("%Y%m%d"),
        end_date=end_date,
        is_open="1",
        fields="cal_date",
    )
    if cal is None or cal.empty:
        raise RuntimeError(f"未获取到交易日历: end_date={end_date}")

    dates = sorted(str(x).strip() for x in cal["cal_date"].tolist() if str(x).strip())
    if not dates:
        raise RuntimeError(f"交易日历为空: end_date={end_date}")
    return dates[-count:]


def resolve_trade_date(trade_date: str, token: str) -> str:
    raw = (trade_date or "").strip()
    if raw:
        return raw
    return recent_open_trade_dates(token=token, count=1)[-1]


def main() -> int:
    parser = argparse.ArgumentParser(description="按交易日历返回最近交易日")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--count", type=int, default=1, help="返回最近几个交易日")
    parser.add_argument("--end-date", default="", help="截止日期 YYYYMMDD，默认北京时间今天")
    args = parser.parse_args()

    for d in recent_open_trade_dates(token=args.token, count=args.count, end_date=args.end_date):
        print(d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
