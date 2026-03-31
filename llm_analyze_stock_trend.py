#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import db_compat as sqlite3
import statistics
from pathlib import Path

from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model


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


def query_prices(db_path: str, ts_code: str, lookback: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT p.trade_date, p.open, p.high, p.low, p.close, p.pct_chg, p.vol, p.amount, s.name
            FROM stock_daily_prices p
            LEFT JOIN stock_codes s ON s.ts_code = p.ts_code
            WHERE p.ts_code = ?
            ORDER BY p.trade_date DESC
            LIMIT ?
            """,
            (ts_code, lookback),
        ).fetchall()
    finally:
        conn.close()
    data = [dict(r) for r in rows]
    data.reverse()
    return data


def safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def calc_ma(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def build_features(rows: list[dict]) -> dict:
    closes = [safe_float(r["close"]) for r in rows if safe_float(r["close"]) is not None]
    pct_chg = [safe_float(r["pct_chg"]) for r in rows if safe_float(r["pct_chg"]) is not None]
    vols = [safe_float(r["vol"]) for r in rows if safe_float(r["vol"]) is not None]

    if len(closes) < 2:
        raise ValueError("数据不足，至少需要2条日线")

    first_close = closes[0]
    last_close = closes[-1]
    total_return = (last_close - first_close) / first_close * 100 if first_close else None
    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)

    daily_returns = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev:
            daily_returns.append((curr - prev) / prev)

    vol_annualized = None
    if len(daily_returns) >= 2:
        vol_annualized = statistics.pstdev(daily_returns) * math.sqrt(252) * 100

    latest = rows[-1]
    latest_close = safe_float(latest["close"])

    return {
        "name": latest.get("name") or "",
        "samples": len(rows),
        "date_range": {
            "start": rows[0]["trade_date"],
            "end": rows[-1]["trade_date"],
        },
        "latest": {
            "trade_date": latest["trade_date"],
            "close": latest_close,
            "pct_chg": safe_float(latest["pct_chg"]),
            "vol": safe_float(latest["vol"]),
        },
        "trend_metrics": {
            "total_return_pct": total_return,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "distance_to_ma20_pct": ((latest_close - ma20) / ma20 * 100) if (latest_close and ma20) else None,
            "annualized_volatility_pct": vol_annualized,
            "avg_daily_pct_chg": (sum(pct_chg) / len(pct_chg)) if pct_chg else None,
            "avg_volume": (sum(vols) / len(vols)) if vols else None,
        },
        "recent_bars": rows[-20:],
    }


def call_llm(model: str, temperature: float, ts_code: str, features: dict):
    system_prompt = (
        "你是专业的A股量化研究助手。"
        "请基于给定特征做趋势分析，输出要客观，明确不确定性。"
        "不要编造不存在的数据。"
    )
    user_prompt = (
        f"请分析股票 {ts_code} 的走势。\n"
        "请按以下结构输出：\n"
        "1) 趋势判断（上涨/震荡/下跌）\n"
        "2) 置信度（0-100）\n"
        "3) 依据（3-5条）\n"
        "4) 风险点（2-4条）\n"
        "5) 未来5-20个交易日的观察要点\n"
        "6) 免责声明（非投资建议）\n\n"
        f"输入特征JSON：\n{json.dumps(features, ensure_ascii=False)}"
    )
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=120,
        max_retries=3,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    ts_code = args.ts_code.strip().upper()
    rows = query_prices(args.db_path, ts_code, args.lookback)
    if not rows:
        raise SystemExit(f"未找到 {ts_code} 的日线数据，请先入库后重试。")
    features = build_features(rows)

    print(f"股票: {ts_code} {features['name']}")
    print(f"区间: {features['date_range']['start']} ~ {features['date_range']['end']}")
    print("调用大模型分析中...\n")

    result = call_llm(model=args.model, temperature=args.temperature, ts_code=ts_code, features=features)
    print(f"实际模型: {result.used_model}\n")
    print(result.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
