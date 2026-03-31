#!/usr/bin/env python3
"""
回填公司治理画像到 company_governance。

当前整合数据源：
- top10_holders
- top10_floatholders
- stk_holdernumber
- stk_holdertrade
- pledge_stat
- stk_rewards

说明：
- 这是一张“研究画像表”，不是原始明细表
- 每只股票写入一条最新治理快照
"""

from __future__ import annotations

import argparse
import json
import math
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填公司治理画像到 company_governance")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="company_governance", help="目标表名")
    parser.add_argument("--ts-code", default="", help="仅处理单只股票，如 000001.SZ")
    parser.add_argument("--start-from", default="", help="从指定 ts_code 开始续跑")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多处理多少只股票")
    parser.add_argument("--pause", type=float, default=0.12, help="每只股票请求后暂停秒数")
    parser.add_argument("--retry", type=int, default=3, help="单接口失败最大重试次数")
    parser.add_argument("--all-status", action="store_true", help="抓全部状态股票")
    parser.add_argument("--missing-only", action="store_true", help="仅处理当前缺失治理快照的股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            asof_date TEXT NOT NULL,
            holder_structure_json TEXT,
            board_structure_json TEXT,
            mgmt_change_json TEXT,
            incentive_plan_json TEXT,
            governance_score REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, asof_date),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_asof ON {table_name}(asof_date)"
    )
    conn.commit()


def load_codes(
    conn: sqlite3.Connection,
    table_name: str,
    ts_code: str,
    start_from: str,
    limit_stocks: int,
    all_status: bool,
    missing_only: bool,
) -> list[str]:
    if ts_code.strip():
        return [ts_code.strip().upper()]
    where = []
    params: list[object] = []
    if not all_status:
        where.append("list_status='L'")
    if missing_only:
        where.append(
            f"NOT EXISTS (SELECT 1 FROM {table_name} g WHERE g.ts_code = stock_codes.ts_code)"
        )
    if start_from.strip():
        where.append("ts_code >= ?")
        params.append(start_from.strip().upper())
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f" LIMIT {int(limit_stocks)}" if limit_stocks > 0 else ""
    sql = f"SELECT ts_code FROM stock_codes{where_sql} ORDER BY ts_code{limit_sql}"
    return [row[0] for row in conn.execute(sql, params).fetchall()]


def fetch_with_retry(fetch_fn, retry: int):
    last_exc = None
    for attempt in range(retry + 1):
        try:
            return fetch_fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retry:
                time.sleep(1.5 * (2**attempt))
                continue
            raise last_exc


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def compact_json(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def latest_end_date(df) -> str:
    if df is None or df.empty or "end_date" not in df.columns:
        return ""
    vals = [normalize_text(v) for v in df["end_date"].tolist() if normalize_text(v)]
    return max(vals) if vals else ""


def latest_ann_date(df) -> str:
    if df is None or df.empty or "ann_date" not in df.columns:
        return ""
    vals = [normalize_text(v) for v in df["ann_date"].tolist() if normalize_text(v)]
    return max(vals) if vals else ""


def safe_float(value):
    try:
        if value is None:
            return None
        num = float(value)
        if math.isnan(num):
            return None
        return num
    except Exception:
        return None


def build_governance_snapshot(pro, ts_code: str, retry: int) -> dict:
    top10 = fetch_with_retry(lambda: pro.top10_holders(ts_code=ts_code), retry)
    top10_float = fetch_with_retry(lambda: pro.top10_floatholders(ts_code=ts_code), retry)
    holder_num = fetch_with_retry(lambda: pro.stk_holdernumber(ts_code=ts_code), retry)
    holder_trade = fetch_with_retry(lambda: pro.stk_holdertrade(ts_code=ts_code, limit=20), retry)
    pledge_stat = fetch_with_retry(lambda: pro.pledge_stat(ts_code=ts_code), retry)
    rewards = fetch_with_retry(lambda: pro.stk_rewards(ts_code=ts_code), retry)

    top10_period = latest_end_date(top10)
    top10_float_period = latest_end_date(top10_float)
    holder_num_period = latest_end_date(holder_num)
    rewards_period = latest_end_date(rewards)

    if top10 is not None and not top10.empty and top10_period:
        top10 = top10[top10["end_date"] == top10_period].copy()
    if top10_float is not None and not top10_float.empty and top10_float_period:
        top10_float = top10_float[top10_float["end_date"] == top10_float_period].copy()
    if holder_num is not None and not holder_num.empty and holder_num_period:
        holder_num = holder_num[holder_num["end_date"] == holder_num_period].copy()
    if rewards is not None and not rewards.empty and rewards_period:
        rewards = rewards[rewards["end_date"] == rewards_period].copy()

    top10_list = []
    top1_ratio = None
    top10_ratio_sum = 0.0
    if top10 is not None and not top10.empty:
        for idx, row in enumerate(top10.to_dict("records"), start=1):
            ratio = safe_float(row.get("hold_ratio")) or 0.0
            if idx == 1:
                top1_ratio = ratio
            top10_ratio_sum += ratio
            top10_list.append(
                {
                    "rank": idx,
                    "holder_name": normalize_text(row.get("holder_name")),
                    "holder_type": normalize_text(row.get("holder_type")),
                    "hold_amount": safe_float(row.get("hold_amount")),
                    "hold_ratio": ratio,
                    "hold_change": safe_float(row.get("hold_change")),
                }
            )

    top10_float_list = []
    if top10_float is not None and not top10_float.empty:
        for idx, row in enumerate(top10_float.to_dict("records"), start=1):
            top10_float_list.append(
                {
                    "rank": idx,
                    "holder_name": normalize_text(row.get("holder_name")),
                    "holder_type": normalize_text(row.get("holder_type")),
                    "hold_amount": safe_float(row.get("hold_amount")),
                    "hold_ratio": safe_float(row.get("hold_ratio")),
                    "hold_change": safe_float(row.get("hold_change")),
                }
            )

    holder_num_latest = None
    if holder_num is not None and not holder_num.empty:
        row = holder_num.iloc[0].to_dict()
        holder_num_latest = {
            "ann_date": normalize_text(row.get("ann_date")),
            "end_date": normalize_text(row.get("end_date")),
            "holder_num": row.get("holder_num"),
        }

    pledge_latest = None
    pledge_ratio = 0.0
    if pledge_stat is not None and not pledge_stat.empty:
        row = pledge_stat.iloc[0].to_dict()
        pledge_ratio = safe_float(row.get("pledge_ratio")) or 0.0
        pledge_latest = {
            "end_date": normalize_text(row.get("end_date")),
            "pledge_count": row.get("pledge_count"),
            "unrest_pledge": safe_float(row.get("unrest_pledge")),
            "rest_pledge": safe_float(row.get("rest_pledge")),
            "total_share": safe_float(row.get("total_share")),
            "pledge_ratio": pledge_ratio,
        }

    mgmt_change_list = []
    if holder_trade is not None and not holder_trade.empty:
        for row in holder_trade.to_dict("records")[:20]:
            mgmt_change_list.append(
                {
                    "ann_date": normalize_text(row.get("ann_date")),
                    "holder_name": normalize_text(row.get("holder_name")),
                    "holder_type": normalize_text(row.get("holder_type")),
                    "direction": normalize_text(row.get("in_de")),
                    "change_vol": safe_float(row.get("change_vol")),
                    "change_ratio": safe_float(row.get("change_ratio")),
                    "after_ratio": safe_float(row.get("after_ratio")),
                    "avg_price": safe_float(row.get("avg_price")),
                }
            )

    rewards_list = []
    total_reward = 0.0
    if rewards is not None and not rewards.empty:
        for row in rewards.to_dict("records")[:50]:
            reward = safe_float(row.get("reward"))
            if reward is not None:
                total_reward += reward
            rewards_list.append(
                {
                    "name": normalize_text(row.get("name")),
                    "title": normalize_text(row.get("title")),
                    "reward": reward,
                    "hold_vol": safe_float(row.get("hold_vol")),
                    "ann_date": normalize_text(row.get("ann_date")),
                    "end_date": normalize_text(row.get("end_date")),
                }
            )

    asof_candidates = [
        top10_period,
        top10_float_period,
        holder_num_period,
        rewards_period,
        latest_ann_date(holder_trade),
        normalize_text(pledge_latest["end_date"]) if pledge_latest else "",
    ]
    asof_date = max([x for x in asof_candidates if x], default=utc_now()[:10].replace("-", ""))

    score = 70.0
    if top1_ratio is not None:
        if top1_ratio >= 50:
            score += 8
        elif top1_ratio < 15:
            score -= 5
    if top10_ratio_sum >= 70:
        score += 5
    if pledge_ratio >= 30:
        score -= 20
    elif pledge_ratio >= 10:
        score -= 8
    if holder_num_latest and holder_num_latest.get("holder_num") and holder_num_latest["holder_num"] < 50000:
        score += 2
    if rewards_list:
        score += 2
    score = max(0.0, min(100.0, score))

    return {
        "ts_code": ts_code,
        "asof_date": asof_date,
        "holder_structure_json": compact_json(
            {
                "top10_period": top10_period,
                "top10_holders": top10_list,
                "top10_ratio_sum": top10_ratio_sum if top10_list else None,
                "top1_ratio": top1_ratio,
                "top10_float_period": top10_float_period,
                "top10_floatholders": top10_float_list,
                "holder_num_latest": holder_num_latest,
                "pledge_stat_latest": pledge_latest,
            }
        ),
        "board_structure_json": compact_json(
            {
                "reward_period": rewards_period,
                "total_reward": total_reward if rewards_list else None,
                "members": rewards_list,
            }
        ),
        "mgmt_change_json": compact_json(
            {
                "recent_holder_trades": mgmt_change_list,
            }
        ),
        "incentive_plan_json": compact_json(
            {
                "status": "pending_extension",
                "note": "当前版本暂未接入股权激励原始接口，后续可扩展。",
            }
        ),
        "governance_score": score,
        "source": "tushare.governance_bundle",
        "update_time": utc_now(),
    }


def upsert_snapshot(conn: sqlite3.Connection, table_name: str, item: dict) -> int:
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, asof_date, holder_structure_json, board_structure_json, mgmt_change_json,
        incentive_plan_json, governance_score, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, asof_date) DO UPDATE SET
        holder_structure_json=excluded.holder_structure_json,
        board_structure_json=excluded.board_structure_json,
        mgmt_change_json=excluded.mgmt_change_json,
        incentive_plan_json=excluded.incentive_plan_json,
        governance_score=excluded.governance_score,
        source=excluded.source,
        update_time=excluded.update_time
    """
    conn.execute(
        sql,
        (
            item["ts_code"],
            item["asof_date"],
            item["holder_structure_json"],
            item["board_structure_json"],
            item["mgmt_change_json"],
            item["incentive_plan_json"],
            item["governance_score"],
            item["source"],
            item["update_time"],
        ),
    )
    conn.commit()
    return 1


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        codes = load_codes(
            conn,
            args.table_name,
            args.ts_code,
            args.start_from,
            args.limit_stocks,
            args.all_status,
            args.missing_only,
        )
        if not codes:
            print("没有可处理的股票。")
            return 0

        print(
            f"待处理股票数: {len(codes)}, all_status={args.all_status}, missing_only={args.missing_only}"
        )
        total = 0
        failed = 0
        for idx, ts_code in enumerate(codes, start=1):
            try:
                item = build_governance_snapshot(pro, ts_code, args.retry)
                upsert_snapshot(conn, args.table_name, item)
                total += 1
                print(
                    f"[{idx}/{len(codes)}] {ts_code}: ok asof={item['asof_date']} score={item['governance_score']}"
                )
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(codes)}] {ts_code}: 失败 -> {exc}", file=sys.stderr)
            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(f"完成: failed={failed}, upsert={total}, table_rows={final_rows}, db={db_path}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
