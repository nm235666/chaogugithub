#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import re
import db_compat as sqlite3
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from market_calendar import DEFAULT_TOKEN, resolve_trade_date
from realtime_streams import publish_app_event

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取所有上市股票新浪分钟线并入库(稳健版)")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_minline", help="分钟线表名")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token，用于按交易日历解析默认日期")
    parser.add_argument("--trade-date", default="", help="交易日 YYYYMMDD，默认北京时间今天")

    # 并发与限速
    parser.add_argument("--workers", type=int, default=10, help="初始并发线程数")
    parser.add_argument("--min-workers", type=int, default=4, help="自适应最小线程数")
    parser.add_argument("--max-workers", type=int, default=16, help="自适应最大线程数")
    parser.add_argument("--timeout", type=float, default=12.0, help="单请求超时秒数")
    parser.add_argument("--retry", type=int, default=2, help="单股票单轮内重试次数")
    parser.add_argument("--base-backoff", type=float, default=0.4, help="指数退避基数秒")
    parser.add_argument("--sleep-min-ms", type=int, default=10, help="请求后最小随机休眠毫秒")
    parser.add_argument("--sleep-max-ms", type=int, default=30, help="请求后最大随机休眠毫秒")

    # 范围与断点
    parser.add_argument("--max-stocks", type=int, default=0, help="最多抓取多少只，0=全部")
    parser.add_argument("--resume-from", default="", help="从某个 ts_code 开始(含)")
    parser.add_argument(
        "--checkpoint-file",
        default=str(Path(__file__).resolve().parent / ".minline_fetch_checkpoint.json"),
        help="断点文件",
    )
    parser.add_argument("--reset-checkpoint", action="store_true", help="忽略并重置断点文件")

    # 批次/轮次/保护
    parser.add_argument("--batch-size", type=int, default=300, help="每批股票数")
    parser.add_argument("--max-rounds", type=int, default=4, help="失败队列最大补抓轮次")
    parser.add_argument("--max-fail-per-stock", type=int, default=8, help="单股票累计最大失败次数")
    parser.add_argument(
        "--stagnation-rounds",
        type=int,
        default=2,
        help="失败队列连续未收敛轮数阈值(第三层保护)",
    )

    # 质量校验
    parser.add_argument("--quality-min-rows", type=int, default=200, help="分钟线最少条数")
    parser.add_argument("--quality-max-rows", type=int, default=260, help="分钟线最多条数")

    return parser.parse_args()

def ts_to_sina_symbol(ts_code: str) -> str:
    code = ts_code.strip().upper()
    symbol, exch = code.split(".", 1)
    if exch == "SH":
        return f"sh{symbol}"
    if exch == "SZ":
        return f"sz{symbol}"
    if exch == "BJ":
        return f"bj{symbol}"
    raise ValueError(f"不支持的交易所: {exch}")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            minute_time TEXT NOT NULL,
            price REAL,
            avg_price REAL,
            volume REAL,
            total_volume REAL,
            source TEXT DEFAULT 'sina',
            PRIMARY KEY (ts_code, trade_date, minute_time),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_td_time ON {table_name}(trade_date, minute_time)"
    )
    conn.commit()


def load_listed_codes(conn: sqlite3.Connection, max_stocks: int, resume_from: str) -> list[str]:
    sql = "SELECT ts_code FROM stock_codes WHERE list_status='L'"
    params: list[object] = []
    if resume_from:
        sql += " AND ts_code >= ?"
        params.append(resume_from)
    sql += " ORDER BY ts_code"
    if max_stocks and max_stocks > 0:
        sql += " LIMIT ?"
        params.append(max_stocks)
    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


def fetch_sina_raw(sina_symbol: str, timeout: float) -> list[dict]:
    callback = f"var t1{sina_symbol}="
    qs = urllib.parse.urlencode({"symbol": sina_symbol, "callback": callback, "dpc": "1"})
    url = f"https://quotes.sina.cn/cn/api/openapi.php/CN_MinlineService.getMinlineData?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": f"https://finance.sina.com.cn/realstock/company/{sina_symbol}/nc.shtml",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        text = resp.read().decode("utf-8", errors="ignore")

    m = re.match(r"var\s+t1\w+=\((.*)\);?$", text)
    if not m:
        raise RuntimeError("响应格式异常")
    obj = json.loads(m.group(1))
    result = obj.get("result", {})
    status = result.get("status", {})
    if status.get("code") != 0:
        raise RuntimeError(f"接口错误: {status}")
    data = result.get("data", [])
    if not isinstance(data, list):
        raise RuntimeError("data 不是数组")
    return data


def classify_error(err_msg: str) -> tuple[str, bool]:
    msg = (err_msg or "").lower()

    # 永久失败: 参数/代码本身问题，继续重试意义很小
    permanent_patterns = [
        "不支持的交易所",
        "http error 404",
        "invalid",
        "bad request",
    ]
    if any(p in msg for p in permanent_patterns):
        return "permanent", False

    # 可重试失败: 网络、限流、超时、服务端波动
    retryable_patterns = [
        "timed out",
        "timeout",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "http error 429",  # 限流
        "http error 456",  # 新浪限流
        "http error 500",
        "http error 502",
        "http error 503",
        "http error 504",
        "响应格式异常",
        "接口错误",
    ]
    if any(p in msg for p in retryable_patterns):
        return "retryable", True

    # 默认按可重试处理（更保守）
    return "unknown", True


def fetch_one(
    ts_code: str,
    timeout: float,
    retry: int,
    base_backoff: float,
    sleep_min_ms: int,
    sleep_max_ms: int,
):
    sina_symbol = ts_to_sina_symbol(ts_code)
    last_err = None
    for attempt in range(retry + 1):
        try:
            data = fetch_sina_raw(sina_symbol, timeout)
            if sleep_max_ms > 0:
                time.sleep(
                    random.uniform(max(0, sleep_min_ms), max(sleep_min_ms, sleep_max_ms))
                    / 1000.0
                )
            return ts_code, data, None, "ok"
        except Exception as exc:  # noqa: PERF203
            last_err = str(exc)
            if attempt < retry:
                backoff = base_backoff * (2**attempt) + random.uniform(0, 0.2)
                time.sleep(backoff)
    err_type, _ = classify_error(last_err or "")
    return ts_code, [], last_err or "unknown error", err_type


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> None:
    if not rows:
        return
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, trade_date, minute_time, price, avg_price, volume, total_volume, source
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date, minute_time) DO UPDATE SET
        price=excluded.price,
        avg_price=excluded.avg_price,
        volume=excluded.volume,
        total_volume=excluded.total_volume,
        source=excluded.source
    """
    conn.executemany(sql, rows)


def make_rows(ts_code: str, trade_date: str, data: list[dict]) -> list[tuple]:
    rows: list[tuple] = []
    for r in data:
        rows.append(
            (
                ts_code,
                trade_date,
                str(r.get("m", "")),
                float(r.get("p")) if r.get("p") not in (None, "") else None,
                float(r.get("avg_p")) if r.get("avg_p") not in (None, "") else None,
                float(r.get("v")) if r.get("v") not in (None, "") else None,
                float(r.get("tot_v")) if r.get("tot_v") not in (None, "") else None,
                "sina",
            )
        )
    return rows


def save_checkpoint(
    path: Path,
    trade_date: str,
    round_idx: int,
    remaining_in_round: list[str],
    next_retry_queue: list[str],
    fail_counts: dict[str, int],
    stats: dict,
) -> None:
    payload = {
        "trade_date": trade_date,
        "round_idx": round_idx,
        "remaining_in_round": remaining_in_round,
        "next_retry_queue": next_retry_queue,
        "fail_counts": fail_counts,
        "stats": stats,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def load_checkpoint(path: Path, trade_date: str):
    if not path.exists():
        return None
    obj = json.loads(path.read_text(encoding="utf-8"))
    if obj.get("trade_date") != trade_date:
        return None
    return obj


def adjust_speed(
    batch_fail_rate: float,
    workers: int,
    min_workers: int,
    max_workers: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
) -> tuple[int, int, int]:
    # 自适应并发 + 限速
    if batch_fail_rate >= 0.30:
        workers = max(min_workers, workers - 2)
        sleep_min_ms = min(300, int(sleep_min_ms * 1.4) + 1)
        sleep_max_ms = min(1000, int(sleep_max_ms * 1.6) + 2)
    elif batch_fail_rate >= 0.15:
        workers = max(min_workers, workers - 1)
        sleep_min_ms = min(250, int(sleep_min_ms * 1.2) + 1)
        sleep_max_ms = min(800, int(sleep_max_ms * 1.25) + 2)
    elif batch_fail_rate <= 0.03:
        workers = min(max_workers, workers + 1)
        sleep_min_ms = max(3, int(sleep_min_ms * 0.9))
        sleep_max_ms = max(sleep_min_ms + 1, int(sleep_max_ms * 0.9))
    return workers, sleep_min_ms, sleep_max_ms


def process_batch(
    batch_codes: list[str],
    trade_date: str,
    workers: int,
    timeout: float,
    retry: int,
    base_backoff: float,
    sleep_min_ms: int,
    sleep_max_ms: int,
    quality_min_rows: int,
    quality_max_rows: int,
):
    success_rows: list[tuple] = []
    retry_queue: list[tuple[str, str]] = []  # (ts_code, reason)
    permanent_fails: list[tuple[str, str]] = []
    ok_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(
                fetch_one,
                ts,
                timeout,
                retry,
                base_backoff,
                sleep_min_ms,
                sleep_max_ms,
            )
            for ts in batch_codes
        ]
        for fut in as_completed(futures):
            ts_code, data, err, status = fut.result()
            if err is None:
                row_n = len(data)
                if row_n < quality_min_rows or row_n > quality_max_rows:
                    # 质量校验失败 -> 二次补抓队列
                    retry_queue.append((ts_code, f"quality({row_n})"))
                    fail_count += 1
                    print(f"  {ts_code}: LOW_QUALITY rows={row_n}")
                else:
                    ok_count += 1
                    success_rows.extend(make_rows(ts_code, trade_date, data))
                    print(f"  {ts_code}: ok rows={row_n}")
            else:
                kind, retryable = classify_error(err)
                fail_count += 1
                if retryable:
                    retry_queue.append((ts_code, f"{kind}:{err}"))
                    print(f"  {ts_code}: RETRY {kind} {err}")
                else:
                    permanent_fails.append((ts_code, f"{kind}:{err}"))
                    print(f"  {ts_code}: PERM_FAIL {kind} {err}")

    batch_fail_rate = fail_count / max(1, len(batch_codes))
    return {
        "success_rows": success_rows,
        "retry_queue": retry_queue,
        "permanent_fails": permanent_fails,
        "ok_count": ok_count,
        "fail_count": fail_count,
        "batch_fail_rate": batch_fail_rate,
    }


def main() -> int:
    args = parse_args()
    trade_date = resolve_trade_date(args.trade_date, args.token)
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    checkpoint_path = Path(args.checkpoint_file).resolve()
    if args.reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        all_codes = load_listed_codes(conn, args.max_stocks, args.resume_from.strip())
        total_codes = len(all_codes)
        if total_codes == 0:
            print("无可抓取股票。")
            return 0

        # 断点恢复
        cp = load_checkpoint(checkpoint_path, trade_date)
        if cp:
            round_idx = int(cp.get("round_idx", 1))
            current_queue = list(cp.get("remaining_in_round", []))
            pending_next_round = list(cp.get("next_retry_queue", []))
            fail_counts = {k: int(v) for k, v in cp.get("fail_counts", {}).items()}
            stats = dict(cp.get("stats", {}))
            print(
                f"从断点恢复: round={round_idx}, remaining={len(current_queue)}, "
                f"next_retry={len(pending_next_round)}"
            )
        else:
            round_idx = 1
            current_queue = list(all_codes)
            pending_next_round = []
            fail_counts: dict[str, int] = {}
            stats = {
                "ok": 0,
                "perm_fail": 0,
                "retry_fail": 0,
                "rows": 0,
            }

        workers = max(args.min_workers, min(args.max_workers, args.workers))
        sleep_min_ms = args.sleep_min_ms
        sleep_max_ms = args.sleep_max_ms

        previous_retry_size = None
        stagnation_count = 0

        while round_idx <= args.max_rounds:
            if not current_queue:
                # 用上一轮失败队列进入下一轮
                if not pending_next_round:
                    break
                current_queue = pending_next_round
                pending_next_round = []
                round_idx += 1
                continue

            print(
                f"\n=== ROUND {round_idx}/{args.max_rounds} === "
                f"queue={len(current_queue)} workers={workers} sleep={sleep_min_ms}-{sleep_max_ms}ms"
            )

            next_round_retry: list[str] = list(pending_next_round)
            pending_next_round = []

            for start in range(0, len(current_queue), args.batch_size):
                batch_codes = current_queue[start : start + args.batch_size]
                print(
                    f"\nBATCH {start // args.batch_size + 1}/"
                    f"{(len(current_queue) + args.batch_size - 1) // args.batch_size} "
                    f"size={len(batch_codes)}"
                )

                batch_result = process_batch(
                    batch_codes=batch_codes,
                    trade_date=trade_date,
                    workers=workers,
                    timeout=args.timeout,
                    retry=args.retry,
                    base_backoff=args.base_backoff,
                    sleep_min_ms=sleep_min_ms,
                    sleep_max_ms=sleep_max_ms,
                    quality_min_rows=args.quality_min_rows,
                    quality_max_rows=args.quality_max_rows,
                )

                # 分批落库
                if batch_result["success_rows"]:
                    upsert_rows(conn, args.table_name, batch_result["success_rows"])
                    conn.commit()
                    publish_app_event(
                        event="minline_batch_update",
                        payload={
                            "trade_date": trade_date,
                            "table": args.table_name,
                            "ok_count": int(batch_result["ok_count"]),
                            "rows_written": int(len(batch_result["success_rows"])),
                            "round": int(round_idx),
                            "batch_start": int(start),
                            "batch_size": int(len(batch_codes)),
                        },
                        producer="fetch_sina_minline_all_listed.py",
                    )

                stats["ok"] += batch_result["ok_count"]
                stats["rows"] += len(batch_result["success_rows"])

                for ts_code, reason in batch_result["permanent_fails"]:
                    stats["perm_fail"] += 1
                    print(f"  -> permanent {ts_code}: {reason}")

                for ts_code, reason in batch_result["retry_queue"]:
                    fail_counts[ts_code] = fail_counts.get(ts_code, 0) + 1
                    if fail_counts[ts_code] >= args.max_fail_per_stock:
                        stats["perm_fail"] += 1
                        print(f"  -> giveup {ts_code}: fail_count={fail_counts[ts_code]} reason={reason}")
                    else:
                        next_round_retry.append(ts_code)
                        stats["retry_fail"] += 1

                # 自适应降速/提速
                workers, sleep_min_ms, sleep_max_ms = adjust_speed(
                    batch_fail_rate=batch_result["batch_fail_rate"],
                    workers=workers,
                    min_workers=args.min_workers,
                    max_workers=args.max_workers,
                    sleep_min_ms=sleep_min_ms,
                    sleep_max_ms=sleep_max_ms,
                )

                # 保存断点(当前批之后剩余 + 下轮失败队列)
                remaining_in_round = current_queue[start + args.batch_size :]
                save_checkpoint(
                    checkpoint_path,
                    trade_date,
                    round_idx,
                    remaining_in_round,
                    next_round_retry,
                    fail_counts,
                    stats,
                )

            retry_size = len(next_round_retry)
            print(f"ROUND {round_idx} done: next_retry={retry_size}")
            publish_app_event(
                event="minline_round_update",
                payload={
                    "trade_date": trade_date,
                    "table": args.table_name,
                    "round": int(round_idx),
                    "next_retry": int(retry_size),
                    "ok_total": int(stats.get("ok", 0)),
                    "rows_total": int(stats.get("rows", 0)),
                },
                producer="fetch_sina_minline_all_listed.py",
            )

            # 三层保护之“收敛监控”
            if previous_retry_size is not None and retry_size >= previous_retry_size:
                stagnation_count += 1
            else:
                stagnation_count = 0
            previous_retry_size = retry_size

            if stagnation_count >= args.stagnation_rounds:
                print(
                    f"触发收敛保护: 失败队列连续{stagnation_count}轮未下降，提前停止。"
                )
                break

            round_idx += 1
            current_queue = list(dict.fromkeys(next_round_retry))  # 去重保序

        table_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        covered_codes = conn.execute(
            f"SELECT COUNT(DISTINCT ts_code) FROM {args.table_name} WHERE trade_date=?",
            (trade_date,),
        ).fetchone()[0]
        listed_total = conn.execute("SELECT COUNT(*) FROM stock_codes WHERE list_status='L'").fetchone()[0]
        coverage = (covered_codes / listed_total * 100) if listed_total else 0

        print(
            "\n完成: "
            f"ok={stats.get('ok', 0)}, perm_fail={stats.get('perm_fail', 0)}, "
            f"retry_fail_events={stats.get('retry_fail', 0)}, rows_written={stats.get('rows', 0)}, "
            f"table_rows={table_rows}, covered_codes={covered_codes}/{listed_total} ({coverage:.2f}%)"
        )
        publish_app_event(
            event="minline_full_update",
            payload={
                "trade_date": trade_date,
                "table": args.table_name,
                "ok_total": int(stats.get("ok", 0)),
                "perm_fail_total": int(stats.get("perm_fail", 0)),
                "rows_written_total": int(stats.get("rows", 0)),
                "covered_codes": int(covered_codes),
                "listed_total": int(listed_total),
                "coverage_pct": float(round(coverage, 4)),
            },
            producer="fetch_sina_minline_all_listed.py",
        )

        # 成功结束删除断点
        if checkpoint_path.exists():
            checkpoint_path.unlink()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
