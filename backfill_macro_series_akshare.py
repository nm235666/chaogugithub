#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import math
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import db_compat as sqlite3

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import pandas as pd
import akshare as ak


DEFAULT_CHINA_SPECS = [
    {
        "api": "macro_china_cpi_yearly",
        "freq": "M",
        "period_col": "日期",
        "publish_col": "日期",
        "item_col": "商品",
        "metrics": [
            ("今值", "current", "pct"),
            ("预测值", "forecast", "pct"),
            ("前值", "previous", "pct"),
        ],
    },
    {
        "api": "macro_china_ppi_yearly",
        "freq": "M",
        "period_col": "日期",
        "publish_col": "日期",
        "item_col": "商品",
        "metrics": [
            ("今值", "current", "pct"),
            ("预测值", "forecast", "pct"),
            ("前值", "previous", "pct"),
        ],
    },
    {
        "api": "macro_china_gdp_yearly",
        "freq": "Q",
        "period_col": "日期",
        "publish_col": "日期",
        "item_col": "商品",
        "metrics": [
            ("今值", "current", "pct"),
            ("预测值", "forecast", "pct"),
            ("前值", "previous", "pct"),
        ],
    },
    {
        "api": "macro_china_m2_yearly",
        "freq": "M",
        "period_col": "日期",
        "publish_col": "日期",
        "item_col": "商品",
        "metrics": [
            ("今值", "current", "pct"),
            ("预测值", "forecast", "pct"),
            ("前值", "previous", "pct"),
        ],
    },
    {
        "api": "macro_china_pmi_yearly",
        "freq": "M",
        "period_col": "日期",
        "publish_col": "日期",
        "item_col": "商品",
        "metrics": [
            ("今值", "current", "index"),
            ("预测值", "forecast", "index"),
            ("前值", "previous", "index"),
        ],
    },
    {
        "api": "macro_china_shrzgm",
        "freq": "M",
        "period_col": "月份",
        "publish_col": "",
        "item_col": "",
        "metrics": [
            ("社会融资规模增量", "social_financing_increment", "cny_100m"),
            ("其中-人民币贷款", "rmb_loans", "cny_100m"),
            ("其中-委托贷款外币贷款", "entrusted_foreign_loans", "cny_100m"),
            ("其中-委托贷款", "entrusted_loans", "cny_100m"),
            ("其中-信托贷款", "trust_loans", "cny_100m"),
            ("其中-未贴现银行承兑汇票", "undiscounted_bank_acceptance", "cny_100m"),
            ("其中-企业债券", "corporate_bonds", "cny_100m"),
            ("其中-非金融企业境内股票融资", "equity_financing", "cny_100m"),
        ],
    },
]

EXCLUDED_AUTO_APIS = {
    "macro_info_ws",
    "macro_cnbs",
    "macro_stock_finance",
    "macro_bank_china_interest_rate",
    "macro_rmb_deposit",
    "macro_rmb_loan",
}

PERIOD_CANDIDATES = ["日期", "时间", "月份", "月度", "季度", "date", "period", "month", "year"]
PUBLISH_CANDIDATES = ["发布日期", "公布时间", "公布日期", "发布日", "日期"]
ITEM_CANDIDATES = ["商品", "指标", "名称", "项目", "item", "name"]
METRIC_ALIASES = {
    "今值": "current",
    "现值": "current",
    "预测值": "forecast",
    "前值": "previous",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用 AKShare 回填宏观数据到 macro_series")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="macro_series", help="目标表名")
    parser.add_argument(
        "--apis",
        default="",
        help="要抓取的 AKShare 宏观接口，逗号分隔；留空则按 china + overseas 自动组合",
    )
    parser.add_argument("--max-apis", type=int, default=0, help="最多处理多少个接口，0=全部")
    parser.add_argument("--china-only", action="store_true", help="只抓中国宏观接口")
    parser.add_argument("--overseas-only", action="store_true", help="只抓海外宏观接口")
    parser.add_argument("--retry", type=int, default=2, help="单接口失败重试次数")
    parser.add_argument("--backoff", type=float, default=1.0, help="失败后的指数退避基数秒")
    parser.add_argument("--pause", type=float, default=0.2, help="每个接口之间暂停秒数")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            indicator_code TEXT NOT NULL,
            indicator_name TEXT,
            freq TEXT NOT NULL,
            period TEXT NOT NULL,
            value REAL,
            unit TEXT,
            source TEXT,
            publish_date TEXT,
            update_time TEXT,
            PRIMARY KEY (indicator_code, freq, period)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name}(period)")
    conn.commit()


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


def normalize_period(value, freq: str) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        d = value.date()
    elif isinstance(value, date):
        d = value
    else:
        text = str(value).strip()
        if not text:
            return ""
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) == 8:
            if freq == "Q":
                month = int(digits[4:6])
                quarter = (month - 1) // 3 + 1
                return f"{digits[:4]}Q{quarter}"
            if freq == "M":
                return digits[:6]
            return digits
        if len(digits) == 6:
            return digits if freq == "M" else digits + "01"
        return text
    if freq == "Q":
        quarter = (d.month - 1) // 3 + 1
        return f"{d.year}Q{quarter}"
    if freq == "M":
        return d.strftime("%Y%m")
    return d.strftime("%Y%m%d")


def normalize_publish_date(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    if len(digits) == 6:
        return digits + "01"
    return text


def slugify(text: str) -> str:
    value = str(text or "").strip().lower()
    value = value.replace("%", "pct")
    value = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "value"


def infer_freq(api_name: str, df: pd.DataFrame, period_col: str) -> str:
    lower = api_name.lower()
    if any(token in lower for token in ["quarterly", "_qoq", "_quarter", "_gdp", "employment_change_qoq"]):
        return "Q"
    if any(token in lower for token in ["weekly", "_week", "_api_crude_stock", "_eia_crude_rate", "_initial_jobless", "_rig_count"]):
        return "W"
    if any(token in lower for token in ["daily", "_bdi", "_bpi", "_bci", "_bcti", "_sox_index", "_lme_", "_sentiment"]):
        return "D"

    sample_values = []
    if period_col and period_col in df.columns:
        for value in df[period_col].head(20).tolist():
            if value is None:
                continue
            sample_values.append(str(value))
    if any("季度" in x or "Q" in x for x in sample_values):
        return "Q"
    if any(("年" in x and "月" in x) for x in sample_values):
        return "M"
    return "M"


def discover_overseas_api_names() -> list[str]:
    names: list[str] = []
    for name in dir(ak):
        if not name.startswith("macro_"):
            continue
        if name.startswith("macro_china"):
            continue
        if name in EXCLUDED_AUTO_APIS:
            continue
        fn = getattr(ak, name, None)
        if not callable(fn):
            continue
        try:
            sig = inspect.signature(fn)
        except Exception:
            continue
        required = [
            p for p in sig.parameters.values()
            if p.default is inspect._empty
            and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        ]
        if required:
            continue
        names.append(name)
    return sorted(names)


def build_auto_spec(api_name: str, df: pd.DataFrame) -> dict | None:
    if df is None or df.empty:
        return None

    period_col = next((c for c in PERIOD_CANDIDATES if c in df.columns), "")
    if not period_col:
        return None
    publish_col = next((c for c in PUBLISH_CANDIDATES if c in df.columns), "")
    item_col = next((c for c in ITEM_CANDIDATES if c in df.columns), "")

    metrics: list[tuple[str, str, str]] = []
    for col in df.columns:
        if col in {period_col, publish_col, item_col}:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if not series.notna().any():
            continue
        metric_code = METRIC_ALIASES.get(col, slugify(col))
        unit = "pct" if any(token in str(col) for token in ["率", "%", "pct"]) else ""
        metrics.append((col, metric_code, unit))

    if not metrics:
        return None

    return {
        "api": api_name,
        "freq": infer_freq(api_name, df, period_col),
        "period_col": period_col,
        "publish_col": publish_col,
        "item_col": item_col,
        "metrics": metrics,
    }


def build_rows_from_spec(spec: dict, df: pd.DataFrame) -> list[tuple]:
    rows: list[tuple] = []
    if df is None or df.empty:
        return rows

    period_col = spec["period_col"]
    publish_col = spec.get("publish_col", "")
    item_col = spec.get("item_col", "")
    update_time = now_utc_str()

    for _, row in df.iterrows():
        period = normalize_period(row.get(period_col), spec["freq"])
        if not period:
            continue
        publish_date = normalize_publish_date(row.get(publish_col)) if publish_col else ""
        item_name = str(row.get(item_col) or "").strip() if item_col else spec["api"]

        for raw_col, metric_code, unit in spec["metrics"]:
            value = safe_float(row.get(raw_col))
            if value is None:
                continue
            indicator_code = f"{spec['api']}.{metric_code}"
            indicator_name = f"{item_name}-{raw_col}" if item_name else raw_col
            rows.append(
                (
                    indicator_code,
                    indicator_name[:200],
                    spec["freq"],
                    period,
                    value,
                    unit,
                    f"akshare.{spec['api']}",
                    publish_date,
                    update_time,
                )
            )
    return rows


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        f"""
        INSERT INTO {table_name} (
            indicator_code, indicator_name, freq, period, value, unit, source, publish_date, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(indicator_code, freq, period) DO UPDATE SET
            indicator_name=excluded.indicator_name,
            value=excluded.value,
            unit=excluded.unit,
            source=excluded.source,
            publish_date=CASE
                WHEN COALESCE(excluded.publish_date, '') <> '' THEN excluded.publish_date
                ELSE {table_name}.publish_date
            END,
            update_time=excluded.update_time
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def fetch_with_retry(api_name: str, retry: int, backoff: float) -> pd.DataFrame:
    last_exc = None
    fn = getattr(ak, api_name)
    for attempt in range(retry + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retry:
                time.sleep(backoff * (2**attempt))
    raise last_exc  # type: ignore[misc]


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    china_map = {spec["api"]: spec for spec in DEFAULT_CHINA_SPECS}
    overseas_names = discover_overseas_api_names()

    if args.china_only and args.overseas_only:
        print("不能同时传 --china-only 和 --overseas-only")
        return 1

    if args.apis.strip():
        api_names = [x.strip() for x in args.apis.split(",") if x.strip()]
    else:
        api_names = []
        if not args.overseas_only:
            api_names.extend(spec["api"] for spec in DEFAULT_CHINA_SPECS)
        if not args.china_only:
            api_names.extend(overseas_names)

    if args.max_apis > 0:
        api_names = api_names[: args.max_apis]
    if not api_names:
        print("没有可处理的接口。")
        return 2

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        total_written = 0
        success = 0
        failed = 0

        for idx, api_name in enumerate(api_names, start=1):
            try:
                df = fetch_with_retry(api_name, retry=args.retry, backoff=args.backoff)
                spec = china_map.get(api_name) or build_auto_spec(api_name, df)
                if spec is None:
                    print(f"[{idx}/{len(api_names)}] {api_name}: 无法识别字段结构，跳过")
                    continue
                rows = build_rows_from_spec(spec, df)
                written = upsert_rows(conn, args.table_name, rows)
                total_written += written
                success += 1
                print(f"[{idx}/{len(api_names)}] {api_name}: raw_rows={0 if df is None else len(df)} metric_rows={written}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"[{idx}/{len(api_names)}] {api_name}: 失败 -> {exc}")
            if args.pause > 0:
                time.sleep(args.pause)

        total_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(f"完成: success={success}, failed={failed}, written={total_written}, table_rows={total_rows}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
