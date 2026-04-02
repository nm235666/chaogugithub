from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone


def parse_json_text(raw: str):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def round_or_none(value, digits: int = 4):
    num = safe_float(value)
    if num is None:
        return None
    return round(num, digits)


def percentile_rank(values: list[float], current: float):
    clean = sorted([v for v in values if v is not None])
    if not clean or current is None:
        return None
    count = sum(1 for v in clean if v <= current)
    return round(count / len(clean) * 100, 2)


def latest_macro_row(conn, indicator_codes: list[str]):
    for code in indicator_codes:
        row = conn.execute(
            """
            SELECT indicator_code, indicator_name, period, value, unit, source
            FROM macro_series
            WHERE indicator_code = ?
            ORDER BY period DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()
        if row:
            return dict(row)
    return None


def build_financial_summary(conn, ts_code: str):
    rows = conn.execute(
        """
        SELECT report_period, report_type, ann_date, revenue, op_profit, net_profit, net_profit_excl_nr,
               roe, gross_margin, debt_to_assets, operating_cf, free_cf, eps, bps
        FROM stock_financials
        WHERE ts_code = ?
        ORDER BY report_period DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = [dict(r) for r in rows]
    latest = items[0]
    yoy_base = None
    latest_suffix = str(latest["report_period"])[-4:]
    for item in items[1:]:
        if str(item["report_period"]).endswith(latest_suffix):
            yoy_base = item
            break

    def _yoy(field: str):
        curr = safe_float(latest.get(field))
        prev = safe_float(yoy_base.get(field)) if yoy_base else None
        if curr is None or prev in (None, 0):
            return None
        return round((curr - prev) / abs(prev) * 100, 2)

    latest_clean = {
        "report_period": latest.get("report_period"),
        "report_type": latest.get("report_type"),
        "ann_date": latest.get("ann_date"),
        "revenue": round_or_none(latest.get("revenue"), 2),
        "op_profit": round_or_none(latest.get("op_profit"), 2),
        "net_profit": round_or_none(latest.get("net_profit"), 2),
        "net_profit_excl_nr": round_or_none(latest.get("net_profit_excl_nr"), 2),
        "roe": round_or_none(latest.get("roe"), 2),
        "gross_margin": round_or_none(latest.get("gross_margin"), 2),
        "debt_to_assets": round_or_none(latest.get("debt_to_assets"), 2),
        "operating_cf": round_or_none(latest.get("operating_cf"), 2),
        "free_cf": round_or_none(latest.get("free_cf"), 2),
        "eps": round_or_none(latest.get("eps"), 4),
        "bps": round_or_none(latest.get("bps"), 4),
    }
    trend = {
        "revenue_yoy_pct": _yoy("revenue"),
        "net_profit_yoy_pct": _yoy("net_profit"),
        "net_profit_excl_nr_yoy_pct": _yoy("net_profit_excl_nr"),
        "operating_cf_yoy_pct": _yoy("operating_cf"),
        "roe_change": (
            round(safe_float(latest.get("roe")) - safe_float(yoy_base.get("roe")), 2)
            if yoy_base and safe_float(latest.get("roe")) is not None and safe_float(yoy_base.get("roe")) is not None
            else None
        ),
        "debt_to_assets_change": (
            round(safe_float(latest.get("debt_to_assets")) - safe_float(yoy_base.get("debt_to_assets")), 2)
            if yoy_base
            and safe_float(latest.get("debt_to_assets")) is not None
            and safe_float(yoy_base.get("debt_to_assets")) is not None
            else None
        ),
    }
    recent_reports = []
    for item in items[:4]:
        recent_reports.append(
            {
                "report_period": item.get("report_period"),
                "revenue": round_or_none(item.get("revenue"), 2),
                "net_profit": round_or_none(item.get("net_profit"), 2),
                "roe": round_or_none(item.get("roe"), 2),
                "operating_cf": round_or_none(item.get("operating_cf"), 2),
                "eps": round_or_none(item.get("eps"), 4),
            }
        )
    return {
        "latest_report_period": latest.get("report_period"),
        "latest": latest_clean,
        "trend": trend,
        "recent_4_reports": recent_reports,
    }


def build_valuation_summary(conn, ts_code: str):
    rows = conn.execute(
        """
        SELECT trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv
        FROM stock_valuation_daily
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 250
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = [dict(r) for r in rows]
    latest = items[0]
    pe_ttm_values = [safe_float(r["pe_ttm"]) for r in items if safe_float(r["pe_ttm"]) is not None]
    pb_values = [safe_float(r["pb"]) for r in items if safe_float(r["pb"]) is not None]
    dv_values = [safe_float(r["dv_ttm"]) for r in items if safe_float(r["dv_ttm"]) is not None]
    return {
        "trade_date": latest.get("trade_date"),
        "current": {
            "pe": round_or_none(latest.get("pe"), 4),
            "pe_ttm": round_or_none(latest.get("pe_ttm"), 4),
            "pb": round_or_none(latest.get("pb"), 4),
            "ps": round_or_none(latest.get("ps"), 4),
            "ps_ttm": round_or_none(latest.get("ps_ttm"), 4),
            "dv_ratio": round_or_none(latest.get("dv_ratio"), 4),
            "dv_ttm": round_or_none(latest.get("dv_ttm"), 4),
            "total_mv": round_or_none(latest.get("total_mv"), 2),
            "circ_mv": round_or_none(latest.get("circ_mv"), 2),
        },
        "history_percentile": {
            "pe_ttm_pct": percentile_rank(pe_ttm_values, safe_float(latest.get("pe_ttm"))),
            "pb_pct": percentile_rank(pb_values, safe_float(latest.get("pb"))),
            "dv_ttm_pct": percentile_rank(dv_values, safe_float(latest.get("dv_ttm"))),
        },
    }


def build_price_rollups_summary(conn, ts_code: str):
    try:
        rows = conn.execute(
            """
            SELECT
                ts_code, window_days, start_date, end_date, rows_count,
                close_first, close_last, close_change_pct, high_max, low_min, vol_avg, amount_avg, update_time
            FROM stock_daily_price_rollups
            WHERE ts_code = ?
            ORDER BY window_days ASC, end_date DESC
            """,
            (ts_code,),
        ).fetchall()
    except Exception:
        return {"items": [], "by_window": {}}

    latest_by_window: dict[str, dict] = {}
    for row in rows:
        d = dict(row)
        key = str(d.get("window_days") or "")
        if not key or key in latest_by_window:
            continue
        latest_by_window[key] = {
            "window_days": d.get("window_days"),
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "rows_count": d.get("rows_count"),
            "close_first": round_or_none(d.get("close_first"), 3),
            "close_last": round_or_none(d.get("close_last"), 3),
            "close_change_pct": round_or_none(d.get("close_change_pct"), 2),
            "high_max": round_or_none(d.get("high_max"), 3),
            "low_min": round_or_none(d.get("low_min"), 3),
            "vol_avg": round_or_none(d.get("vol_avg"), 2),
            "amount_avg": round_or_none(d.get("amount_avg"), 2),
            "update_time": d.get("update_time"),
        }
    ordered_items = [latest_by_window[k] for k in sorted(latest_by_window.keys(), key=lambda x: int(x))]
    return {"items": ordered_items, "by_window": latest_by_window}


def build_capital_flow_summary(conn, ts_code: str):
    stock_rows = conn.execute(
        """
        SELECT trade_date, net_inflow, main_inflow, super_large_inflow, large_inflow, medium_inflow, small_inflow
        FROM capital_flow_stock
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 5
        """,
        (ts_code,),
    ).fetchall()
    market_rows = conn.execute(
        """
        SELECT trade_date, flow_type, net_inflow
        FROM capital_flow_market
        ORDER BY trade_date DESC
        LIMIT 10
        """
    ).fetchall()
    out = {}
    if stock_rows:
        stock_items = [dict(r) for r in stock_rows]
        latest = stock_items[0]
        out["stock_flow"] = {
            "latest": {
                "trade_date": latest.get("trade_date"),
                "net_inflow": round_or_none(latest.get("net_inflow"), 2),
                "main_inflow": round_or_none(latest.get("main_inflow"), 2),
                "super_large_inflow": round_or_none(latest.get("super_large_inflow"), 2),
                "large_inflow": round_or_none(latest.get("large_inflow"), 2),
            },
            "recent_5d_sum": {
                "net_inflow": round(sum(safe_float(x["net_inflow"]) or 0 for x in stock_items), 2),
                "main_inflow": round(sum(safe_float(x["main_inflow"]) or 0 for x in stock_items), 2),
            },
        }
    if market_rows:
        grouped: dict[str, dict] = {}
        for row in market_rows:
            d = dict(row)
            flow_type = d["flow_type"]
            if flow_type not in grouped:
                grouped[flow_type] = {
                    "trade_date": d["trade_date"],
                    "net_inflow": round_or_none(d["net_inflow"], 2),
                }
        out["market_flow"] = grouped
    return out


def build_event_summary(conn, ts_code: str):
    rows = conn.execute(
        """
        SELECT event_type, event_date, ann_date, title, detail_json, source
        FROM stock_events
        WHERE ts_code = ?
        ORDER BY COALESCE(event_date, ann_date) DESC, ann_date DESC, id DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = []
    type_count: dict[str, int] = {}
    for row in rows:
        d = dict(row)
        type_count[d["event_type"]] = type_count.get(d["event_type"], 0) + 1
        items.append(
            {
                "event_type": d["event_type"],
                "event_date": d["event_date"],
                "ann_date": d["ann_date"],
                "title": d["title"],
                "detail": parse_json_text(d["detail_json"]),
            }
        )
    return {"recent_events": items, "event_type_count": type_count}


def build_macro_context(conn):
    shibor_1m = latest_macro_row(conn, ["shibor.1m"])
    shibor_3m = latest_macro_row(conn, ["shibor.3m"])
    shibor_1y = latest_macro_row(conn, ["shibor.1y"])
    m1_yoy = latest_macro_row(conn, ["cn_m.m1_yoy", "cn_m.m1"])
    m0_yoy = latest_macro_row(conn, ["cn_m.m0_yoy", "cn_m.m0"])
    cpi = latest_macro_row(conn, ["cn_cpi.nt_yoy", "cn_cpi.nt_val"])
    return {
        "asof": max(
            [x["period"] for x in [shibor_1m, shibor_3m, shibor_1y, m1_yoy, m0_yoy, cpi] if x],
            default="",
        ),
        "liquidity": {
            "shibor_1m": round_or_none(shibor_1m["value"], 4) if shibor_1m else None,
            "shibor_3m": round_or_none(shibor_3m["value"], 4) if shibor_3m else None,
            "shibor_1y": round_or_none(shibor_1y["value"], 4) if shibor_1y else None,
        },
        "money_supply": {
            "m1_value": round_or_none(m1_yoy["value"], 4) if m1_yoy else None,
            "m1_code": m1_yoy["indicator_code"] if m1_yoy else None,
            "m0_value": round_or_none(m0_yoy["value"], 4) if m0_yoy else None,
            "m0_code": m0_yoy["indicator_code"] if m0_yoy else None,
        },
        "inflation": {
            "cpi_value": round_or_none(cpi["value"], 4) if cpi else None,
            "cpi_code": cpi["indicator_code"] if cpi else None,
        },
    }


def build_fx_context(conn):
    pairs = ["USDCNH.FXCM", "USDJPY.FXCM", "EURUSD.FXCM"]
    out = {}
    for pair in pairs:
        rows = conn.execute(
            """
            SELECT trade_date, close, pct_chg
            FROM fx_daily
            WHERE pair_code = ?
            ORDER BY trade_date DESC
            LIMIT 20
            """,
            (pair,),
        ).fetchall()
        if not rows:
            continue
        items = [dict(r) for r in rows]
        latest = items[0]
        oldest = items[-1]
        latest_close = safe_float(latest["close"])
        oldest_close = safe_float(oldest["close"])
        out[pair] = {
            "trade_date": latest.get("trade_date"),
            "close": round_or_none(latest_close, 6),
            "pct_chg": round_or_none(latest.get("pct_chg"), 4),
            "return_20d_pct": (
                round((latest_close - oldest_close) / oldest_close * 100, 4)
                if latest_close is not None and oldest_close not in (None, 0)
                else None
            ),
        }
    return out


def build_rate_spread_context(conn):
    latest_curve_date_row = conn.execute("SELECT MAX(trade_date) FROM rate_curve_points").fetchone()
    latest_spread_date_row = conn.execute("SELECT MAX(trade_date) FROM spread_daily").fetchone()
    latest_curve_date = latest_curve_date_row[0] if latest_curve_date_row else None
    latest_spread_date = latest_spread_date_row[0] if latest_spread_date_row else None
    out = {}
    if latest_curve_date:
        curve_rows = conn.execute(
            """
            SELECT market, curve_code, tenor, value
            FROM rate_curve_points
            WHERE trade_date = ?
            AND ((market='CN' AND curve_code='shibor') OR (market='US' AND curve_code='treasury'))
            """,
            (latest_curve_date,),
        ).fetchall()
        grouped: dict[str, dict] = {}
        for row in curve_rows:
            d = dict(row)
            key = f"{d['market']}_{d['curve_code']}"
            grouped.setdefault(key, {})[d["tenor"]] = round_or_none(d["value"], 4)
        out["curve_date"] = latest_curve_date
        out["curves"] = grouped
    if latest_spread_date:
        spread_rows = conn.execute(
            """
            SELECT spread_code, value
            FROM spread_daily
            WHERE trade_date = ?
            """,
            (latest_spread_date,),
        ).fetchall()
        out["spread_date"] = latest_spread_date
        out["spreads"] = {r["spread_code"]: round_or_none(r["value"], 4) for r in spread_rows}
    return out


def build_governance_summary(conn, ts_code: str):
    row = conn.execute(
        """
        SELECT asof_date, holder_structure_json, board_structure_json, mgmt_change_json, incentive_plan_json, governance_score
        FROM company_governance
        WHERE ts_code = ?
        ORDER BY asof_date DESC
        LIMIT 1
        """,
        (ts_code,),
    ).fetchone()
    if not row:
        return {}
    d = dict(row)
    holder = parse_json_text(d.get("holder_structure_json"))
    board = parse_json_text(d.get("board_structure_json"))
    mgmt = parse_json_text(d.get("mgmt_change_json"))
    incentive = parse_json_text(d.get("incentive_plan_json"))
    return {
        "asof_date": d.get("asof_date"),
        "governance_score": round_or_none(d.get("governance_score"), 2),
        "holder_summary": {
            "top1_ratio": round_or_none(holder.get("top1_ratio"), 4),
            "top10_ratio_sum": round_or_none(holder.get("top10_ratio_sum"), 4),
            "holder_num_latest": holder.get("holder_num_latest"),
            "pledge_stat_latest": holder.get("pledge_stat_latest"),
            "top10_holders": (holder.get("top10_holders") or [])[:5],
        },
        "board_summary": {
            "reward_period": board.get("reward_period"),
            "total_reward": round_or_none(board.get("total_reward"), 2),
            "members": (board.get("members") or [])[:10],
        },
        "mgmt_changes": (mgmt.get("recent_holder_trades") or [])[:5],
        "incentive_plan": incentive,
    }


def build_risk_summary(conn, ts_code: str):
    latest_row = conn.execute(
        "SELECT MAX(scenario_date) FROM risk_scenarios WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()
    latest_date = latest_row[0] if latest_row else None
    if not latest_date:
        return {}
    rows = conn.execute(
        """
        SELECT scenario_name, horizon, pnl_impact, max_drawdown, var_95, cvar_95, assumptions_json
        FROM risk_scenarios
        WHERE ts_code = ? AND scenario_date = ?
        ORDER BY scenario_name
        """,
        (ts_code, latest_date),
    ).fetchall()
    items = []
    for row in rows:
        d = dict(row)
        items.append(
            {
                "scenario_name": d["scenario_name"],
                "horizon": d["horizon"],
                "pnl_impact": round_or_none(d["pnl_impact"], 4),
                "max_drawdown": round_or_none(d["max_drawdown"], 4),
                "var_95": round_or_none(d["var_95"], 4),
                "cvar_95": round_or_none(d["cvar_95"], 4),
                "assumptions": parse_json_text(d["assumptions_json"]),
            }
        )
    return {"scenario_date": latest_date, "items": items}


def stock_news_latest_pub(conn, ts_code: str):
    row = conn.execute(
        "SELECT MAX(pub_time) FROM stock_news_items WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()
    return row[0] if row and row[0] else ""


def stock_news_is_fresh(conn, ts_code: str):
    latest_pub = stock_news_latest_pub(conn, ts_code)
    if not latest_pub:
        return False, ""
    latest_date = str(latest_pub).strip()[:10]
    today_cn = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    return latest_date == today_cn, latest_pub


def build_stock_news_summary(conn, ts_code: str):
    rows = conn.execute(
        """
        SELECT pub_time, title, summary, link, llm_system_score, llm_finance_impact_score,
               llm_finance_importance, llm_summary, llm_impacts_json
        FROM stock_news_items
        WHERE ts_code = ?
        ORDER BY pub_time DESC, id DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = []
    high_count = 0
    for row in rows:
        d = dict(row)
        imp = d.get("llm_finance_importance") or ""
        if imp in {"极高", "高", "中"}:
            high_count += 1
        items.append(
            {
                "pub_time": d.get("pub_time"),
                "title": d.get("title"),
                "summary": d.get("summary"),
                "llm_summary": d.get("llm_summary"),
                "finance_importance": imp,
                "system_score": d.get("llm_system_score"),
                "finance_impact_score": d.get("llm_finance_impact_score"),
                "impacts": parse_json_text(d.get("llm_impacts_json") or ""),
                "link": d.get("link"),
            }
        )
    return {
        "latest_pub_time": items[0].get("pub_time"),
        "high_importance_count_recent_8": high_count,
        "recent_items": items,
    }
