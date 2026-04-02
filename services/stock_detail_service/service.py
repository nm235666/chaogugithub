from __future__ import annotations

from datetime import datetime, timezone

from .assembler import (
    build_capital_flow_summary,
    build_event_summary,
    build_financial_summary,
    build_governance_summary,
    build_price_rollups_summary,
    build_risk_summary,
    build_stock_news_summary,
    build_valuation_summary,
    parse_json_text,
    round_or_none,
    safe_float,
)


def query_stock_detail(*, sqlite3_module, db_path, ts_code: str, keyword: str, lookback: int = 60):
    ts_code = (ts_code or "").strip().upper()
    keyword = (keyword or "").strip()
    lookback = min(max(int(lookback or 60), 20), 240)

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        profile = None
        if ts_code:
            profile = conn.execute(
                """
                SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
                FROM stock_codes
                WHERE ts_code = ?
                LIMIT 1
                """,
                (ts_code,),
            ).fetchone()
        if not profile and keyword:
            kw = f"%{keyword}%"
            profile = conn.execute(
                """
                SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
                FROM stock_codes
                WHERE ts_code LIKE ? OR symbol LIKE ? OR name LIKE ?
                ORDER BY CASE WHEN list_status = 'L' THEN 0 ELSE 1 END, ts_code
                LIMIT 1
                """,
                (kw, kw, kw),
            ).fetchone()
        if not profile:
            raise ValueError(f"未找到股票: {ts_code or keyword}")

        profile_dict = dict(profile)
        resolved_ts_code = str(profile["ts_code"])
        name = str(profile["name"] or "")
        symbol = str(profile["symbol"] or "")

        recent_prices = [
            dict(r)
            for r in conn.execute(
                """
                SELECT trade_date, open, high, low, close, pct_chg, vol, amount
                FROM stock_daily_prices
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (resolved_ts_code, lookback),
            ).fetchall()
        ]
        recent_prices.reverse()

        minute_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT trade_date, minute_time, price, avg_price, volume, total_volume
                FROM stock_minline
                WHERE ts_code = ?
                ORDER BY trade_date DESC, minute_time DESC
                LIMIT 120
                """,
                (resolved_ts_code,),
            ).fetchall()
        ]
        minute_rows.reverse()
        latest_minline = minute_rows[-1] if minute_rows else None

        latest_score = None
        score_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_scores_daily'"
        ).fetchone()[0]
        if score_exists:
            latest_score = conn.execute(
                """
                SELECT *
                FROM stock_scores_daily
                WHERE ts_code = ?
                ORDER BY score_date DESC
                LIMIT 1
                """,
                (resolved_ts_code,),
            ).fetchone()
        score_dict = dict(latest_score) if latest_score else {}
        score_payload = parse_json_text(score_dict.get("score_payload_json") or "")
        if score_dict:
            score_dict["score_summary"] = score_payload.get("score_summary", {})
            score_dict["raw_metrics"] = score_payload.get("raw_metrics", {})

        financial_summary = build_financial_summary(conn, resolved_ts_code)
        valuation_summary = build_valuation_summary(conn, resolved_ts_code)
        capital_flow_summary = build_capital_flow_summary(conn, resolved_ts_code)
        event_summary = build_event_summary(conn, resolved_ts_code)
        governance_summary = build_governance_summary(conn, resolved_ts_code)
        risk_summary = build_risk_summary(conn, resolved_ts_code)
        stock_news_summary = build_stock_news_summary(conn, resolved_ts_code)
        price_rollups = build_price_rollups_summary(conn, resolved_ts_code)

        candidate_pool_item = conn.execute(
            """
            SELECT candidate_name, candidate_type, bullish_room_count, bearish_room_count, net_score,
                   dominant_bias, mention_count, room_count, latest_analysis_date
            FROM chatroom_stock_candidate_pool
            WHERE candidate_name IN (?, ?, ?)
            ORDER BY
              CASE candidate_name WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END,
              ABS(COALESCE(net_score, 0)) DESC
            LIMIT 1
            """,
            (name, resolved_ts_code, symbol, name, resolved_ts_code),
        ).fetchone()

        latest_subquery = """
        SELECT room_id, MAX(update_time) AS max_update_time
        FROM chatroom_investment_analysis
        GROUP BY room_id
        """
        chatroom_mentions = (
            [
                dict(r)
                for r in conn.execute(
                    f"""
                    SELECT a.room_id, a.talker, a.analysis_date, a.latest_message_date, a.final_bias,
                           a.targets_json, a.room_summary, a.update_time
                    FROM chatroom_investment_analysis a
                    JOIN ({latest_subquery}) latest
                      ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
                    WHERE a.targets_json LIKE ?
                    ORDER BY a.latest_message_date DESC, a.update_time DESC
                    LIMIT 8
                    """,
                    (f"%{name}%",),
                ).fetchall()
            ]
            if conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_investment_analysis'"
            ).fetchone()[0]
            else []
        )

        price_summary = {}
        if recent_prices:
            closes = [safe_float(x.get("close")) for x in recent_prices if safe_float(x.get("close")) is not None]
            latest_bar = recent_prices[-1]
            first_close = closes[0] if closes else None
            last_close = closes[-1] if closes else None
            price_summary = {
                "latest_trade_date": latest_bar.get("trade_date"),
                "latest_close": round_or_none(latest_bar.get("close"), 3),
                "latest_pct_chg": round_or_none(latest_bar.get("pct_chg"), 2),
                "range_return_pct": (
                    round((last_close - first_close) / first_close * 100, 2)
                    if first_close not in (None, 0) and last_close is not None
                    else None
                ),
                "high_lookback": max(closes) if closes else None,
                "low_lookback": min(closes) if closes else None,
            }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "profile": profile_dict,
            "price_summary": price_summary,
            "recent_prices": recent_prices,
            "recent_minline": minute_rows,
            "latest_minline": latest_minline,
            "score": score_dict,
            "financial_summary": financial_summary,
            "valuation_summary": valuation_summary,
            "capital_flow_summary": capital_flow_summary,
            "event_summary": event_summary,
            "governance_summary": governance_summary,
            "risk_summary": risk_summary,
            "stock_news_summary": stock_news_summary,
            "price_rollups": price_rollups,
            "candidate_pool_item": dict(candidate_pool_item) if candidate_pool_item else None,
            "chatroom_mentions": chatroom_mentions,
        }
    finally:
        conn.close()


def build_stock_detail_runtime_deps(*, sqlite3_module, db_path) -> dict:
    return {
        "query_stock_detail": lambda **kwargs: query_stock_detail(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        )
    }
