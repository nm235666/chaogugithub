from __future__ import annotations

import subprocess
from pathlib import Path


def query_wechat_chatlog(
    *,
    sqlite3_module,
    db_path,
    talker: str,
    sender_name: str,
    keyword: str,
    is_quote: str,
    query_date_start: str,
    query_date_end: str,
    page: int,
    page_size: int,
):
    talker = (talker or "").strip()
    sender_name = (sender_name or "").strip()
    keyword = (keyword or "").strip()
    is_quote = (is_quote or "").strip()
    query_date_start = (query_date_start or "").strip()
    query_date_end = (query_date_end or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='wechat_chatlog_clean_items'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "filters": {"talkers": [], "senders": []},
            }

        where_clauses = []
        params: list[object] = []
        if talker:
            where_clauses.append("talker = ?")
            params.append(talker)
        if sender_name:
            where_clauses.append("sender_name = ?")
            params.append(sender_name)
        if keyword:
            where_clauses.append("(content_clean LIKE ? OR quote_content LIKE ? OR sender_name LIKE ?)")
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw])
        if is_quote in {"0", "1"}:
            where_clauses.append("is_quote = ?")
            params.append(int(is_quote))
        if query_date_start:
            where_clauses.append("query_date_start >= ?")
            params.append(query_date_start)
        if query_date_end:
            where_clauses.append("query_date_end <= ?")
            params.append(query_date_end)

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(
            f"SELECT COUNT(*) FROM wechat_chatlog_clean_items{where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, talker, query_date_start, query_date_end, message_date, message_time,
                sender_name, sender_id, message_type, content, content_clean, is_quote,
                quote_sender_name, quote_sender_id, quote_time_text, quote_content,
                raw_block, source_url, fetched_at, update_time
            FROM wechat_chatlog_clean_items
            {where_sql}
            ORDER BY query_date_start DESC, message_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        talkers = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT talker FROM wechat_chatlog_clean_items WHERE talker IS NOT NULL AND talker <> '' ORDER BY talker"
            ).fetchall()
        ]
        sender_params: list[object] = []
        sender_where_clauses = ["sender_name IS NOT NULL", "sender_name <> ''"]
        if talker:
            sender_where_clauses.append("talker = ?")
            sender_params.append(talker)
        sender_where_sql = " WHERE " + " AND ".join(sender_where_clauses)
        senders = [
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT sender_name FROM wechat_chatlog_clean_items{sender_where_sql} ORDER BY sender_name",
                sender_params,
            ).fetchall()
        ]
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "filters": {"talkers": talkers, "senders": senders},
    }


def query_chatroom_overview(
    *,
    sqlite3_module,
    db_path,
    keyword: str,
    primary_category: str,
    activity_level: str,
    risk_level: str,
    skip_realtime_monitor: str,
    fetch_status: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    primary_category = (primary_category or "").strip()
    activity_level = (activity_level or "").strip()
    risk_level = (risk_level or "").strip()
    skip_realtime_monitor = (skip_realtime_monitor or "").strip()
    fetch_status = (fetch_status or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        room_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_list_items'"
        ).fetchone()[0]
        if not room_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"primary_categories": [], "activity_levels": [], "risk_levels": [], "fetch_statuses": []},
            }

        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append(
                "(c.room_id LIKE ? OR c.remark LIKE ? OR c.nick_name LIKE ? OR c.llm_chatroom_summary LIKE ? OR c.llm_chatroom_tags_json LIKE ?)"
            )
            params.extend([kw, kw, kw, kw, kw])
        if primary_category:
            where_clauses.append("COALESCE(c.llm_chatroom_primary_category, '') = ?")
            params.append(primary_category)
        if activity_level:
            where_clauses.append("COALESCE(c.llm_chatroom_activity_level, '') = ?")
            params.append(activity_level)
        if risk_level:
            where_clauses.append("COALESCE(c.llm_chatroom_risk_level, '') = ?")
            params.append(risk_level)
        if skip_realtime_monitor in {"0", "1"}:
            where_clauses.append("COALESCE(c.skip_realtime_monitor, 0) = ?")
            params.append(int(skip_realtime_monitor))
        if fetch_status == "failed":
            where_clauses.append(
                "COALESCE(c.last_chatlog_backfill_status, '') <> '' AND COALESCE(c.last_chatlog_backfill_status, '') <> 'ok'"
            )
        elif fetch_status == "ok":
            where_clauses.append("COALESCE(c.last_chatlog_backfill_status, '') = 'ok'")
        elif fetch_status == "unknown":
            where_clauses.append("COALESCE(c.last_chatlog_backfill_status, '') = ''")

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        count_sql = f"SELECT COUNT(*) FROM chatroom_list_items c {where_sql}"
        data_sql = f"""
        SELECT
            c.room_id, c.remark, c.nick_name, c.owner, c.user_count, c.first_seen_at, c.last_seen_at, c.update_time,
            c.skip_realtime_monitor, c.skip_realtime_reason, c.skip_realtime_marked_at, c.last_message_date,
            c.last_chatlog_backfill_at, c.last_chatlog_backfill_status, c.last_30d_raw_message_count,
            c.last_30d_clean_message_count, c.last_30d_fetch_fail_count, c.silent_candidate_runs,
            c.silent_candidate_since, c.llm_chatroom_summary, c.llm_chatroom_tags_json,
            c.llm_chatroom_primary_category, c.llm_chatroom_activity_level, c.llm_chatroom_risk_level,
            c.llm_chatroom_confidence, c.llm_chatroom_model, c.llm_chatroom_tagged_at,
            COALESCE(logs.message_row_count, 0) AS message_row_count
        FROM chatroom_list_items c
        LEFT JOIN (
            SELECT talker, COUNT(*) AS message_row_count
            FROM wechat_chatlog_clean_items
            GROUP BY talker
        ) logs
          ON logs.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
        {where_sql}
        ORDER BY COALESCE(c.last_message_date, '') DESC, COALESCE(logs.message_row_count, 0) DESC, COALESCE(c.user_count, 0) DESC, c.room_id
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        summary = {
            "room_total": conn.execute("SELECT COUNT(*) FROM chatroom_list_items").fetchone()[0],
            "room_with_logs": conn.execute(
                """
                SELECT COUNT(*)
                FROM chatroom_list_items c
                WHERE EXISTS (
                    SELECT 1 FROM wechat_chatlog_clean_items l
                    WHERE l.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
                )
                """
            ).fetchone()[0],
            "skip_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_list_items WHERE COALESCE(skip_realtime_monitor, 0) = 1"
            ).fetchone()[0],
            "tagged_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_list_items WHERE llm_chatroom_primary_category IS NOT NULL AND llm_chatroom_primary_category <> ''"
            ).fetchone()[0],
        }
        filters = {
            "primary_categories": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT llm_chatroom_primary_category FROM chatroom_list_items WHERE llm_chatroom_primary_category IS NOT NULL AND llm_chatroom_primary_category <> '' ORDER BY llm_chatroom_primary_category"
                ).fetchall()
            ],
            "activity_levels": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT llm_chatroom_activity_level FROM chatroom_list_items WHERE llm_chatroom_activity_level IS NOT NULL AND llm_chatroom_activity_level <> '' ORDER BY llm_chatroom_activity_level"
                ).fetchall()
            ],
            "risk_levels": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT llm_chatroom_risk_level FROM chatroom_list_items WHERE llm_chatroom_risk_level IS NOT NULL AND llm_chatroom_risk_level <> '' ORDER BY llm_chatroom_risk_level"
                ).fetchall()
            ],
            "fetch_statuses": ["failed", "ok", "unknown"],
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def fetch_single_chatroom_now(
    *,
    sqlite3_module,
    db_path,
    root_dir: Path,
    publish_app_event,
    room_id: str,
    fetch_yesterday_and_today: bool,
):
    room_id = (room_id or "").strip()
    if not room_id:
        raise ValueError("room_id 不能为空")

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        row = conn.execute(
            """
            SELECT room_id, remark, nick_name, skip_realtime_monitor
            FROM chatroom_list_items
            WHERE room_id = ?
            """,
            (room_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"未找到群聊: {room_id}")
    if int(row["skip_realtime_monitor"] or 0) != 0:
        raise ValueError("该群当前未处于监控中")

    talker = str(row["remark"] or row["nick_name"] or row["room_id"] or "").strip()
    cmd = ["python3", str(root_dir / "fetch_monitored_chatlogs_once.py"), "--only-room", talker]
    if fetch_yesterday_and_today:
        cmd.append("--yesterday-and-today")
    publish_app_event(
        event="chatroom_fetch_update",
        payload={
            "room_id": room_id,
            "talker": talker,
            "status": "running",
            "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        },
        producer="services.chatrooms_service",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
    output = ((proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")).strip()
    if proc.returncode != 0:
        publish_app_event(
            event="chatroom_fetch_update",
            payload={
                "room_id": room_id,
                "talker": talker,
                "status": "error",
                "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
                "error": output or "立即拉取失败",
            },
            producer="services.chatrooms_service",
        )
        raise RuntimeError(output or "立即拉取失败")
    publish_app_event(
        event="chatroom_fetch_update",
        payload={
            "room_id": room_id,
            "talker": talker,
            "status": "done",
            "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        },
        producer="services.chatrooms_service",
    )
    return {
        "ok": True,
        "room_id": room_id,
        "talker": talker,
        "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        "output": output,
    }


def query_chatroom_investment_analysis(
    *,
    sqlite3_module,
    db_path,
    keyword: str,
    final_bias: str,
    target_keyword: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    final_bias = (final_bias or "").strip()
    target_keyword = (target_keyword or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_investment_analysis'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"final_biases": []},
            }
        cols = {r[1] for r in conn.execute("PRAGMA table_info(chatroom_investment_analysis)").fetchall()}
        select_chatroom_sentiment = ", ".join(
            [
                "a.llm_sentiment_score" if "llm_sentiment_score" in cols else "NULL AS llm_sentiment_score",
                "a.llm_sentiment_label" if "llm_sentiment_label" in cols else "NULL AS llm_sentiment_label",
                "a.llm_sentiment_reason" if "llm_sentiment_reason" in cols else "NULL AS llm_sentiment_reason",
                "a.llm_sentiment_confidence" if "llm_sentiment_confidence" in cols else "NULL AS llm_sentiment_confidence",
                "a.llm_sentiment_model" if "llm_sentiment_model" in cols else "NULL AS llm_sentiment_model",
                "a.llm_sentiment_scored_at" if "llm_sentiment_scored_at" in cols else "NULL AS llm_sentiment_scored_at",
            ]
        )
        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(a.talker LIKE ? OR a.room_summary LIKE ?)")
            params.extend([kw, kw])
        if final_bias in {"看多", "看空"}:
            where_clauses.append("a.final_bias = ?")
            params.append(final_bias)
        if target_keyword:
            where_clauses.append("a.targets_json LIKE ?")
            params.append(f"%{target_keyword}%")
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        latest_subquery = "SELECT room_id, MAX(update_time) AS max_update_time FROM chatroom_investment_analysis GROUP BY room_id"
        count_sql = f"""
        SELECT COUNT(*)
        FROM chatroom_investment_analysis a
        JOIN ({latest_subquery}) latest
          ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
        {where_sql}
        """
        data_sql = f"""
        SELECT
            a.id, a.room_id, a.talker, a.analysis_date, a.analysis_window_days, a.message_count,
            a.sender_count, a.latest_message_date, a.room_summary, a.targets_json, a.final_bias,
            {select_chatroom_sentiment},
            a.model, a.prompt_version, a.created_at, a.update_time,
            c.remark, c.nick_name, c.user_count, c.skip_realtime_monitor
        FROM chatroom_investment_analysis a
        JOIN ({latest_subquery}) latest
          ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
        LEFT JOIN chatroom_list_items c ON c.room_id = a.room_id
        {where_sql}
        ORDER BY a.latest_message_date DESC, a.update_time DESC, a.id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        summary = {
            "analysis_total": conn.execute(f"SELECT COUNT(*) FROM ({latest_subquery}) latest_all").fetchone()[0],
            "bullish_total": conn.execute(
                f"SELECT COUNT(*) FROM chatroom_investment_analysis a JOIN ({latest_subquery}) latest ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time WHERE a.final_bias = '看多'"
            ).fetchone()[0],
            "bearish_total": conn.execute(
                f"SELECT COUNT(*) FROM chatroom_investment_analysis a JOIN ({latest_subquery}) latest ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time WHERE a.final_bias = '看空'"
            ).fetchone()[0],
            "with_targets_total": conn.execute(
                f"SELECT COUNT(*) FROM chatroom_investment_analysis a JOIN ({latest_subquery}) latest ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time WHERE a.targets_json IS NOT NULL AND a.targets_json <> '' AND a.targets_json <> '[]'"
            ).fetchone()[0],
        }
        filters = {
            "final_biases": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT final_bias FROM chatroom_investment_analysis WHERE final_bias IS NOT NULL AND final_bias <> '' ORDER BY final_bias"
                ).fetchall()
            ]
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def query_chatroom_candidate_pool(
    *,
    sqlite3_module,
    db_path,
    keyword: str,
    dominant_bias: str,
    candidate_type: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    dominant_bias = (dominant_bias or "").strip()
    candidate_type = (candidate_type or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_stock_candidate_pool'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"dominant_biases": [], "candidate_types": []},
            }
        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(candidate_name LIKE ? OR sample_reasons_json LIKE ?)")
            params.extend([kw, kw])
        if dominant_bias in {"看多", "看空"}:
            where_clauses.append("dominant_bias = ?")
            params.append(dominant_bias)
        if candidate_type:
            where_clauses.append("candidate_type = ?")
            params.append(candidate_type)
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(f"SELECT COUNT(*) FROM chatroom_stock_candidate_pool{where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, candidate_name, candidate_type, bullish_room_count, bearish_room_count,
                net_score, dominant_bias, mention_count, room_count, latest_analysis_date, ts_code,
                sample_reasons_json, source_room_ids_json, source_talkers_json, created_at, update_time
            FROM chatroom_stock_candidate_pool
            {where_sql}
            ORDER BY ABS(net_score) DESC, room_count DESC, mention_count DESC, candidate_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        summary = {
            "candidate_total": conn.execute("SELECT COUNT(*) FROM chatroom_stock_candidate_pool").fetchone()[0],
            "bullish_total": conn.execute("SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE dominant_bias = '看多'").fetchone()[0],
            "bearish_total": conn.execute("SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE dominant_bias = '看空'").fetchone()[0],
            "stock_like_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE candidate_type IN ('股票', '标的')"
            ).fetchone()[0],
        }
        filters = {
            "dominant_biases": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT dominant_bias FROM chatroom_stock_candidate_pool WHERE dominant_bias IS NOT NULL AND dominant_bias <> '' ORDER BY dominant_bias"
                ).fetchall()
            ],
            "candidate_types": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT candidate_type FROM chatroom_stock_candidate_pool WHERE candidate_type IS NOT NULL AND candidate_type <> '' ORDER BY candidate_type"
                ).fetchall()
            ],
        }
        stock_name_map = {}
        try:
            stock_name_map = {
                str(r[0] or "").strip().upper(): str(r[1] or "").strip()
                for r in conn.execute("SELECT ts_code, name FROM stock_codes").fetchall()
            }
        except Exception:
            stock_name_map = {}
        data = []
        for r in rows:
            item = dict(r)
            ts_code = str(item.get("ts_code") or "").strip().upper()
            display_name = item.get("candidate_name") or ""
            if ts_code and stock_name_map.get(ts_code):
                display_name = stock_name_map.get(ts_code) or display_name
            item["display_name"] = display_name
            data.append(item)
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def build_chatrooms_service_deps(*, sqlite3_module, db_path, root_dir: Path, publish_app_event) -> dict:
    return {
        "query_wechat_chatlog": lambda **kwargs: query_wechat_chatlog(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_chatroom_overview": lambda **kwargs: query_chatroom_overview(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "fetch_single_chatroom_now": lambda **kwargs: fetch_single_chatroom_now(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            root_dir=root_dir,
            publish_app_event=publish_app_event,
            **kwargs,
        ),
        "query_chatroom_investment_analysis": lambda **kwargs: query_chatroom_investment_analysis(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
        "query_chatroom_candidate_pool": lambda **kwargs: query_chatroom_candidate_pool(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
    }
