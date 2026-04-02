from __future__ import annotations


def _top_market_expectations_for_theme(conn, theme_name: str, limit: int = 8):
    if not theme_name:
        return []
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='market_expectation_items'"
    ).fetchone()[0]
    if not exists:
        return []
    rows = conn.execute(
        """
        SELECT question, volume, liquidity, end_date, source_url, related_theme_names_json, outcome_prices_json
        FROM market_expectation_items
        WHERE related_theme_names_json LIKE ?
        ORDER BY COALESCE(volume, 0) DESC, COALESCE(liquidity, 0) DESC, id DESC
        LIMIT ?
        """,
        (f'%"{theme_name}"%', limit),
    ).fetchall()
    return [dict(r) for r in rows]


def query_investment_signals(
    *,
    sqlite3_module,
    db_path,
    resolve_signal_table_fn,
    cache_get_json_fn,
    cache_set_json_fn,
    redis_cache_ttl_signals: int,
    keyword: str,
    signal_type: str,
    signal_group: str,
    scope: str,
    source_filter: str,
    direction: str,
    signal_status: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    signal_type = (signal_type or "").strip()
    signal_group = (signal_group or "").strip()
    scope = (scope or "").strip().lower()
    source_filter = (source_filter or "").strip()
    direction = (direction or "").strip()
    signal_status = (signal_status or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    cache_key = (
        "api:investment-signals:v1:"
        f"kw={keyword}:stype={signal_type}:sg={signal_group}:scope={scope}:src={source_filter}:"
        f"dir={direction}:status={signal_status}:page={page}:size={page_size}"
    )
    cached = cache_get_json_fn(cache_key)
    if isinstance(cached, dict):
        return cached

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_name, normalized_scope = resolve_signal_table_fn(conn, scope)
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"signal_types": [], "directions": [], "signal_statuses": []},
                "scope": normalized_scope,
            }
        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(subject_name LIKE ? OR COALESCE(ts_code, '') LIKE ? OR COALESCE(evidence_json, '') LIKE ?)")
            params.extend([kw, kw, kw])
        if signal_type:
            where_clauses.append("signal_type = ?")
            params.append(signal_type)
        elif signal_group == "stock":
            where_clauses.append("signal_type = 'stock'")
        elif signal_group == "non_stock":
            where_clauses.append("signal_type <> 'stock'")
        elif signal_group == "chatroom_stock":
            where_clauses.append("signal_type = 'stock'")
            where_clauses.append("COALESCE(chatroom_count, 0) > 0")
        if source_filter == "chatroom":
            where_clauses.append("COALESCE(chatroom_count, 0) > 0")
        if direction:
            where_clauses.append("direction = ?")
            params.append(direction)
        if signal_status:
            where_clauses.append("signal_status = ?")
            params.append(signal_status)
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}{where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                latest_signal_date, evidence_json, source_summary_json, created_at, update_time
            FROM {table_name}
            {where_sql}
            ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC, subject_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        summary = {
            "signal_total": conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0],
            "bullish_total": conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE direction = '看多'").fetchone()[0],
            "bearish_total": conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE direction = '看空'").fetchone()[0],
            "active_total": conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE signal_status = '活跃'").fetchone()[0],
            "stock_total": conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE signal_type = 'stock'").fetchone()[0],
        }
        filters = {
            "signal_types": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT signal_type FROM {table_name} WHERE signal_type IS NOT NULL AND signal_type <> '' ORDER BY signal_type"
                ).fetchall()
            ],
            "directions": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT direction FROM {table_name} WHERE direction IS NOT NULL AND direction <> '' ORDER BY direction"
                ).fetchall()
            ],
            "signal_statuses": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT signal_status FROM {table_name} WHERE signal_status IS NOT NULL AND signal_status <> '' ORDER BY signal_status"
                ).fetchall()
            ],
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    payload = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
        "scope": normalized_scope,
    }
    cache_set_json_fn(cache_key, payload, redis_cache_ttl_signals)
    return payload


def query_investment_signal_timeline(
    *,
    sqlite3_module,
    db_path,
    get_or_build_cached_logic_view_fn,
    build_signal_logic_view_fn,
    build_signal_event_logic_view_fn,
    signal_key: str,
    page: int,
    page_size: int,
):
    signal_key = (signal_key or "").strip()
    requested_signal_key = signal_key
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    if not signal_key:
        raise ValueError("缺少 signal_key")

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        events_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_events'"
        ).fetchone()[0]
        snapshots_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_daily_snapshots'"
        ).fetchone()[0]
        tracker_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_tracker'"
        ).fetchone()[0]
        if not tracker_exists:
            return {"signal": None, "events": [], "snapshots": [], "page": page, "page_size": page_size, "total": 0, "total_pages": 0}

        signal_row = conn.execute(
            """
            SELECT id, signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                   evidence_count, news_count, stock_news_count, chatroom_count, signal_status, latest_signal_date,
                   evidence_json, source_summary_json, created_at, update_time
            FROM investment_signal_tracker
            WHERE signal_key = ?
            LIMIT 1
            """,
            (signal_key,),
        ).fetchone()
        total = 0
        if events_exists:
            total = conn.execute(
                "SELECT COUNT(*) FROM investment_signal_events WHERE signal_key = ?",
                (signal_key,),
            ).fetchone()[0]

        if (not signal_row) and total <= 0 and ":" in signal_key:
            prefix, subject_name = signal_key.split(":", 1)
            subject_name = subject_name.strip()
            if subject_name:
                candidate_prefixes = [prefix, "theme", "macro", "commodity", "fx"]
                seen = set()
                candidate_keys: list[str] = []
                for pfx in candidate_prefixes:
                    pfx = str(pfx or "").strip()
                    if not pfx:
                        continue
                    key = f"{pfx}:{subject_name}"
                    if key in seen:
                        continue
                    seen.add(key)
                    candidate_keys.append(key)

                best_score = -1
                best_key = signal_key
                best_row = None
                best_total = 0
                for key in candidate_keys:
                    row = conn.execute(
                        """
                        SELECT id, signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                               evidence_count, news_count, stock_news_count, chatroom_count, signal_status, latest_signal_date,
                               evidence_json, source_summary_json, created_at, update_time
                        FROM investment_signal_tracker
                        WHERE signal_key = ?
                        LIMIT 1
                        """,
                        (key,),
                    ).fetchone()
                    event_count = 0
                    if events_exists:
                        event_count = conn.execute(
                            "SELECT COUNT(*) FROM investment_signal_events WHERE signal_key = ?",
                            (key,),
                        ).fetchone()[0]
                    score = event_count * 10 + (1 if row else 0)
                    if score > best_score:
                        best_score = score
                        best_key = key
                        best_row = row
                        best_total = event_count

                if best_score > 0:
                    signal_key = best_key
                    signal_row = best_row
                    total = best_total

        events: list[dict] = []
        if events_exists:
            if total <= 0:
                total = conn.execute(
                    "SELECT COUNT(*) FROM investment_signal_events WHERE signal_key = ?",
                    (signal_key,),
                ).fetchone()[0]
            rows = conn.execute(
                """
                SELECT id, signal_key, event_time, event_date, event_type, old_direction, new_direction,
                       old_strength, new_strength, delta_strength, old_confidence, new_confidence,
                       delta_confidence, event_level, driver_type, driver_source, driver_ref_id,
                       driver_title, status_after_event, event_summary, evidence_json,
                       snapshot_before_json, snapshot_after_json, created_at
                FROM investment_signal_events
                WHERE signal_key = ?
                ORDER BY event_time DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (signal_key, page_size, offset),
            ).fetchall()
            for row in rows:
                item = dict(row)
                event_logic = get_or_build_cached_logic_view_fn(
                    conn,
                    entity_type="investment_signal_event",
                    entity_key=str(item.get("id") or ""),
                    source_payload={
                        "event_time": item.get("event_time"),
                        "event_type": item.get("event_type"),
                        "driver_type": item.get("driver_type"),
                        "driver_source": item.get("driver_source"),
                        "event_summary": item.get("event_summary"),
                        "new_direction": item.get("new_direction"),
                        "status_after_event": item.get("status_after_event"),
                        "evidence_json": item.get("evidence_json"),
                    },
                    builder=lambda current=item: build_signal_event_logic_view_fn(current),
                )
                item["logic_view"] = event_logic
                item["evidence_items"] = event_logic.get("evidence_chain", [])
                events.append(item)

        snapshots: list[dict] = []
        if snapshots_exists:
            rows = conn.execute(
                """
                SELECT snapshot_at, snapshot_date, signal_key, signal_type, subject_name, ts_code, direction,
                       signal_strength, confidence, evidence_count, news_count, stock_news_count, chatroom_count,
                       signal_status, latest_signal_date, source_summary_json
                FROM investment_signal_daily_snapshots
                WHERE signal_key = ?
                ORDER BY snapshot_at DESC, id DESC
                LIMIT 60
                """,
                (signal_key,),
            ).fetchall()
            snapshots = [dict(r) for r in rows]
        signal = dict(signal_row) if signal_row else None
        signal_logic = (
            get_or_build_cached_logic_view_fn(
                conn,
                entity_type="investment_signal",
                entity_key=str(signal.get("signal_key") or ""),
                source_payload={
                    "signal_key": signal.get("signal_key"),
                    "subject_name": signal.get("subject_name"),
                    "direction": signal.get("direction"),
                    "signal_strength": signal.get("signal_strength"),
                    "confidence": signal.get("confidence"),
                    "signal_status": signal.get("signal_status"),
                    "source_summary_json": signal.get("source_summary_json"),
                    "evidence_json": signal.get("evidence_json"),
                },
                builder=lambda current=signal: build_signal_logic_view_fn(current),
            )
            if signal
            else {"summary": {}, "chains": [], "has_logic": False, "evidence_chain": []}
        )
        if signal is not None:
            signal["logic_view"] = signal_logic
    finally:
        conn.close()

    return {
        "signal": signal,
        "events": events,
        "snapshots": snapshots,
        "logic_view": signal_logic,
        "requested_signal_key": requested_signal_key,
        "resolved_signal_key": signal_key,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
    }


def query_theme_hotspots(
    *,
    sqlite3_module,
    db_path,
    cache_get_json_fn,
    cache_set_json_fn,
    redis_cache_ttl_themes: int,
    keyword: str,
    theme_group: str,
    direction: str,
    heat_level: str,
    state_filter: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    theme_group = (theme_group or "").strip()
    direction = (direction or "").strip()
    heat_level = (heat_level or "").strip()
    state_filter = (state_filter or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    cache_key = (
        "api:theme-hotspots:v1:"
        f"kw={keyword}:group={theme_group}:dir={direction}:heat={heat_level}:state={state_filter}:"
        f"page={page}:size={page_size}"
    )
    cached = cache_get_json_fn(cache_key)
    if isinstance(cached, dict):
        return cached

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        tracker_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='theme_hotspot_tracker'"
        ).fetchone()[0]
        if not tracker_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": [], "summary": {}, "filters": {}}
        where = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where.append("(t.theme_name LIKE ? OR COALESCE(t.theme_group,'') LIKE ? OR COALESCE(t.top_terms_json,'') LIKE ?)")
            params.extend([kw, kw, kw])
        if theme_group:
            where.append("t.theme_group = ?")
            params.append(theme_group)
        if direction:
            where.append("t.direction = ?")
            params.append(direction)
        if heat_level:
            where.append("t.heat_level = ?")
            params.append(heat_level)
        if state_filter:
            where.append("COALESCE(s.current_state, '') = ?")
            params.append(state_filter)
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM theme_hotspot_tracker t
            LEFT JOIN signal_state_tracker s
              ON s.signal_scope = 'theme' AND s.signal_key = ('theme:' || t.theme_name)
            {where_sql}
            """,
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
              t.theme_name, t.theme_group, t.direction, t.theme_strength, t.confidence, t.evidence_count,
              t.intl_news_count, t.domestic_news_count, t.stock_news_count, t.chatroom_count,
              t.stock_link_count, t.latest_evidence_time, t.heat_level, t.top_terms_json, t.top_stocks_json,
              t.source_summary_json, t.evidence_json,
              s.current_state, s.prev_state, s.driver_type, s.driver_title
            FROM theme_hotspot_tracker t
            LEFT JOIN signal_state_tracker s
              ON s.signal_scope = 'theme' AND s.signal_key = ('theme:' || t.theme_name)
            {where_sql}
            ORDER BY t.theme_strength DESC, t.confidence DESC, t.theme_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["market_expectations"] = _top_market_expectations_for_theme(conn, str(item.get("theme_name") or ""), limit=5)
            items.append(item)
        summary = {
            "theme_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker").fetchone()[0],
            "bullish_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE direction='看多'").fetchone()[0],
            "bearish_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE direction='看空'").fetchone()[0],
            "high_heat_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE heat_level IN ('极高','高')").fetchone()[0],
        }
        filters = {
            "theme_groups": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT theme_group FROM theme_hotspot_tracker WHERE COALESCE(theme_group,'')<>'' ORDER BY theme_group"
                ).fetchall()
            ],
            "directions": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT direction FROM theme_hotspot_tracker WHERE COALESCE(direction,'')<>'' ORDER BY direction"
                ).fetchall()
            ],
            "heat_levels": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT heat_level FROM theme_hotspot_tracker WHERE COALESCE(heat_level,'')<>'' ORDER BY heat_level"
                ).fetchall()
            ],
            "states": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT current_state FROM signal_state_tracker WHERE signal_scope='theme' AND COALESCE(current_state,'')<>'' ORDER BY current_state"
                ).fetchall()
            ],
        }
    finally:
        conn.close()
    payload = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": items,
        "summary": summary,
        "filters": filters,
    }
    cache_set_json_fn(cache_key, payload, redis_cache_ttl_themes)
    return payload


def query_signal_state_timeline(*, sqlite3_module, db_path, signal_scope: str, signal_key: str, page: int, page_size: int):
    signal_scope = (signal_scope or "").strip()
    signal_key = (signal_key or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    if not signal_key:
        raise ValueError("缺少 signal_key")
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        state_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_state_tracker'"
        ).fetchone()[0]
        event_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_state_events'"
        ).fetchone()[0]
        if not state_exists:
            return {"signal": None, "events": [], "page": page, "page_size": page_size, "total": 0, "total_pages": 0}
        where_extra = ""
        params: list[object] = [signal_key]
        if signal_scope:
            where_extra = " AND signal_scope = ?"
            params.append(signal_scope)
        signal = conn.execute(
            f"""
            SELECT *
            FROM signal_state_tracker
            WHERE signal_key = ?{where_extra}
            ORDER BY update_time DESC, id DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        total = 0
        events = []
        if event_exists:
            total = conn.execute(
                f"SELECT COUNT(*) FROM signal_state_events WHERE signal_key = ?{where_extra}",
                params,
            ).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT *
                FROM signal_state_events
                WHERE signal_key = ?{where_extra}
                ORDER BY event_time DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()
            events = [dict(r) for r in rows]
        expectations = []
        if signal and str(signal["signal_scope"] or "") == "theme":
            expectations = _top_market_expectations_for_theme(conn, str(signal["subject_name"] or ""), limit=8)
        return {
            "signal": dict(signal) if signal else None,
            "events": events,
            "market_expectations": expectations,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
        }
    finally:
        conn.close()
