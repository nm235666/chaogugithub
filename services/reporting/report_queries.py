from __future__ import annotations

import json

from .contracts import build_reporting_protocol_meta


def _parse_json_text(raw: str):
    try:
        payload = json.loads(raw or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


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


def _market_expectations_for_report(conn, report_type: str, subject_key: str, context_json_text: str, limit: int = 8):
    themes: list[str] = []
    if report_type == "theme":
        themes = [subject_key]
    elif report_type == "market":
        ctx = _parse_json_text(context_json_text) or {}
        for row in (ctx.get("theme_rows") or [])[:5]:
            if isinstance(row, (list, tuple)) and row:
                themes.append(str(row[0]))
    elif report_type == "stock":
        ctx = _parse_json_text(context_json_text) or {}
        for row in (ctx.get("themes") or [])[:5]:
            if isinstance(row, (list, tuple)) and row:
                themes.append(str(row[0]))
    if not themes:
        return []
    seen = set()
    out = []
    for theme in themes:
        for item in _top_market_expectations_for_theme(conn, theme, limit=limit):
            question = str(item.get("question") or "")
            if question in seen:
                continue
            seen.add(question)
            out.append(item)
            if len(out) >= limit:
                return out
    return out


def query_research_reports(*, sqlite3_module, db_path, report_type: str, keyword: str, report_date: str, page: int, page_size: int):
    report_type = (report_type or "").strip()
    keyword = (keyword or "").strip()
    report_date = (report_date or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='research_reports'"
        ).fetchone()[0]
        if not exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "filters": {},
                "protocol": build_reporting_protocol_meta(),
            }
        where = []
        params: list[object] = []
        if report_type:
            where.append("report_type = ?")
            params.append(report_type)
        if keyword:
            kw = f"%{keyword}%"
            where.append("(subject_key LIKE ? OR COALESCE(subject_name,'') LIKE ? OR COALESCE(markdown_content,'') LIKE ?)")
            params.extend([kw, kw, kw])
        if report_date:
            where.append("report_date = ?")
            params.append(report_date)
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        total = conn.execute(f"SELECT COUNT(*) FROM research_reports{where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT id, report_date, report_type, subject_key, subject_name, model, markdown_content, context_json, created_at, update_time
            FROM research_reports
            {where_sql}
            ORDER BY report_date DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            analysis_markdown = str(item.get("markdown_content") or "")
            # Unified protocol: analysis_markdown is primary; markdown_content kept as compatibility mirror.
            item["analysis_markdown"] = analysis_markdown
            item["markdown_content"] = analysis_markdown
            item["market_expectations"] = _market_expectations_for_report(
                conn,
                str(item.get("report_type") or ""),
                str(item.get("subject_key") or ""),
                str(item.get("context_json") or ""),
                limit=6,
            )
            items.append(item)
        filters = {
            "report_types": [r[0] for r in conn.execute("SELECT DISTINCT report_type FROM research_reports ORDER BY report_type").fetchall()],
            "report_dates": [r[0] for r in conn.execute("SELECT DISTINCT report_date FROM research_reports ORDER BY report_date DESC LIMIT 30").fetchall()],
        }
    finally:
        conn.close()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": items,
        "filters": filters,
        "protocol": build_reporting_protocol_meta(),
    }
