from __future__ import annotations

import json
import re
from datetime import datetime, timezone


def query_signal_audit(
    *,
    sqlite3_module,
    db_path,
    resolve_signal_table_fn,
    scope: str = "7d",
):
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
                "scope": normalized_scope,
                "table_name": table_name,
                "summary": {},
                "stats": {},
                "sections": {},
                "generated_at": generated_at,
            }
        rows = conn.execute(
            f"""
            SELECT signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                   evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                   latest_signal_date, evidence_json, source_summary_json
            FROM {table_name}
            ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC
            """
        ).fetchall()
        stock_name_map = {
            str(r[0] or "").strip().upper(): str(r[1] or "").strip()
            for r in conn.execute("SELECT ts_code, name FROM stock_codes").fetchall()
        }
    finally:
        conn.close()

    def parse_obj(raw):
        try:
            obj = json.loads(raw or "{}")
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def parse_list(raw):
        try:
            obj = json.loads(raw or "[]")
            return obj if isinstance(obj, list) else []
        except Exception:
            return []

    def top_evidence_title(item: dict) -> str:
        evidences = parse_list(item.get("evidence_json"))
        if evidences:
            ev = evidences[0] if isinstance(evidences[0], dict) else {}
            return str(ev.get("title") or ev.get("theme_name") or ev.get("source") or "").strip()
        return ""

    def dominant_source(item: dict) -> str:
        source_summary = parse_obj(item.get("source_summary_json"))
        counts = {
            "国际新闻": int(source_summary.get("intl_news", 0)),
            "国内新闻": int(source_summary.get("domestic_news", 0)),
            "个股新闻": int(source_summary.get("stock_news", 0)),
            "群聊": int(source_summary.get("chatroom", 0)),
            "主题映射": int(source_summary.get("theme_mapping", 0)),
        }
        best = max(counts.items(), key=lambda kv: kv[1]) if counts else ("无", 0)
        return best[0] if best[1] > 0 else "无"

    all_items = [dict(r) for r in rows]
    stock_items = [r for r in all_items if str(r.get("signal_type") or "") == "stock"]
    active_items = [r for r in all_items if str(r.get("signal_status") or "") == "活跃"]
    code_name_re = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")

    code_named_stocks = []
    theme_only_stocks = []
    low_conf_active = []
    missing_ts_stock = []
    weak_chatroom_stocks = []
    no_direct_source_stocks = []

    for item in stock_items:
        subject_name = str(item.get("subject_name") or "").strip()
        ts_code = str(item.get("ts_code") or "").strip().upper()
        source_summary = parse_obj(item.get("source_summary_json"))
        news_ct = int(item.get("news_count") or 0)
        stock_news_ct = int(item.get("stock_news_count") or 0)
        chatroom_ct = int(item.get("chatroom_count") or 0)
        theme_ct = int(source_summary.get("theme_mapping", 0))
        confidence = float(item.get("confidence") or 0.0)
        strength = float(item.get("signal_strength") or 0.0)
        base = {
            "signal_key": item.get("signal_key"),
            "subject_name": subject_name,
            "display_name": stock_name_map.get(ts_code) or subject_name or ts_code,
            "ts_code": ts_code,
            "direction": item.get("direction"),
            "signal_strength": strength,
            "confidence": confidence,
            "signal_status": item.get("signal_status"),
            "latest_signal_date": item.get("latest_signal_date"),
            "dominant_source": dominant_source(item),
            "top_evidence": top_evidence_title(item),
            "news_count": news_ct,
            "stock_news_count": stock_news_ct,
            "chatroom_count": chatroom_ct,
            "theme_mapping_count": theme_ct,
        }
        if code_name_re.fullmatch(subject_name):
            code_named_stocks.append(base)
        if not ts_code:
            missing_ts_stock.append(base)
        if news_ct == 0 and stock_news_ct == 0 and chatroom_ct == 0 and theme_ct > 0:
            theme_only_stocks.append(base)
        if str(item.get("signal_status") or "") == "活跃" and confidence < 40:
            low_conf_active.append(base)
        if chatroom_ct > 0 and strength < 3.0:
            weak_chatroom_stocks.append(base)
        if news_ct == 0 and stock_news_ct == 0 and chatroom_ct == 0:
            no_direct_source_stocks.append(base)

    def sort_rows(items: list[dict]) -> list[dict]:
        return sorted(
            items,
            key=lambda x: (-float(x.get("signal_strength") or 0.0), -float(x.get("confidence") or 0.0), str(x.get("display_name") or "")),
        )

    summary = {
        "signal_total": len(all_items),
        "stock_total": len(stock_items),
        "active_total": len(active_items),
        "chatroom_stock_total": sum(1 for x in stock_items if int(x.get("chatroom_count") or 0) > 0),
        "theme_only_stock_total": len(theme_only_stocks),
        "code_named_stock_total": len(code_named_stocks),
        "low_conf_active_total": len(low_conf_active),
        "missing_ts_stock_total": len(missing_ts_stock),
    }
    stats = {
        "scope_label": "最近1天" if normalized_scope == "1d" else "近7天累计" if normalized_scope == "7d" else "主表",
        "table_name": table_name,
        "quality_score": max(
            0,
            min(
                100,
                int(
                    100
                    - len(code_named_stocks) * 2
                    - len(missing_ts_stock) * 4
                    - len(low_conf_active) * 3
                    - len(theme_only_stocks)
                ),
            ),
        ),
    }
    sections = {
        "code_named_stocks": sort_rows(code_named_stocks)[:30],
        "missing_ts_stock": sort_rows(missing_ts_stock)[:30],
        "theme_only_stocks": sort_rows(theme_only_stocks)[:30],
        "low_conf_active": sort_rows(low_conf_active)[:30],
        "weak_chatroom_stocks": sort_rows(weak_chatroom_stocks)[:30],
        "no_direct_source_stocks": sort_rows(no_direct_source_stocks)[:30],
    }
    return {
        "scope": normalized_scope,
        "table_name": table_name,
        "summary": summary,
        "stats": stats,
        "sections": sections,
        "generated_at": generated_at,
    }


def query_signal_quality_config(*, sqlite3_module, db_path):
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        rules_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_quality_rules'"
        ).fetchone()[0]
        block_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_mapping_blocklist'"
        ).fetchone()[0]
        rules = []
        blocklist = []
        if rules_exists:
            rules = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT rule_key, rule_value, value_type, category, description, enabled, update_time
                    FROM signal_quality_rules
                    ORDER BY category, rule_key
                    """
                ).fetchall()
            ]
        if block_exists:
            blocklist = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, term, target_type, match_type, source, reason, enabled, update_time
                    FROM signal_mapping_blocklist
                    ORDER BY enabled DESC, target_type, match_type, term
                    """
                ).fetchall()
            ]
    finally:
        conn.close()
    return {
        "rules": rules,
        "blocklist": blocklist,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def save_signal_quality_rules(*, sqlite3_module, db_path, items: list[dict]):
    conn = sqlite3_module.connect(db_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_quality_rules (
                rule_key TEXT PRIMARY KEY,
                rule_value TEXT,
                value_type TEXT DEFAULT 'number',
                category TEXT,
                description TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT,
                update_time TEXT
            )
            """
        )
        affected = 0
        for item in items:
            rule_key = str(item.get("rule_key") or "").strip()
            if not rule_key:
                continue
            rule_value = str(item.get("rule_value") if item.get("rule_value") is not None else "").strip()
            value_type = str(item.get("value_type") or "number").strip()
            category = str(item.get("category") or "").strip()
            description = str(item.get("description") or "").strip()
            enabled = 1 if str(item.get("enabled", 1)).strip().lower() not in {"0", "false", "no"} else 0
            updated = conn.execute(
                """
                UPDATE signal_quality_rules
                SET rule_value = ?, value_type = ?, category = ?, description = ?, enabled = ?, update_time = ?
                WHERE rule_key = ?
                """,
                (rule_value, value_type, category, description, enabled, now, rule_key),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_quality_rules (
                        rule_key, rule_value, value_type, category, description, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (rule_key, rule_value, value_type, category, description, enabled, now, now),
                )
            affected += 1
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "affected": affected, "updated_at": now}


def save_signal_mapping_blocklist(*, sqlite3_module, db_path, items: list[dict]):
    conn = sqlite3_module.connect(db_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_mapping_blocklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term TEXT NOT NULL,
                target_type TEXT NOT NULL DEFAULT 'stock',
                match_type TEXT NOT NULL DEFAULT 'exact',
                source TEXT,
                reason TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT,
                update_time TEXT,
                UNIQUE(term, target_type, match_type)
            )
            """
        )
        affected = 0
        for item in items:
            term = str(item.get("term") or "").strip()
            if not term:
                continue
            target_type = str(item.get("target_type") or "stock").strip()
            match_type = str(item.get("match_type") or "exact").strip()
            source = str(item.get("source") or "signal_quality_admin").strip()
            reason = str(item.get("reason") or "").strip()
            enabled = 1 if str(item.get("enabled", 1)).strip().lower() not in {"0", "false", "no"} else 0
            updated = conn.execute(
                """
                UPDATE signal_mapping_blocklist
                SET source = ?, reason = ?, enabled = ?, update_time = ?
                WHERE term = ? AND target_type = ? AND match_type = ?
                """,
                (source, reason, enabled, now, term, target_type, match_type),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_mapping_blocklist (
                        term, target_type, match_type, source, reason, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (term, target_type, match_type, source, reason, enabled, now, now),
                )
            affected += 1
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "affected": affected, "updated_at": now}
