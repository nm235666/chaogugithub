#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import db_compat as sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
PROMPT_VERSION = "chatroom_tag_v1"
TAG_HISTORY_TABLE = "chatroom_tag_history"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用大模型为群聊总结并打分类标签")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）")
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    parser.add_argument("--days", type=int, default=30, help="取最近多少天聊天记录用于打标")
    parser.add_argument("--limit", type=int, default=20, help="本次最多处理多少个群")
    parser.add_argument("--only-room", default="", help="只处理指定群聊（remark/nick_name/room_id/talker）")
    parser.add_argument("--include-skipped", action="store_true", help="是否包含已停监控群")
    parser.add_argument("--force", action="store_true", help="强制重跑，默认只处理未打标或标签过旧的群")
    parser.add_argument("--monitored-only", action="store_true", help="仅处理仍在监控中的群")
    parser.add_argument("--retag-days", type=int, default=7, help="距离上次打标签超过多少天才允许自动重跑；0=不限制")
    parser.add_argument("--min-confidence", type=int, default=78, help="低于该置信度的结果只入历史，不覆盖当前标签")
    parser.add_argument("--history-window", type=int, default=7, help="用于稳定标签的最近历史样本数")
    parser.add_argument("--majority-threshold", type=float, default=0.62, help="新标签想覆盖当前标签时所需多数占比")
    parser.add_argument("--min-majority-count", type=int, default=3, help="新标签想覆盖当前标签时所需最少支持样本数")
    parser.add_argument("--max-messages", type=int, default=120, help="每个群最多送入模型的消息条数")
    parser.add_argument("--max-chars", type=int, default=12000, help="每个群送入模型的最大字符数")
    parser.add_argument("--sleep", type=float, default=0.3, help="每个群之间暂停秒数")
    parser.add_argument("--retry", type=int, default=2, help="单个群失败重试次数")
    parser.add_argument("--verbose", action="store_true", help="打印更多上下文")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(chatroom_list_items)").fetchall()}
    need = [
        ("llm_chatroom_summary", "TEXT"),
        ("llm_chatroom_tags_json", "TEXT"),
        ("llm_chatroom_primary_category", "TEXT"),
        ("llm_chatroom_activity_level", "TEXT"),
        ("llm_chatroom_risk_level", "TEXT"),
        ("llm_chatroom_confidence", "INTEGER"),
        ("llm_chatroom_model", "TEXT"),
        ("llm_chatroom_tagged_at", "TEXT"),
        ("llm_chatroom_prompt_version", "TEXT"),
        ("llm_chatroom_raw_output", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE chatroom_list_items ADD COLUMN {name} {typ}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chatroom_list_items_llm_tagged_at ON chatroom_list_items(llm_chatroom_tagged_at)"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TAG_HISTORY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            talker TEXT,
            sample_start_date TEXT,
            sample_end_date TEXT,
            sample_message_count INTEGER,
            proposed_primary_category TEXT,
            proposed_activity_level TEXT,
            proposed_risk_level TEXT,
            proposed_confidence INTEGER,
            proposed_summary TEXT,
            proposed_tags_json TEXT,
            all_tags_text TEXT,
            model TEXT,
            prompt_version TEXT,
            raw_output TEXT,
            is_applied INTEGER DEFAULT 0,
            apply_reason TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TAG_HISTORY_TABLE}_room_created ON {TAG_HISTORY_TABLE}(room_id, created_at DESC)"
    )
    conn.commit()


def build_room_filter_sql(args: argparse.Namespace) -> tuple[str, list[object]]:
    where = []
    params: list[object] = []
    if args.only_room.strip():
        value = args.only_room.strip()
        where.append("(c.remark = ? OR c.nick_name = ? OR c.room_id = ? OR l.talker = ?)")
        params.extend([value, value, value, value])
    if args.monitored_only or not args.include_skipped:
        where.append("COALESCE(c.skip_realtime_monitor, 0) = 0")
    if not args.force:
        if args.retag_days > 0:
            where.append(
                "("
                "c.llm_chatroom_tagged_at IS NULL "
                "OR c.llm_chatroom_prompt_version IS NULL "
                "OR c.llm_chatroom_prompt_version <> ? "
                "OR COALESCE(c.llm_chatroom_tagged_at, '') < ?"
                ")"
            )
            params.append(PROMPT_VERSION)
            cutoff = (datetime.now(timezone.utc) - timedelta(days=max(args.retag_days, 1))).strftime("%Y-%m-%dT%H:%M:%SZ")
            params.append(cutoff)
        else:
            where.append(
                "("
                "c.llm_chatroom_tagged_at IS NULL "
                "OR c.llm_chatroom_prompt_version IS NULL "
                "OR c.llm_chatroom_prompt_version <> ?"
                ")"
            )
            params.append(PROMPT_VERSION)
    return (" WHERE " + " AND ".join(where) if where else ""), params


def fetch_candidate_rooms(conn: sqlite3.Connection, args: argparse.Namespace) -> list[sqlite3.Row]:
    where_sql, params = build_room_filter_sql(args)
    sql = f"""
    SELECT
        c.room_id,
        c.remark,
        c.nick_name,
        c.user_count,
        c.last_message_date,
        COUNT(l.id) AS message_count,
        MAX(l.message_date) AS max_message_date
    FROM chatroom_list_items c
    JOIN wechat_chatlog_clean_items l
      ON l.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
    {where_sql}
    GROUP BY c.room_id, c.remark, c.nick_name, c.user_count, c.last_message_date
    ORDER BY MAX(l.message_date) DESC, COUNT(l.id) DESC
    LIMIT ?
    """
    return conn.execute(sql, [*params, max(args.limit, 1)]).fetchall()


def room_display_name(row: sqlite3.Row) -> str:
    return str(row["remark"] or row["nick_name"] or row["room_id"] or "").strip()


def fetch_recent_messages(
    conn: sqlite3.Connection,
    talker: str,
    days: int,
    max_messages: int,
) -> list[sqlite3.Row]:
    start_date = (datetime.now(timezone.utc).date() - timedelta(days=max(days - 1, 0))).isoformat()
    sql = """
    SELECT
        message_date,
        message_time,
        sender_name,
        message_type,
        content_clean,
        is_quote,
        quote_sender_name,
        quote_content
    FROM wechat_chatlog_clean_items
    WHERE talker = ?
      AND COALESCE(message_date, '') >= ?
    ORDER BY message_date DESC, message_time DESC, id DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (talker, start_date, max_messages)).fetchall()
    return list(reversed(rows))


def build_message_digest(rows: list[sqlite3.Row], max_chars: int) -> str:
    parts: list[str] = []
    remaining = max_chars
    for row in rows:
        content = str(row["content_clean"] or "").strip()
        if not content:
            continue
        sender = str(row["sender_name"] or "未知发送人").strip()
        message_type = str(row["message_type"] or "text").strip()
        msg = f"[{row['message_date']} {row['message_time']}] {sender} ({message_type}): {content}"
        if int(row["is_quote"] or 0) == 1:
            quote_sender = str(row["quote_sender_name"] or "").strip()
            quote_content = str(row["quote_content"] or "").strip()
            if quote_sender or quote_content:
                msg += f" | 引用 {quote_sender}: {quote_content}"
        if len(msg) > remaining:
            if remaining > 50:
                parts.append(msg[:remaining])
            break
        parts.append(msg)
        remaining -= len(msg) + 1
        if remaining <= 0:
            break
    return "\n".join(parts)


def build_prompt(room: dict) -> str:
    return (
        "你是一个专业的信息分析助手，需要根据一个微信群最近的聊天记录，为该群做“分类标签”和“内容摘要”。\n"
        "请基于聊天内容本身判断，不要臆测。\n\n"
        "任务要求：\n"
        "1. 判断群聊主要主题与用途。\n"
        "2. 给出适合检索和筛选的分类标签。\n"
        "3. 判断群聊活跃度和风险等级。\n"
        "4. 输出一段简洁摘要，说明这个群大致在聊什么。\n\n"
        "标签输出要求：\n"
        "- primary_category: 只能从以下中选择一个：投资交易、财经资讯、技术交流、课程培训、电商团购、生活社交、行业社群、游戏娱乐、本地组织、其他\n"
        "- topic_tags: 3到8个中文短标签，例如 A股、短线交易、量化、逆向、安全、团购、海鲜、AI、课程、八卦\n"
        "- style_tags: 1到5个标签，例如 高活跃、低活跃、消息密集、问答型、观点输出、广告导向、资讯转发、实战讨论、闲聊\n"
        "- risk_level: 只能是 低 / 中 / 高\n"
        "- activity_level: 只能是 低 / 中 / 高\n"
        "- confidence: 0到100 的整数\n"
        "- summary: 80字以内中文总结\n\n"
        "只输出 JSON，不要输出任何多余文字，格式如下：\n"
        "{\n"
        '  "primary_category": "投资交易",\n'
        '  "topic_tags": ["A股", "短线交易", "龙虎榜"],\n'
        '  "style_tags": ["高活跃", "实战讨论", "观点输出"],\n'
        '  "risk_level": "中",\n'
        '  "activity_level": "高",\n'
        '  "confidence": 88,\n'
        '  "summary": "这是一个以A股短线和题材交易为主的高活跃讨论群。"\n'
        "}\n\n"
        f"输入数据：\n{json.dumps(room, ensure_ascii=False)}"
    )


def call_llm(model: str, temperature: float, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=180,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨、克制、结构化的信息标签引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def extract_json_text(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return "{}"
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    match = re.search(r"\{.*\}", text, flags=re.S)
    return match.group(0) if match else text


def clamp_confidence(value: object) -> int:
    try:
        n = int(round(float(value)))
    except Exception:
        n = 0
    return max(0, min(100, n))


def normalize_tag_list(value: object, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def parse_llm_output(raw: str) -> dict:
    text = extract_json_text(raw)
    obj = json.loads(text)
    primary_category = str(obj.get("primary_category") or "其他").strip() or "其他"
    risk_level = str(obj.get("risk_level") or "中").strip() or "中"
    activity_level = str(obj.get("activity_level") or "中").strip() or "中"
    summary = str(obj.get("summary") or "").strip()[:200]
    topic_tags = normalize_tag_list(obj.get("topic_tags"), 8)
    style_tags = normalize_tag_list(obj.get("style_tags"), 5)
    merged_tags = []
    for tag in topic_tags + style_tags:
        if tag not in merged_tags:
            merged_tags.append(tag)
    return {
        "primary_category": primary_category,
        "topic_tags": topic_tags,
        "style_tags": style_tags,
        "tags_json": json.dumps(
            {
                "topic_tags": topic_tags,
                "style_tags": style_tags,
                "all_tags": merged_tags,
            },
            ensure_ascii=False,
        ),
        "risk_level": risk_level,
        "activity_level": activity_level,
        "confidence": clamp_confidence(obj.get("confidence")),
        "summary": summary,
    }


def parse_tags_json_text(raw: object) -> dict:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def fetch_recent_tag_history(conn: sqlite3.Connection, room_id: str, limit: int) -> list[sqlite3.Row]:
    sql = f"""
    SELECT id, room_id, proposed_primary_category, proposed_activity_level, proposed_risk_level,
           proposed_confidence, proposed_summary, proposed_tags_json, is_applied, created_at
    FROM {TAG_HISTORY_TABLE}
    WHERE room_id = ?
    ORDER BY created_at DESC, id DESC
    LIMIT ?
    """
    return conn.execute(sql, (room_id, max(limit, 1))).fetchall()


def aggregate_tags(rows: list[sqlite3.Row], category: str, fallback_tags_json: str) -> str:
    scores: dict[str, float] = {}
    topic_scores: dict[str, float] = {}
    style_scores: dict[str, float] = {}
    matched = 0
    for row in rows:
        if str(row["proposed_primary_category"] or "").strip() != category:
            continue
        matched += 1
        weight = max(0.3, float(row["proposed_confidence"] or 0) / 100.0)
        tags_obj = parse_tags_json_text(row["proposed_tags_json"])
        for tag in tags_obj.get("all_tags") or []:
            tag_text = str(tag or "").strip()
            if tag_text:
                scores[tag_text] = scores.get(tag_text, 0.0) + weight
        for tag in tags_obj.get("topic_tags") or []:
            tag_text = str(tag or "").strip()
            if tag_text:
                topic_scores[tag_text] = topic_scores.get(tag_text, 0.0) + weight
        for tag in tags_obj.get("style_tags") or []:
            tag_text = str(tag or "").strip()
            if tag_text:
                style_scores[tag_text] = style_scores.get(tag_text, 0.0) + weight
    if matched <= 0:
        return fallback_tags_json
    topic_tags = [k for k, _ in sorted(topic_scores.items(), key=lambda item: (-item[1], item[0]))[:8]]
    style_tags = [k for k, _ in sorted(style_scores.items(), key=lambda item: (-item[1], item[0]))[:5]]
    all_tags = [k for k, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:10]]
    return json.dumps(
        {"topic_tags": topic_tags, "style_tags": style_tags, "all_tags": all_tags},
        ensure_ascii=False,
    )


def choose_stable_tagging(
    current_row: sqlite3.Row,
    candidate: dict,
    history_rows: list[sqlite3.Row],
    args: argparse.Namespace,
) -> tuple[dict, bool, str]:
    current_category = str(current_row["llm_chatroom_primary_category"] or "").strip()
    current_confidence = int(current_row["llm_chatroom_confidence"] or 0)
    all_rows = [
        {
            "proposed_primary_category": candidate["primary_category"],
            "proposed_activity_level": candidate["activity_level"],
            "proposed_risk_level": candidate["risk_level"],
            "proposed_confidence": candidate["confidence"],
            "proposed_summary": candidate["summary"],
            "proposed_tags_json": candidate["tags_json"],
        }
    ]
    all_rows.extend([dict(r) for r in history_rows[: max(args.history_window - 1, 0)]])

    category_stats: dict[str, dict[str, float]] = {}
    for row in all_rows:
        category = str(row.get("proposed_primary_category") or "").strip()
        if not category:
            continue
        conf = max(1, int(row.get("proposed_confidence") or 0))
        stat = category_stats.setdefault(category, {"count": 0.0, "weight": 0.0})
        stat["count"] += 1
        stat["weight"] += conf / 100.0

    candidate_category = candidate["primary_category"]
    candidate_count = int(category_stats.get(candidate_category, {}).get("count", 0))
    total_count = max(sum(int(v["count"]) for v in category_stats.values()), 1)
    candidate_ratio = candidate_count / total_count

    if candidate["confidence"] < args.min_confidence:
        return current_snapshot_from_row(current_row), False, f"confidence<{args.min_confidence}"

    if not current_category:
        stable = dict(candidate)
        stable["tags_json"] = aggregate_tags(history_rows + [row_to_history_row(candidate)], candidate_category, candidate["tags_json"])
        return stable, True, "initial_apply"

    if candidate_category == current_category:
        stable = dict(candidate)
        stable["primary_category"] = current_category
        stable["confidence"] = max(candidate["confidence"], current_confidence)
        stable["tags_json"] = aggregate_tags(history_rows + [row_to_history_row(candidate)], current_category, candidate["tags_json"])
        return stable, True, "same_category_refresh"

    if candidate_count >= args.min_majority_count and candidate_ratio >= args.majority_threshold:
        stable = dict(candidate)
        stable["tags_json"] = aggregate_tags(history_rows + [row_to_history_row(candidate)], candidate_category, candidate["tags_json"])
        return stable, True, f"switch_by_majority:{candidate_count}/{total_count}"

    return current_snapshot_from_row(current_row), False, f"keep_current:{current_category}:{candidate_count}/{total_count}"


def row_to_history_row(parsed: dict) -> sqlite3.Row | dict:
    return {
        "proposed_primary_category": parsed["primary_category"],
        "proposed_activity_level": parsed["activity_level"],
        "proposed_risk_level": parsed["risk_level"],
        "proposed_confidence": parsed["confidence"],
        "proposed_summary": parsed["summary"],
        "proposed_tags_json": parsed["tags_json"],
    }


def current_snapshot_from_row(row: sqlite3.Row) -> dict:
    tags_json = str(row["llm_chatroom_tags_json"] or "").strip() or json.dumps(
        {"topic_tags": [], "style_tags": [], "all_tags": []},
        ensure_ascii=False,
    )
    return {
        "primary_category": str(row["llm_chatroom_primary_category"] or "其他").strip() or "其他",
        "activity_level": str(row["llm_chatroom_activity_level"] or "中").strip() or "中",
        "risk_level": str(row["llm_chatroom_risk_level"] or "中").strip() or "中",
        "confidence": int(row["llm_chatroom_confidence"] or 0),
        "summary": str(row["llm_chatroom_summary"] or "").strip()[:200],
        "tags_json": tags_json,
    }


def insert_tag_history(
    conn: sqlite3.Connection,
    room_id: str,
    talker: str,
    sample_start_date: str,
    sample_end_date: str,
    sample_message_count: int,
    parsed: dict,
    model: str,
    raw_output: str,
    is_applied: bool,
    apply_reason: str,
) -> None:
    tags_obj = parse_tags_json_text(parsed["tags_json"])
    all_tags_text = ",".join([str(x or "").strip() for x in tags_obj.get("all_tags") or [] if str(x or "").strip()])
    conn.execute(
        f"""
        INSERT INTO {TAG_HISTORY_TABLE} (
            room_id, talker, sample_start_date, sample_end_date, sample_message_count,
            proposed_primary_category, proposed_activity_level, proposed_risk_level, proposed_confidence,
            proposed_summary, proposed_tags_json, all_tags_text,
            model, prompt_version, raw_output, is_applied, apply_reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            room_id,
            talker,
            sample_start_date,
            sample_end_date,
            sample_message_count,
            parsed["primary_category"],
            parsed["activity_level"],
            parsed["risk_level"],
            parsed["confidence"],
            parsed["summary"],
            parsed["tags_json"],
            all_tags_text,
            model,
            PROMPT_VERSION,
            raw_output,
            1 if is_applied else 0,
            apply_reason,
            now_utc_str(),
        ),
    )
    conn.commit()


def update_room_tagging(
    conn: sqlite3.Connection,
    room_id: str,
    parsed: dict,
    model: str,
    raw_output: str,
) -> None:
    conn.execute(
        """
        UPDATE chatroom_list_items
        SET
            llm_chatroom_summary = ?,
            llm_chatroom_tags_json = ?,
            llm_chatroom_primary_category = ?,
            llm_chatroom_activity_level = ?,
            llm_chatroom_risk_level = ?,
            llm_chatroom_confidence = ?,
            llm_chatroom_model = ?,
            llm_chatroom_tagged_at = ?,
            llm_chatroom_prompt_version = ?,
            llm_chatroom_raw_output = ?
        WHERE room_id = ?
        """,
        (
            parsed["summary"],
            parsed["tags_json"],
            parsed["primary_category"],
            parsed["activity_level"],
            parsed["risk_level"],
            parsed["confidence"],
            model,
            now_utc_str(),
            PROMPT_VERSION,
            raw_output,
            room_id,
        ),
    )
    conn.commit()


def fetch_current_room_row(conn: sqlite3.Connection, room_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT room_id, llm_chatroom_summary, llm_chatroom_tags_json, llm_chatroom_primary_category,
               llm_chatroom_activity_level, llm_chatroom_risk_level, llm_chatroom_confidence
        FROM chatroom_list_items
        WHERE room_id = ?
        LIMIT 1
        """,
        (room_id,),
    ).fetchone()


def main() -> int:
    args = parse_args()
    model = normalize_model_name(args.model)
    temperature = normalize_temperature_for_model(model, args.temperature)

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_columns(conn)
        rooms = fetch_candidate_rooms(conn, args)
        if not rooms:
            print("没有需要处理的群聊。")
            return 0

        print(f"待处理群聊数: {len(rooms)} | 模型: {model}")
        for idx, room in enumerate(rooms, start=1):
            talker = room_display_name(room)
            room_id = str(room["room_id"] or "")
            recent_messages = fetch_recent_messages(conn, talker, args.days, args.max_messages)
            if not recent_messages:
                print(f"[{idx}/{len(rooms)}] {talker}: skip, 最近{args.days}天没有聊天记录")
                continue

            digest = build_message_digest(recent_messages, args.max_chars)
            payload = {
                "room_id": room_id,
                "talker": talker,
                "user_count": room["user_count"],
                "message_count_in_db_window": room["message_count"],
                "latest_message_date": room["max_message_date"],
                "recent_messages_sample": digest,
            }
            prompt = build_prompt(payload)

            if args.verbose:
                print(f"[{idx}/{len(rooms)}] {talker}: messages={len(recent_messages)} digest_chars={len(digest)}")
            else:
                print(f"[{idx}/{len(rooms)}] {talker}: 分析中...")

            last_error = None
            parsed = None
            raw_output = ""
            raw_result = None
            for _ in range(max(args.retry, 0) + 1):
                try:
                    raw_result = call_llm(model, temperature, prompt)
                    raw_output = raw_result.text
                    parsed = parse_llm_output(raw_output)
                    break
                except Exception as exc:
                    last_error = exc
                    time.sleep(1.0)
            if parsed is None:
                print(f"  失败: {last_error}")
                continue

            current_row = fetch_current_room_row(conn, room_id)
            history_rows = fetch_recent_tag_history(conn, room_id, args.history_window)
            stable_parsed, should_apply, apply_reason = choose_stable_tagging(current_row, parsed, history_rows, args) if current_row else (parsed, True, "initial_apply")
            insert_tag_history(
                conn,
                room_id=room_id,
                talker=talker,
                sample_start_date=str(recent_messages[0]["message_date"] or ""),
                sample_end_date=str(recent_messages[-1]["message_date"] or ""),
                sample_message_count=len(recent_messages),
                parsed=parsed,
                model=raw_result.used_model if raw_result else model,
                raw_output=raw_output,
                is_applied=should_apply,
                apply_reason=apply_reason,
            )
            if should_apply:
                update_room_tagging(conn, room_id, stable_parsed, raw_result.used_model if raw_result else model, raw_output)
            tags_obj = json.loads((stable_parsed if should_apply else parsed)["tags_json"])
            print(
                f"  {'应用' if should_apply else '保留'} "
                f"category={(stable_parsed if should_apply else parsed)['primary_category']} "
                f"activity={(stable_parsed if should_apply else parsed)['activity_level']} "
                f"risk={(stable_parsed if should_apply else parsed)['risk_level']} "
                f"reason={apply_reason} "
                f"tags={','.join(tags_obj.get('all_tags', [])[:5])} "
                f"actual_model={(raw_result.used_model if raw_result else model)}"
            )
            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
