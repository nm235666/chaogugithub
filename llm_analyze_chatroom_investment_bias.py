#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import db_compat as sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
DEFAULT_TABLE_NAME = "chatroom_investment_analysis"
PROMPT_VERSION = "chatroom_investment_bias_v1"
TARGET_BIAS_VALUES = {"看多", "看空"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按群维度分析聊天记录中的投资标的与整体投资倾向")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）")
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME, help="结果表名")
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    parser.add_argument("--days", type=int, default=30, help="分析最近多少天聊天记录")
    parser.add_argument("--limit", type=int, default=20, help="本次最多处理多少个群")
    parser.add_argument("--only-room", default="", help="只处理指定群聊（remark/nick_name/room_id/talker）")
    parser.add_argument("--include-skipped", action="store_true", help="是否包含已停监控群")
    parser.add_argument(
        "--primary-category",
        default="投资交易",
        help="只分析指定主分类的群，默认仅分析 投资交易；传空字符串可不过滤",
    )
    parser.add_argument("--force", action="store_true", help="强制重跑，默认按消息最新日期判断是否需要重跑")
    parser.add_argument("--max-messages", type=int, default=180, help="每个群最多送入模型的消息条数")
    parser.add_argument("--max-chars", type=int, default=18000, help="每个群送入模型的最大字符数")
    parser.add_argument("--sleep", type=float, default=0.3, help="每个群之间暂停秒数")
    parser.add_argument("--retry", type=int, default=2, help="单个群失败重试次数")
    parser.add_argument("--verbose", action="store_true", help="打印更多上下文")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            talker TEXT NOT NULL,
            analysis_date TEXT NOT NULL,
            analysis_window_days INTEGER NOT NULL,
            message_count INTEGER DEFAULT 0,
            sender_count INTEGER DEFAULT 0,
            latest_message_date TEXT,
            room_summary TEXT,
            targets_json TEXT,
            final_bias TEXT,
            llm_sentiment_score REAL,
            llm_sentiment_label TEXT,
            llm_sentiment_reason TEXT,
            llm_sentiment_confidence REAL,
            llm_sentiment_model TEXT,
            llm_sentiment_scored_at TEXT,
            model TEXT,
            prompt_version TEXT,
            raw_output TEXT,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_room_window
        ON {table_name}(room_id, analysis_date, analysis_window_days)
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_talker_date ON {table_name}(talker, analysis_date)"
    )
    conn.commit()


def build_room_filter_sql(
    only_room: str,
    include_skipped: bool,
    primary_category: str,
) -> tuple[str, list[object]]:
    where = []
    params: list[object] = []
    if only_room.strip():
        value = only_room.strip()
        where.append("(c.remark = ? OR c.nick_name = ? OR c.room_id = ? OR l.talker = ?)")
        params.extend([value, value, value, value])
    if not include_skipped:
        where.append("COALESCE(c.skip_realtime_monitor, 0) = 0")
    if primary_category.strip():
        where.append("COALESCE(c.llm_chatroom_primary_category, '') = ?")
        params.append(primary_category.strip())
    return (" WHERE " + " AND ".join(where) if where else ""), params


def fetch_candidate_rooms(conn: sqlite3.Connection, args: argparse.Namespace, table_name: str) -> list[sqlite3.Row]:
    where_sql, params = build_room_filter_sql(
        args.only_room,
        args.include_skipped,
        args.primary_category,
    )
    aggregated_sql = f"""
    SELECT
        c.room_id AS room_id,
        c.remark AS remark,
        c.nick_name AS nick_name,
        COUNT(l.id) AS message_count,
        COUNT(DISTINCT COALESCE(l.sender_name, '')) AS sender_count,
        MAX(l.message_date) AS latest_message_date
    FROM chatroom_list_items c
    JOIN wechat_chatlog_clean_items l
      ON l.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
    {where_sql}
    GROUP BY c.room_id, c.remark, c.nick_name
    """

    outer_where = ["latest_message_date IS NOT NULL"]
    outer_params: list[object] = list(params)
    if not args.force:
        outer_where.append(
            f"""
            NOT EXISTS (
                SELECT 1
                FROM {table_name} a
                WHERE a.room_id = base.room_id
                  AND a.analysis_date = base.latest_message_date
                  AND a.analysis_window_days = ?
                  AND a.prompt_version = ?
            )
            """
        )
        outer_params.extend([max(args.days, 1), PROMPT_VERSION])

    outer_where_sql = " WHERE " + " AND ".join(outer_where)
    sql = f"""
    SELECT *
    FROM ({aggregated_sql}) base
    {outer_where_sql}
    ORDER BY latest_message_date DESC, message_count DESC
    LIMIT ?
    """
    return conn.execute(sql, [*outer_params, max(args.limit, 1)]).fetchall()


def room_display_name(row: sqlite3.Row) -> str:
    return str(row["remark"] or row["nick_name"] or row["room_id"] or "").strip()


def fetch_recent_messages(
    conn: sqlite3.Connection,
    talker: str,
    days: int,
    max_messages: int,
) -> list[sqlite3.Row]:
    start_date = (datetime.now(timezone.utc).date() - timedelta(days=max(days - 1, 0))).isoformat()
    rows = conn.execute(
        """
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
        """,
        (talker, start_date, max_messages),
    ).fetchall()
    return list(reversed(rows))


CODE_RE = re.compile(r"\b\d{6}\.(?:SZ|SH|BJ)\b", re.I)
US_TICKER_RE = re.compile(r"\b[A-Z]{2,5}\b")


def infer_candidate_markers(rows: list[sqlite3.Row], limit: int = 20) -> list[dict]:
    counter: Counter[str] = Counter()
    for row in rows:
        text = str(row["content_clean"] or "")
        for code in CODE_RE.findall(text):
            counter[code.upper()] += 1
        for token in US_TICKER_RE.findall(text):
            if token in {"A", "I", "THE", "AND", "FOR", "WITH", "GPT", "AI"}:
                continue
            counter[token.upper()] += 1
    return [{"marker": key, "count": value} for key, value in counter.most_common(limit)]


def build_message_digest(rows: list[sqlite3.Row], max_chars: int) -> str:
    parts: list[str] = []
    remaining = max_chars
    for row in rows:
        content = str(row["content_clean"] or "").strip()
        if not content:
            continue
        sender = str(row["sender_name"] or "未知发送人").strip()
        msg_type = str(row["message_type"] or "text").strip()
        msg = f"[{row['message_date']} {row['message_time']}] {sender} ({msg_type}): {content}"
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


def build_prompt(payload: dict) -> str:
    return (
        f"""
            你是一名专业的群聊投资情绪分析助手。你的任务是基于“某个群在最近一段时间内的全部消息摘要”，从群整体视角分析讨论内容、提炼投资标的，并判断群内主导投资倾向。

            请严格按照以下要求执行：

            【任务目标】
            1. 以“群”维度对全部消息内容进行整体汇总，生成简洁的群聊总结。
            2. 从全部讨论中提炼出明确提及或可直接归纳出的投资标的。
            3. 针对每个投资标的，判断群内主导观点是“看多”还是“看空”，并给出简要理由。
            4. 基于该群全部消息，判断该群当前整体主导投资倾向，并输出最终结论。

            【判定原则】
            1. 只能依据输入内容进行分析，不得编造任何未被提及或无法从内容中直接归纳出的投资标的。
            2. 如果同一标的存在分歧，必须结合讨论频次、语气强弱、观点集中度，判断当前群内“主导倾向”是看多还是看空。
            3. 若同一标的存在别名、简称、代码或同义表达，应合并为同一标的，避免重复输出。
            4. 若某标的缺乏明确方向判断依据，则不要输出该标的。
            5. final_bias 必须基于全群主要讨论方向二选一，只能输出“看多”或“看空”。

            【输出要求】
            - 只输出 JSON，不要输出任何解释、代码块、前后缀或额外文本。
            - 输出必须是合法 JSON，且能被直接解析。
            - 不要输出 null，不要省略字段，不要添加额外字段。

            【JSON格式】
            {{
            "room_summary": "字符串",
            "targets": [
                {{
                "name": "字符串",
                "bias": "看多或看空",
                "reason": "字符串"
                }}
            ],
            "final_bias": "看多或看空"
            }}

            【字段约束】
            - room_summary：中文，120字以内。
            - targets：最多 12 个，按重要度或讨论热度排序。
            - 每个 target 只能包含 name、bias、reason 三个字段。
            - bias 只能是：看多、看空。
            - final_bias 只能是：看多、看空。
            - reason：一句中文简述依据，尽量控制在 40 字以内。

            【特殊情况】
            - 若没有可提炼的明确投资标的，targets 返回 []。
            - 即使分歧很大，也必须判断群当前主导倾向，并输出 final_bias。

            输入数据：
            {json.dumps(payload, ensure_ascii=False)}
        """
    )


def call_llm(model: str, temperature: float, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=180,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨、克制、结构化的投资群聊分析引擎。"},
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


def normalize_targets(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    result: list[dict] = []
    for item in value[:12]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        bias = str(item.get("bias") or "").strip()
        reason = str(item.get("reason") or "").strip()
        if not name or bias not in TARGET_BIAS_VALUES:
            continue
        result.append({"name": name, "bias": bias, "reason": reason[:120]})
    return result


def parse_llm_output(raw: str) -> dict:
    obj = json.loads(extract_json_text(raw))
    room_summary = str(obj.get("room_summary") or "").strip()[:300]
    targets = normalize_targets(obj.get("targets"))
    final_bias = str(obj.get("final_bias") or "").strip()
    if final_bias not in TARGET_BIAS_VALUES:
        long_count = sum(1 for item in targets if item["bias"] == "看多")
        short_count = sum(1 for item in targets if item["bias"] == "看空")
        final_bias = "看多" if long_count >= short_count else "看空"
    return {
        "room_summary": room_summary,
        "targets_json": json.dumps(targets, ensure_ascii=False),
        "targets": targets,
        "final_bias": final_bias,
    }


def upsert_analysis(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    room_id: str,
    talker: str,
    analysis_date: str,
    analysis_window_days: int,
    message_count: int,
    sender_count: int,
    latest_message_date: str,
    room_summary: str,
    targets_json: str,
    final_bias: str,
    model: str,
    raw_output: str,
) -> None:
    now = now_utc_str()
    params = (
        talker,
        message_count,
        sender_count,
        latest_message_date,
        room_summary,
        targets_json,
        final_bias,
        model,
        PROMPT_VERSION,
        raw_output,
        now,
        room_id,
        analysis_date,
        analysis_window_days,
    )
    cur = conn.execute(
        f"""
        UPDATE {table_name}
        SET talker = ?,
            message_count = ?,
            sender_count = ?,
            latest_message_date = ?,
            room_summary = ?,
            targets_json = ?,
            final_bias = ?,
            model = ?,
            prompt_version = ?,
            raw_output = ?,
            update_time = ?
        WHERE room_id = ?
          AND analysis_date = ?
          AND analysis_window_days = ?
        """,
        params,
    )
    if int(getattr(cur, "rowcount", 0) or 0) <= 0:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                room_id, talker, analysis_date, analysis_window_days, message_count, sender_count,
                latest_message_date, room_summary, targets_json, final_bias, model, prompt_version,
                raw_output, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                room_id,
                talker,
                analysis_date,
                analysis_window_days,
                message_count,
                sender_count,
                latest_message_date,
                room_summary,
                targets_json,
                final_bias,
                model,
                PROMPT_VERSION,
                raw_output,
                now,
                now,
            ),
        )
    conn.commit()


def main() -> int:
    args = parse_args()
    model = normalize_model_name(args.model)
    temperature = normalize_temperature_for_model(model, args.temperature)

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_table(conn, args.table_name)
        rooms = fetch_candidate_rooms(conn, args, args.table_name)
        if not rooms:
            print("没有需要处理的群聊。")
            return 0

        category_text = args.primary_category.strip() or "全部分类"
        print(f"待处理群聊数: {len(rooms)} | 模型: {model} | 分类: {category_text}")
        for idx, room in enumerate(rooms, start=1):
            talker = room_display_name(room)
            room_id = str(room["room_id"] or "")
            latest_message_date = str(room["latest_message_date"] or "")
            rows = fetch_recent_messages(conn, talker, args.days, args.max_messages)
            if not rows:
                print(f"[{idx}/{len(rooms)}] {talker}: skip, 最近{args.days}天无记录")
                continue

            digest = build_message_digest(rows, args.max_chars)
            payload = {
                "room_id": room_id,
                "talker": talker,
                "analysis_window_days": max(args.days, 1),
                "message_count": int(room["message_count"] or 0),
                "sender_count": int(room["sender_count"] or 0),
                "latest_message_date": latest_message_date,
                "candidate_markers": infer_candidate_markers(rows, limit=20),
                "recent_messages_sample": digest,
            }
            prompt = build_prompt(payload)
            if args.verbose:
                print(
                    f"[{idx}/{len(rooms)}] {talker}: "
                    f"messages={len(rows)} digest_chars={len(digest)} latest={latest_message_date}"
                )
            else:
                print(f"[{idx}/{len(rooms)}] {talker}: 分析中...")

            parsed = None
            raw_output = ""
            last_error = None
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

            upsert_analysis(
                conn,
                args.table_name,
                room_id=room_id,
                talker=talker,
                analysis_date=latest_message_date,
                analysis_window_days=max(args.days, 1),
                message_count=int(room["message_count"] or 0),
                sender_count=int(room["sender_count"] or 0),
                latest_message_date=latest_message_date,
                room_summary=parsed["room_summary"],
                targets_json=parsed["targets_json"],
                final_bias=parsed["final_bias"],
                model=raw_result.used_model,
                raw_output=raw_output,
            )
            print(
                f"  完成 final_bias={parsed['final_bias']} "
                f"targets={len(parsed['targets'])} actual_model={raw_result.used_model}"
            )
            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
