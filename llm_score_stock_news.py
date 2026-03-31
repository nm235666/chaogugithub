#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import db_compat as sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model
from realtime_streams import publish_app_event

IMPORTANCE_LEVELS = {"极高", "高", "中", "低", "极低"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="对个股新闻进行LLM评分和摘要并入库")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--model", default="GPT-5.4", help="模型名；批处理/并发默认只使用 GPT-5.4")
    parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    parser.add_argument("--limit", type=int, default=20, help="本次最多评分条数")
    parser.add_argument("--row-id", type=int, default=0, help="仅评分指定新闻ID")
    parser.add_argument("--ts-code", default="", help="仅评分指定股票代码")
    parser.add_argument("--force", action="store_true", help="强制重评")
    parser.add_argument("--retry", type=int, default=1, help="单条失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.05, help="每批间隔秒数，单条回退时也复用")
    parser.add_argument("--workers", type=int, default=6, help="并发 worker 数")
    parser.add_argument("--batch-size", type=int, default=8, help="每次请求打包的新闻条数")
    parser.add_argument("--no-single-fallback", action="store_true", help="批处理失败时不退回单条 GPT 补打")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



def ensure_columns(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            company_name TEXT NOT NULL,
            source TEXT NOT NULL,
            news_code TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            link TEXT,
            pub_time TEXT,
            comment_num INTEGER,
            relation_stock_tags_json TEXT,
            llm_system_score INTEGER,
            llm_finance_impact_score INTEGER,
            llm_finance_importance TEXT,
            llm_impacts_json TEXT,
            llm_summary TEXT,
            llm_model TEXT,
            llm_scored_at TEXT,
            llm_prompt_version TEXT,
            llm_raw_output TEXT,
            content_hash TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            update_time TEXT,
            UNIQUE(ts_code, source, content_hash)
        )
        """
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
    need = [
        ("llm_system_score", "INTEGER"),
        ("llm_finance_impact_score", "INTEGER"),
        ("llm_finance_importance", "TEXT"),
        ("llm_impacts_json", "TEXT"),
        ("llm_summary", "TEXT"),
        ("llm_model", "TEXT"),
        ("llm_scored_at", "TEXT"),
        ("llm_prompt_version", "TEXT"),
        ("llm_raw_output", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE stock_news_items ADD COLUMN {name} {typ}")
    conn.commit()


def fetch_rows(conn: sqlite3.Connection, limit: int, row_id: int, ts_code: str, force: bool) -> list[sqlite3.Row]:
    where = []
    params: list[object] = []
    if int(row_id or 0) > 0:
        where.append("id = ?")
        params.append(int(row_id))
    if ts_code.strip():
        where.append("ts_code = ?")
        params.append(ts_code.strip().upper())
    if not force:
        where.append("(llm_system_score IS NULL OR llm_finance_impact_score IS NULL OR llm_finance_importance IS NULL OR llm_summary IS NULL)")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT id, ts_code, company_name, source, news_code, title, summary, link, pub_time, comment_num
    FROM stock_news_items
    {where_sql}
    ORDER BY pub_time DESC, id DESC
    LIMIT ?
    """
    params.append(max(limit, 1))
    return conn.execute(sql, params).fetchall()


def build_prompt(news: dict) -> str:
    return (
        "你是一个专业的个股新闻事件分析系统。请对输入的单条个股新闻做结构化评分与摘要。\n\n"
        "输出要求：只输出 JSON，不要输出任何额外解释。\n"
        "字段如下：\n"
        "{\n"
        '  "system_score": 0,\n'
        '  "finance_impact_score": 0,\n'
        '  "finance_importance": "中",\n'
        '  "summary": "不超过120字，概括事件及潜在影响",\n'
        '  "impacts": {\n'
        '    "macro": [{"item":"风险偏好","direction":"中性"}],\n'
        '    "markets": [{"item":"A股","direction":"利空"}],\n'
        '    "sectors": [{"item":"机械设备","direction":"利空"}]\n'
        "  }\n"
        "}\n\n"
        "评分规则与财经新闻评分一致：\n"
        "- 系统评分: 事件本身重要性\n"
        "- 财经影响评分: 对市场与资产价格影响强弱\n"
        "- 财经重要程度: 极高/高/中/低/极低\n"
        "- 若信息不足仍需审慎作答\n\n"
        f"输入新闻：\n{json.dumps(news, ensure_ascii=False)}"
    )


def build_batch_prompt(news_batch: list[dict]) -> str:
    return (
        "你是一个专业的个股新闻事件分析系统。请对输入的多条个股新闻逐条做结构化评分与摘要。\n\n"
        "输出要求：只输出 JSON，不要输出任何额外解释。\n"
        "输出格式如下：\n"
        "{\n"
        '  "items": [\n'
        "    {\n"
        '      "id": 123,\n'
        '      "system_score": 0,\n'
        '      "finance_impact_score": 0,\n'
        '      "finance_importance": "中",\n'
        '      "summary": "不超过120字，概括事件及潜在影响",\n'
        '      "impacts": {\n'
        '        "macro": [{"item":"风险偏好","direction":"中性"}],\n'
        '        "markets": [{"item":"A股","direction":"利多"}],\n'
        '        "sectors": [{"item":"机械设备","direction":"中性"}]\n'
        "      }\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "要求：\n"
        "- 必须为输入中的每条新闻都输出一条结果，按 id 一一对应，不得遗漏。\n"
        "- finance_importance 只能取：极高/高/中/低/极低。\n"
        "- 若信息不足也必须谨慎评分，不得留空。\n\n"
        f"输入新闻列表：\n{json.dumps(news_batch, ensure_ascii=False)}"
    )


def call_llm(model: str, temperature: float, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=120,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨、克制、结构化的个股新闻评分引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def to_score(v) -> int:
    try:
        n = int(round(float(v)))
    except Exception:
        n = 0
    return max(0, min(100, n))


def fallback_importance(system_score: int, impact_score: int) -> str:
    x = max(system_score, impact_score)
    if x >= 90:
        return "极高"
    if x >= 75:
        return "高"
    if x >= 50:
        return "中"
    if x >= 20:
        return "低"
    return "极低"


def parse_llm_output(raw: str) -> dict:
    txt = (raw or "").strip()
    if not txt:
        raise RuntimeError("LLM返回空内容")

    candidates = [txt]
    fence_matches = re.findall(r"```(?:json)?\s*([\s\S]*?)```", txt, flags=re.IGNORECASE)
    candidates.extend(item.strip() for item in fence_matches if item.strip())

    json_object_match = re.search(r"(\{[\s\S]*\})", txt)
    if json_object_match:
        candidates.append(json_object_match.group(1).strip())

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            obj = json.loads(candidate)
            ss = to_score(obj.get("system_score"))
            fi = to_score(obj.get("finance_impact_score"))
            imp = str(obj.get("finance_importance", "")).strip()
            if imp not in IMPORTANCE_LEVELS:
                imp = fallback_importance(ss, fi)
            impacts = obj.get("impacts", {})
            if not isinstance(impacts, dict):
                impacts = {}
            summary = str(obj.get("summary", "")).strip()[:240]
            return {
                "system_score": ss,
                "finance_impact_score": fi,
                "finance_importance": imp,
                "impacts_json": json.dumps(impacts, ensure_ascii=False),
                "summary": summary,
            }
        except Exception:
            pass

    def find_num(pattern: str) -> int:
        match = re.search(pattern, txt, flags=re.IGNORECASE)
        if not match:
            return 0
        return to_score(match.group(1))

    ss = find_num(r"系统评分[：:\s]*([0-9]+(?:\.[0-9]+)?)")
    fi = find_num(r"财经影响评分[：:\s]*([0-9]+(?:\.[0-9]+)?)")
    match_imp = re.search(r"财经重要程度[：:\s]*(极高|高|中|低|极低)", txt)
    match_summary = re.search(r"摘要[：:\s]*(.+)", txt)
    if ss or fi or match_imp or match_summary:
        imp = match_imp.group(1) if match_imp else fallback_importance(ss, fi)
        summary = (match_summary.group(1).strip() if match_summary else txt.strip())[:240]
        return {
            "system_score": ss,
            "finance_impact_score": fi,
            "finance_importance": imp,
            "impacts_json": json.dumps({}, ensure_ascii=False),
            "summary": summary,
        }

    preview = re.sub(r"\s+", " ", txt)[:280]
    raise RuntimeError(f"LLM返回内容无法解析为评分结果: {preview}")


def parse_batch_llm_output(raw: str, expected_ids: set[int]) -> dict[int, dict]:
    txt = (raw or "").strip()
    if not txt:
        raise RuntimeError("LLM批处理返回空内容")

    candidates = [txt]
    fence_matches = re.findall(r"```(?:json)?\s*([\s\S]*?)```", txt, flags=re.IGNORECASE)
    candidates.extend(item.strip() for item in fence_matches if item.strip())
    json_match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", txt)
    if json_match:
        candidates.append(json_match.group(1).strip())

    last_error = None
    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except Exception as exc:
            last_error = exc
            continue
        if isinstance(obj, dict):
            items = obj.get("items")
            if isinstance(items, list):
                obj = items
            else:
                obj = [obj]
        if not isinstance(obj, list):
            continue

        parsed_map: dict[int, dict] = {}
        for item in obj:
            if not isinstance(item, dict):
                continue
            try:
                row_id = int(item.get("id"))
            except Exception:
                continue
            if row_id not in expected_ids:
                continue
            parsed_map[row_id] = {
                "system_score": to_score(item.get("system_score")),
                "finance_impact_score": to_score(item.get("finance_impact_score")),
                "finance_importance": (
                    str(item.get("finance_importance", "")).strip()
                    if str(item.get("finance_importance", "")).strip() in IMPORTANCE_LEVELS
                    else fallback_importance(
                        to_score(item.get("system_score")),
                        to_score(item.get("finance_impact_score")),
                    )
                ),
                "impacts_json": json.dumps(item.get("impacts", {}) if isinstance(item.get("impacts", {}), dict) else {}, ensure_ascii=False),
                "summary": str(item.get("summary", "")).strip()[:240],
            }
        if parsed_map:
            return parsed_map

    if last_error is not None:
        raise RuntimeError(f"LLM批处理返回无法解析: {last_error}")
    preview = re.sub(r"\s+", " ", txt)[:280]
    raise RuntimeError(f"LLM批处理返回内容无法解析: {preview}")


def update_row(conn: sqlite3.Connection, row_id: int, parsed: dict, model: str, prompt_version: str, raw_output: str):
    conn.execute(
        """
        UPDATE stock_news_items
        SET
            llm_system_score = ?,
            llm_finance_impact_score = ?,
            llm_finance_importance = ?,
            llm_impacts_json = ?,
            llm_summary = ?,
            llm_model = ?,
            llm_scored_at = ?,
            llm_prompt_version = ?,
            llm_raw_output = ?,
            update_time = ?
        WHERE id = ?
        """,
        (
            parsed["system_score"],
            parsed["finance_impact_score"],
            parsed["finance_importance"],
            parsed["impacts_json"],
            parsed["summary"],
            model,
            now_utc_str(),
            prompt_version,
            raw_output,
            now_utc_str(),
            row_id,
        ),
    )


def emit_result_marker(payload: dict) -> None:
    try:
        print(f"__LLM_RESULT__={json.dumps(payload, ensure_ascii=False)}")
    except Exception:
        pass


def normalize_stock_news_model(model: str, workers: int, batch_size: int) -> str:
    normalized = normalize_model_name(model)
    if workers > 1 or batch_size > 1:
        if normalized != "GPT-5.4":
            print(f"批处理/并发模式仅使用 GPT-5.4，已将模型从 {normalized or 'auto'} 切换为 GPT-5.4")
        return "GPT-5.4"
    if normalized == "auto":
        return "GPT-5.4"
    return normalized


def chunked(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), max(size, 1))]


def score_single_news(news: dict, *, model: str, temperature: float, retry: int, sleep_s: float) -> tuple[dict, str, str, list[dict]]:
    prompt = build_prompt(news)
    last_err = None
    for attempt in range(retry + 1):
        try:
            raw = call_llm(model, temperature, prompt)
            parsed = parse_llm_output(raw.text)
            return (
                parsed,
                raw.used_model,
                raw.text,
                [{"model": item.model, "base_url": item.base_url, "error": item.error} for item in raw.attempts],
            )
        except Exception as exc:
            last_err = exc
            if attempt < retry:
                time.sleep(max(sleep_s, 0.0))
    raise RuntimeError(str(last_err) if last_err else "单条评分失败")


def score_batch(news_batch: list[dict], *, model: str, temperature: float, retry: int, sleep_s: float, single_fallback: bool) -> dict:
    prompt = build_batch_prompt(news_batch)
    expected_ids = {int(item["id"]) for item in news_batch}
    last_err = None
    for attempt in range(retry + 1):
        try:
            raw = call_llm(model, temperature, prompt)
            parsed_map = parse_batch_llm_output(raw.text, expected_ids)
            missing_ids = expected_ids - set(parsed_map)
            if missing_ids:
                raise RuntimeError(f"批处理遗漏 {len(missing_ids)} 条: {sorted(missing_ids)[:6]}")
            return {
                "mode": "batch",
                "results": [
                    {
                        "id": int(item["id"]),
                        "ts_code": str(item["ts_code"] or ""),
                        "parsed": parsed_map[int(item["id"])],
                        "used_model": raw.used_model,
                        "raw_output": json.dumps(
                            {"id": int(item["id"]), **parsed_map[int(item["id"])]},
                            ensure_ascii=False,
                        ),
                        "attempts": [
                            {"model": step.model, "base_url": step.base_url, "error": step.error}
                            for step in raw.attempts
                        ],
                    }
                    for item in news_batch
                ],
            }
        except Exception as exc:
            last_err = exc
            if attempt < retry:
                time.sleep(max(sleep_s, 0.0))

    if not single_fallback:
        raise RuntimeError(str(last_err) if last_err else "批处理评分失败")

    fallback_results = []
    fallback_errors = []
    for item in news_batch:
        try:
            parsed, used_model, raw_output, attempts = score_single_news(
                item,
                model=model,
                temperature=temperature,
                retry=retry,
                sleep_s=sleep_s,
            )
            fallback_results.append(
                {
                    "id": int(item["id"]),
                    "ts_code": str(item["ts_code"] or ""),
                    "parsed": parsed,
                    "used_model": used_model,
                    "raw_output": raw_output,
                    "attempts": attempts,
                }
            )
        except Exception as exc:
            fallback_errors.append(f"id={item['id']}: {exc}")
    if fallback_errors:
        raise RuntimeError(
            "批处理失败且单条回退未全部成功: "
            + " | ".join(fallback_errors[:6])
        )
    return {"mode": "single_fallback", "results": fallback_results}


def main() -> int:
    args = parse_args()
    args.workers = max(int(args.workers), 1)
    args.batch_size = max(int(args.batch_size), 1)
    args.model = normalize_stock_news_model(args.model, args.workers, args.batch_size)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1

    prompt_version = "stock_news_score_v1"
    publish_app_event(
        event="stock_news_score_update",
        payload={
            "status": "running",
            "ts_code": args.ts_code.strip().upper(),
            "limit": int(args.limit),
            "model": args.model,
            "workers": int(args.workers),
            "batch_size": int(args.batch_size),
        },
        producer="llm_score_stock_news.py",
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_columns(conn)
        rows = fetch_rows(conn, limit=args.limit, row_id=args.row_id, ts_code=args.ts_code, force=args.force)
        if not rows:
            print("没有待评分个股新闻。")
            return 0
        ok = 0
        fail = 0
        scored_meta: list[dict] = []
        prepared_rows = [
            {
                "id": int(r["id"]),
                "ts_code": str(r["ts_code"] or ""),
                "company_name": str(r["company_name"] or ""),
                "source": str(r["source"] or ""),
                "title": str(r["title"] or ""),
                "summary": str(r["summary"] or ""),
                "pub_time": str(r["pub_time"] or ""),
                "link": str(r["link"] or ""),
                "comment_num": r["comment_num"],
            }
            for r in rows
        ]
        batches = chunked(prepared_rows, args.batch_size)
        print(
            f"批量评分启动: total={len(prepared_rows)}, batches={len(batches)}, "
            f"batch_size={args.batch_size}, workers={args.workers}, model={args.model}"
        )

        def _run_batch(idx: int, batch: list[dict]) -> tuple[int, dict]:
            print(f"[batch {idx}/{len(batches)}] scoring ids={batch[0]['id']}..{batch[-1]['id']} size={len(batch)}")
            result = score_batch(
                batch,
                model=args.model,
                temperature=args.temperature,
                retry=args.retry,
                sleep_s=args.sleep,
                single_fallback=not args.no_single_fallback,
            )
            return idx, result

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_map = {
                executor.submit(_run_batch, idx, batch): (idx, batch)
                for idx, batch in enumerate(batches, start=1)
            }
            for future in as_completed(future_map):
                idx, batch = future_map[future]
                try:
                    _, batch_result = future.result()
                    batch_ok = 0
                    for item in batch_result["results"]:
                        update_row(
                            conn,
                            int(item["id"]),
                            item["parsed"],
                            item["used_model"],
                            prompt_version if batch_result["mode"] == "batch" else f"{prompt_version}_single_fallback",
                            item["raw_output"],
                        )
                        scored_meta.append(
                            {
                                "id": int(item["id"]),
                                "ts_code": str(item["ts_code"] or ""),
                                "used_model": item["used_model"],
                                "attempts": item["attempts"],
                            }
                        )
                        batch_ok += 1
                        ok += 1
                    conn.commit()
                    used_models = sorted({item["used_model"] for item in batch_result["results"] if item.get("used_model")})
                    print(
                        f"[batch {idx}/{len(batches)}] 完成 mode={batch_result['mode']} "
                        f"success={batch_ok}/{len(batch)} models={','.join(used_models) or '-'}"
                    )
                except Exception as exc:
                    fail += len(batch)
                    print(f"[batch {idx}/{len(batches)}] 失败 size={len(batch)}: {exc}")
                time.sleep(max(args.sleep, 0.0))
        print(f"完成: success={ok}, failed={fail}, total={len(rows)}")
        emit_result_marker(
            {
                "success": ok,
                "failed": fail,
                "total": len(rows),
                "requested_model": args.model,
                "workers": int(args.workers),
                "batch_size": int(args.batch_size),
                "used_models": sorted({item["used_model"] for item in scored_meta if item.get("used_model")}),
                "items": scored_meta,
            }
        )
        publish_app_event(
            event="stock_news_score_update",
            payload={
                "status": "done",
                "ts_code": args.ts_code.strip().upper(),
                "success": ok,
                "failed": fail,
                "total": len(rows),
                "model": args.model,
                "workers": int(args.workers),
                "batch_size": int(args.batch_size),
            },
            producer="llm_score_stock_news.py",
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="stock_news_score_update",
            payload={"status": "error", "error": str(exc)},
            producer="llm_score_stock_news.py",
        )
        raise
