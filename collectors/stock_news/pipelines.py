from __future__ import annotations

from collectors.news.common import ROOT_DIR, run_python_script
import db_compat as sqlite3
from services.news_priority_guard import ensure_news_scored_before_stock_news


def _enforce_news_priority() -> dict:
    result = ensure_news_scored_before_stock_news(
        sqlite3_module=sqlite3,
        db_path=str(ROOT_DIR / "stock_codes.db"),
        root_dir=ROOT_DIR,
        batch_limit=40,
        retry=0,
        sleep_s=0.05,
        timeout_s=300,
        max_rounds=6,
    )
    return result


def run_stock_news_score_refresh() -> dict:
    priority = _enforce_news_priority()
    if not priority.get("ok"):
        return {
            "ok": False,
            "runner": "python_script",
            "command": [],
            "stdout": "",
            "stderr": "国际/国内新闻仍有未评分记录，已优先尝试补分但未清空，停止个股评分。",
            "meta": {"kind": "stock_news_score_refresh", "priority_guard": priority},
        }
    return run_python_script(
        "llm_score_stock_news.py",
        "--model",
        "GPT-5.4",
        "--limit",
        "240",
        "--workers",
        "6",
        "--batch-size",
        "8",
        "--retry",
        "1",
        "--sleep",
        "0.02",
        timeout_s=1800,
        meta={"kind": "stock_news_score_refresh", "priority_guard": priority},
    )


def run_stock_news_backfill_missing() -> dict:
    return run_python_script(
        "backfill_stock_news_items.py",
        "--missing-only",
        "--limit-stocks",
        "200",
        "--page-size",
        "20",
        "--max-pages",
        "2",
        "--retry",
        "2",
        "--pause",
        "0.2",
        timeout_s=1800,
        meta={"kind": "stock_news_backfill_missing"},
    )


def run_stock_news_expand_focus() -> dict:
    priority = _enforce_news_priority()
    if not priority.get("ok"):
        return {
            "ok": False,
            "runner": "python_script",
            "command": [],
            "stdout": "",
            "stderr": "国际/国内新闻仍有未评分记录，已优先尝试补分但未清空，停止个股扩抓评分。",
            "meta": {"kind": "stock_news_expand_focus", "priority_guard": priority},
        }
    return run_python_script(
        "run_stock_news_expand_focus.py",
        "--limit-scores",
        "100",
        "--limit-candidates",
        "50",
        "--max-targets",
        "120",
        "--page-size",
        "20",
        "--score-after-fetch",
        timeout_s=1800,
        meta={"kind": "stock_news_expand_focus", "priority_guard": priority},
    )
