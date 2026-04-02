from __future__ import annotations

import subprocess
from pathlib import Path


def _pending_news_unscored_count(*, sqlite3_module, db_path) -> int:
    conn = sqlite3_module.connect(db_path)
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_feed_items'"
        ).fetchone()[0]
        if not table_exists:
            return 0
        cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
        required = {"llm_system_score", "llm_finance_impact_score", "llm_finance_importance"}
        if not required.issubset(cols):
            return 0
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM news_feed_items
            WHERE llm_system_score IS NULL
               OR llm_finance_impact_score IS NULL
               OR llm_finance_importance IS NULL
            """
        ).fetchone()
        return int((row[0] if row else 0) or 0)
    finally:
        conn.close()


def _run_news_score_batch(
    *,
    root_dir: Path,
    db_path,
    limit: int,
    retry: int,
    sleep_s: float,
    timeout_s: int,
) -> dict:
    score_cmd = [
        "python3",
        str(root_dir / "llm_score_news.py"),
        "--db-path",
        str(db_path),
        "--limit",
        str(max(1, int(limit))),
        "--retry",
        str(max(0, int(retry))),
        "--sleep",
        str(max(0.0, float(sleep_s))),
    ]
    sentiment_cmd = [
        "python3",
        str(root_dir / "llm_score_sentiment.py"),
        "--db-path",
        str(db_path),
        "--target",
        "news",
        "--limit",
        str(max(1, int(limit))),
        "--retry",
        str(max(0, int(retry))),
        "--sleep",
        str(max(0.0, float(sleep_s))),
    ]
    map_cmd = [
        "python3",
        str(root_dir / "map_news_items_to_stocks.py"),
        "--limit",
        "300",
        "--days",
        "7",
    ]

    outputs = []
    for cmd in (score_cmd, sentiment_cmd, map_cmd):
        proc = subprocess.run(
            cmd,
            cwd=str(root_dir),
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_s)),
            check=False,
        )
        outputs.append(
            {
                "ok": proc.returncode == 0,
                "command": cmd,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "returncode": proc.returncode,
            }
        )
        if proc.returncode != 0:
            break
    return {
        "ok": all(item["ok"] for item in outputs),
        "steps": outputs,
    }


def ensure_news_scored_before_stock_news(
    *,
    sqlite3_module,
    db_path,
    root_dir: Path,
    batch_limit: int = 200,
    retry: int = 1,
    sleep_s: float = 0.05,
    timeout_s: int = 900,
    max_rounds: int = 12,
) -> dict:
    pending_before = _pending_news_unscored_count(sqlite3_module=sqlite3_module, db_path=db_path)
    if pending_before <= 0:
        return {
            "ok": True,
            "pending_before": 0,
            "pending_after": 0,
            "rounds": 0,
            "steps": [],
        }

    round_steps = []
    for _ in range(max(1, int(max_rounds))):
        pending_now = _pending_news_unscored_count(sqlite3_module=sqlite3_module, db_path=db_path)
        if pending_now <= 0:
            break
        step = _run_news_score_batch(
            root_dir=root_dir,
            db_path=db_path,
            limit=min(max(1, int(batch_limit)), max(1, pending_now)),
            retry=retry,
            sleep_s=sleep_s,
            timeout_s=timeout_s,
        )
        round_steps.append(
            {
                "pending_before_round": pending_now,
                **step,
            }
        )
        if not step["ok"]:
            break

    pending_after = _pending_news_unscored_count(sqlite3_module=sqlite3_module, db_path=db_path)
    return {
        "ok": pending_after <= 0,
        "pending_before": pending_before,
        "pending_after": pending_after,
        "rounds": len(round_steps),
        "steps": round_steps,
    }
