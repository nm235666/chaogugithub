from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
STRATEGIES_DIR = ROOT_DIR / "skills" / "strategies"


def load_strategy_template_text(template_name: str) -> str:
    safe_name = str(template_name or "").strip()
    if not safe_name:
        return ""
    if "/" in safe_name or ".." in safe_name:
        return ""
    path = STRATEGIES_DIR / safe_name
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
