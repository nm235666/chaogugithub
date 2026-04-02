from __future__ import annotations

import re

from services.agent_service.contracts import DecisionConfidence, ReviewBlock


def extract_section(markdown: str, title: str) -> str:
    text = str(markdown or "").replace("\r\n", "\n")
    pattern = re.compile(rf"^##\s*{re.escape(title)}\s*$", re.MULTILINE)
    match = pattern.search(text)
    if not match:
        return ""
    start = match.end()
    next_match = re.compile(r"^##\s+", re.MULTILINE).search(text, start)
    end = next_match.start() if next_match else len(text)
    return text[start:end].strip()


def infer_decision_confidence(markdown: str) -> DecisionConfidence:
    text = str(markdown or "")
    score = None
    match = re.search(r"置信度[（(]?(?:0-100)?[）)]?[:：]?\s*([0-9]{1,3})", text)
    if match:
        try:
            score = max(0, min(int(match.group(1)), 100))
        except Exception:
            score = None
    if score is None:
        return DecisionConfidence(score=None, label="未显式给出", summary="原始分析未稳定提供数值化置信度。")
    if score >= 75:
        label = "高"
    elif score >= 50:
        label = "中"
    else:
        label = "低"
    return DecisionConfidence(score=score, label=label, summary=f"模型显式给出了{label}置信度（{score}/100）。")


def build_risk_review(markdown: str, fallback_text: str = "") -> ReviewBlock:
    summary = (
        extract_section(markdown, "关键分歧")
        or extract_section(markdown, "风险提示")
        or fallback_text
        or "原始分析未提供稳定的结构化风险段落。"
    )
    return ReviewBlock(summary=summary, source="markdown")


def build_portfolio_view(markdown: str, fallback_text: str = "") -> ReviewBlock:
    summary = (
        extract_section(markdown, "行动清单")
        or extract_section(markdown, "未来5-20个交易日观察要点")
        or fallback_text
        or "原始分析未提供稳定的结构化行动建议。"
    )
    return ReviewBlock(summary=summary, source="markdown")
