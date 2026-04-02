from __future__ import annotations


def render_markdown_document(markdown_content: str) -> str:
    return str(markdown_content or "").strip() + "\n"
