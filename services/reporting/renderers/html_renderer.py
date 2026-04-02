from __future__ import annotations

import html


def render_html_document(markdown_content: str, title: str) -> str:
    escaped = html.escape(str(markdown_content or ""))
    safe_title = html.escape(str(title or "报告"))
    return (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<title>{safe_title}</title>"
        "<style>body{font-family:Arial,sans-serif;max-width:960px;margin:40px auto;line-height:1.7;padding:0 20px;}pre{white-space:pre-wrap;word-break:break-word;}</style>"
        "</head><body>"
        f"<h1>{safe_title}</h1><pre>{escaped}</pre>"
        "</body></html>"
    )
