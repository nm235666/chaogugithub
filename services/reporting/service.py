from __future__ import annotations

from services.reporting.contracts import ReportDocument
from services.reporting.exporters.image_exporter import build_image_export_meta
from services.reporting.renderers.html_renderer import render_html_document
from services.reporting.renderers.markdown_renderer import render_markdown_document
from skills.strategies import load_strategy_template_text


REPORT_TEMPLATE_MAPPING = {
    "daily_news_summary": "daily_summary_template.md",
    "research_report": "multi_role_research_template.md",
    "trend_analysis": "trend_analysis_template.md",
}


def build_report_payload(
    *,
    report_type: str,
    subject_key: str,
    subject_name: str,
    report_date: str,
    markdown_content: str,
    context_json: dict,
) -> ReportDocument:
    normalized_markdown = render_markdown_document(markdown_content)
    template_name = REPORT_TEMPLATE_MAPPING.get(str(report_type or "").strip(), "")
    strategy_template = load_strategy_template_text(template_name) if template_name else ""
    export_meta = {
        "html_preview": render_html_document(normalized_markdown, subject_name or subject_key),
        "image_export": build_image_export_meta(
            report_type=report_type,
            subject_key=subject_key,
            report_date=report_date,
        ),
        "strategy_template": {
            "template_name": template_name,
            "loaded": bool(strategy_template),
        },
    }
    return ReportDocument(
        report_type=report_type,
        subject_key=subject_key,
        subject_name=subject_name,
        report_date=report_date,
        markdown_content=normalized_markdown,
        context_json=context_json,
        export_meta=export_meta,
    )
