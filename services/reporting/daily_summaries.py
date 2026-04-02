from __future__ import annotations

from .contracts import build_reporting_protocol_meta
from .service import build_report_payload


def enrich_summary_item(item: dict | None) -> dict | None:
    if not item:
        return None
    summary_item = dict(item)
    report_doc = build_report_payload(
        report_type="daily_news_summary",
        subject_key=str(summary_item.get("summary_date") or summary_item.get("id") or ""),
        subject_name=f"{summary_item.get('summary_date') or '日报'} 新闻日报",
        report_date=str(summary_item.get("summary_date") or ""),
        markdown_content=str(summary_item.get("summary_markdown") or ""),
        context_json={
            "filter_importance": summary_item.get("filter_importance"),
            "source_filter": summary_item.get("source_filter"),
            "news_count": summary_item.get("news_count"),
            "model": summary_item.get("model"),
        },
    )
    summary_item["export_meta"] = report_doc.export_meta
    summary_item["analysis_markdown"] = report_doc.markdown_content
    summary_item["markdown_content"] = report_doc.markdown_content
    return summary_item


def query_daily_summaries(
    deps: dict,
    *,
    summary_date: str,
    source_filter: str,
    model: str,
    page: int,
    page_size: int,
) -> dict:
    payload = deps["query_news_daily_summaries"](
        summary_date=summary_date,
        source_filter=source_filter,
        model=model,
        page=page,
        page_size=page_size,
    )
    items = [enrich_summary_item(item) for item in list(payload.get("items") or [])]
    payload["items"] = [item for item in items if item is not None]
    payload["protocol"] = build_reporting_protocol_meta()
    return payload


def start_daily_summary_generation(deps: dict, *, model: str, summary_date: str) -> dict:
    return deps["start_async_daily_summary_job"](model=model, summary_date=summary_date)


def get_daily_summary_task(deps: dict, *, job_id: str) -> dict | None:
    job = deps["get_async_daily_summary_job"](job_id)
    if not job:
        return None
    if job.get("item"):
        job = dict(job)
        job["item"] = enrich_summary_item(job.get("item"))
    return job
