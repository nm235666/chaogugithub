from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

REPORTING_PROTOCOL_VERSION = "2026-04-02"
REPORTING_PRIMARY_MARKDOWN_FIELD = "analysis_markdown"
REPORTING_COMPAT_MARKDOWN_FIELD = "markdown_content"
REPORTING_COMPAT_WINDOW = "temporary"
REPORTING_COMPAT_RETIRE_AFTER = "2026-05-02"


@dataclass
class ReportDocument:
    report_type: str
    subject_key: str
    subject_name: str
    report_date: str
    markdown_content: str
    context_json: dict[str, Any]
    export_meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_reporting_protocol_meta() -> dict[str, Any]:
    return {
        "version": REPORTING_PROTOCOL_VERSION,
        "primary_markdown_field": REPORTING_PRIMARY_MARKDOWN_FIELD,
        "compat_markdown_field": REPORTING_COMPAT_MARKDOWN_FIELD,
        "compat_window": REPORTING_COMPAT_WINDOW,
        "compat_retire_after": REPORTING_COMPAT_RETIRE_AFTER,
    }
