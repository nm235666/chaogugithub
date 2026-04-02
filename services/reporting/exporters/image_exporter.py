from __future__ import annotations


def build_image_export_meta(*, report_type: str, subject_key: str, report_date: str) -> dict:
    return {
        "format": "png",
        "filename_hint": f"{subject_key or report_type}_{report_date or 'report'}.png",
    }
