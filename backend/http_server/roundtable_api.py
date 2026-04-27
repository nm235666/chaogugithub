#!/usr/bin/env python3
from __future__ import annotations

import db_compat as sqlite3
from services.agent_service.chief_roundtable_v1 import (
create_roundtable_job as _create_roundtable_job,
get_roundtable_job as _get_roundtable_job,
list_roundtable_jobs as _list_roundtable_jobs,
)

from backend.http_server import config

# ---------------------------------------------------------------------------
# Chief Roundtable API helpers
# ---------------------------------------------------------------------------

def _roundtable_create(payload: dict) -> dict:
    ts_code = str(payload.get("ts_code") or "").strip().upper()
    if not ts_code:
        raise ValueError("ts_code 不能为空")
    return _create_roundtable_job(
        sqlite3_module=sqlite3,
        ts_code=ts_code,
        trigger=str(payload.get("trigger") or "manual"),
        source_job_id=str(payload.get("source_job_id") or ""),
        owner=str(payload.get("_owner") or ""),
    )


def _roundtable_get(job_id: str) -> dict | None:
    return _get_roundtable_job(sqlite3_module=sqlite3, job_id=job_id)


def _roundtable_list(ts_code: str = "", owner: str = "", page: int = 1, page_size: int = 20) -> dict:
    return _list_roundtable_jobs(
        sqlite3_module=sqlite3,
        ts_code=ts_code, owner=owner, page=page, page_size=page_size,
    )

__all__ = ['_roundtable_create', '_roundtable_get', '_roundtable_list']
