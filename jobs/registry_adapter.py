from __future__ import annotations

from job_registry import get_default_jobs


def list_job_specs() -> list[dict]:
    return [
        {
            "job_key": job.job_key,
            "name": job.name,
            "category": job.category,
            "owner": job.owner,
            "schedule_expr": job.schedule_expr,
        }
        for job in get_default_jobs()
    ]
